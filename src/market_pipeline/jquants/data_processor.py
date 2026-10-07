"""J-Quants API V2 経由の日次株価データプロセッサ。

V2 移行(2026-05-31 V1 廃止)に伴い、HTTP 通信を `JQuantsClient` に、
レスポンス整形を `_v2_translator` に集約した。外向け関数シグネチャと戻り値の
カラム名は V1 互換で維持しているため、呼び出し元と DB スキーマに変更はない。

旧 V1 実装は `_old/v1_data_processor.py` に退避済み。
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Optional, cast

import aiohttp
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from market_pipeline.jquants._v2_translator import (  # noqa: E402
    normalize_daily_quotes,
    normalize_listed_info,
)
from market_pipeline.jquants.client import JQuantsClient  # noqa: E402
from market_pipeline.utils.cache_manager import get_cache  # noqa: E402
from market_pipeline.utils.parallel_processor import (  # noqa: E402
    BatchDatabaseProcessor,
    measure_performance,
)

load_dotenv()

CALENDAR_PATH = "/v2/markets/calendar"
DAILY_BARS_PATH = "/v2/equities/bars/daily"
# 取引カレンダーの HolDiv のうち現物の取引がある日（1=営業日・2=半日立会）。
TRADING_HOL_DIVS = frozenset({"1", "2"})
FAILED_DATES_TABLE = "daily_quotes_failed_dates"

# 取り直す日の規則（ssp の短信同期と同じ「直近 7 平日」＋薄い日の補助判定）。
RECENT_WEEKDAYS = 7
THIN_LOOKBACK_TRADING_DAYS = 30
THIN_RATIO = 0.9


def recent_weekdays_start(today: date, n: int = RECENT_WEEKDAYS) -> date:
    """今日を含む直近 n 平日の最初の日（祝日も平日として数える＝ssp の窓と同じ）。"""
    d = today
    count = 0
    while True:
        if d.weekday() < 5:
            count += 1
            if count == n:
                return d
        d -= timedelta(days=1)


def select_dates_to_fetch(
    *,
    trading_days: list[str],
    today: str,
    last_jquants_date: Optional[str],
    failed_dates: Iterable[str],
    row_counts: dict[str, int],
) -> dict[str, set[str]]:
    """取り直す営業日を、理由ごとの集合で返す（和集合が取得対象）。

    1. ``window``: 直近 7 平日に含まれる営業日（`INSERT OR REPLACE` なので取り直しは冪等）
    2. ``after_last``: 全体の最終日より後の営業日（窓より長く止まっていた場合）
    3. ``failed``: 記録された失敗日（窓や 30 日を過ぎても成功するまで残る）
    4. ``thin``: 補助。直近 30 営業日のうち、行数がその 30 日の中央値の 90% 未満の日
       （行数 0 を含む）。中央値が 0 なら判定しない（全滅は ``after_last`` が拾う）。

    ``trading_days`` は今日以前の営業日の昇順。``row_counts`` は日ごとの jquants 行数。
    """
    days = [d for d in trading_days if d <= today]
    window_start = recent_weekdays_start(date.fromisoformat(today)).isoformat()
    window = {d for d in days if d >= window_start}
    after_last = {d for d in days if last_jquants_date is None or d > last_jquants_date}
    failed = set(failed_dates)

    thin: set[str] = set()
    lookback = days[-THIN_LOOKBACK_TRADING_DAYS:]
    if lookback:
        counts = [row_counts.get(d, 0) for d in lookback]
        m = median(counts)
        if m > 0:
            thin = {d for d, c in zip(lookback, counts) if c < THIN_RATIO * m}
    return {"window": window, "after_last": after_last, "failed": failed, "thin": thin}


class JQuantsDataProcessor:
    """V2 API 経由の日次株価プロセッサ。

    `JQuantsClient` を DI で受け取り、URL 構築・認証・レート制限・リトライは
    クライアントに委譲する。`_v2_translator` で V2 短縮カラム名を V1 ロング名へ
    rename してから DB に投入するため、DB スキーマと下流分析モジュールに変更はない。
    """

    # 日本市場の上場銘柄は通常 4000+。100 未満なら API 異常とみなしキャッシュしない。
    # 日付メジャー取得でも「1 日分が 100 行未満なら失敗」に使う。⚠️ これは異常応答の下限であって
    # 完全性の検査ではない（1 日は約 4,440 行）。一部の銘柄が欠けた日は select_dates_to_fetch の
    # 「薄い日」（中央値の 90% 未満）が拾う。半日立会の日も行数はほぼ変わらないので、ここを
    # 行数の期待値へ引き上げないこと。
    MIN_EXPECTED_COMPANIES = 100

    def __init__(
        self,
        client: Optional[JQuantsClient] = None,
        max_concurrent_requests: int = 10,
        batch_size: int = 100,
        request_delay: float = 0.05,
        timeout_seconds: int = 30,
    ):
        """`client` 未指定時はデフォルト設定で `JQuantsClient()` を生成する。
        `JQUANTS_API_KEY` が設定されていない場合は API 呼び出し時に例外。
        """
        self.client = (
            client
            if client is not None
            else JQuantsClient(timeout_seconds=timeout_seconds)
        )
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.request_delay = request_delay
        self.timeout_seconds = timeout_seconds

        self.logger = logging.getLogger(__name__)
        self.cache = get_cache()
        self.db_processor: Optional[BatchDatabaseProcessor] = None

    # ------------------------------------------------------------------ master
    def get_listed_info_cached(self) -> pd.DataFrame:
        """上場銘柄一覧を取得(キャッシュ優先)。

        キャッシュには V1 互換カラム名のレコード列を保存するため、
        `fundamentals_calculator` 等の下流が `CompanyName` / `Sector33CodeName`
        / `MarketCodeName` を参照する既存挙動を維持する。
        """
        cache_key = "jquants_listed_info"
        cached_data = self.cache.get(cache_key)

        if cached_data is not None:
            if len(cached_data) >= self.MIN_EXPECTED_COMPANIES:
                self.logger.info(
                    "Using cached listed info (%d companies)", len(cached_data)
                )
                return pd.DataFrame(cached_data)
            self.logger.warning(
                "Cached listed info has only %d entries (expected %d+). Re-fetching.",
                len(cached_data),
                self.MIN_EXPECTED_COMPANIES,
            )

        self.logger.info("Fetching listed company info from J-Quants V2 API...")
        rows: list[dict[str, Any]] = []
        for page in self.client.paginate("/v2/equities/master"):
            rows.extend(page)

        df = normalize_listed_info(rows)

        if len(df) >= self.MIN_EXPECTED_COMPANIES:
            # キャッシュには V1 互換カラム名で保存
            self.cache.put(cache_key, df.to_dict("records"), ttl_hours=24)
            self.logger.info(f"Retrieved and cached {len(df)} company listings")
        else:
            self.logger.warning(
                f"API returned only {len(df)} companies (expected "
                f"{self.MIN_EXPECTED_COMPANIES}+). Result NOT cached."
            )

        return df

    # --------------------------------------------------------------- daily quotes
    async def get_daily_quotes_async(
        self,
        session: aiohttp.ClientSession,
        code: str,
        from_date: str,
        to_date: str,
    ) -> tuple[str, pd.DataFrame]:
        """単一銘柄の日次株価を V2 API で取得し、V1 互換 DataFrame を返す。"""
        params = {"code": code, "from": from_date, "to": to_date}
        rows: list[dict[str, Any]] = []
        try:
            async for page in self.client.paginate_async(
                session, "/v2/equities/bars/daily", params=params
            ):
                rows.extend(page)
        except Exception as exc:  # noqa: BLE001 - 銘柄単位の失敗は処理を継続
            self.logger.warning(f"Failed to get quotes for {code}: {exc}")
            return code, pd.DataFrame()

        return code, normalize_daily_quotes(rows)

    async def process_codes_batch(
        self, codes: list[str], from_date: str, to_date: str
    ) -> list[tuple[str, pd.DataFrame]]:
        """並列で複数銘柄の日次株価を取得する。"""
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def process_with_semaphore(session, code):
            async with semaphore:
                result = await self.get_daily_quotes_async(
                    session, code, from_date, to_date
                )
                if self.request_delay > 0:
                    await asyncio.sleep(self.request_delay)
                return result

        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            tasks = [process_with_semaphore(session, code) for code in codes]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            valid_results: list[tuple[str, pd.DataFrame]] = []
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Task failed with exception: {result}")
                else:
                    valid_results.append(cast(tuple[str, pd.DataFrame], result))
            return valid_results

    # -------------------------------------------------------------------- DB ops
    def get_last_dates_batch(self, db_path: str, codes: list[str]) -> dict[str, str]:
        if not self.db_processor:
            self.db_processor = BatchDatabaseProcessor(db_path)

        try:
            placeholders = ",".join(["?" for _ in codes])
            query = f"""
                SELECT Code, MAX(Date) as last_date
                FROM daily_quotes
                WHERE Code IN ({placeholders})
                GROUP BY Code
            """
            results_df = self.db_processor.batch_fetch(
                query, params=codes, as_dataframe=True
            )

            last_dates: dict[str, str] = {}
            for _, row in results_df.iterrows():
                last_dates[row["Code"]] = row["last_date"]

            default_date = (datetime.now() - relativedelta(years=5)).strftime(
                "%Y-%m-%d"
            )
            for code in codes:
                if code not in last_dates:
                    last_dates[code] = default_date
            return last_dates

        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"Error getting last dates batch: {exc}")
            default_date = (datetime.now() - relativedelta(years=5)).strftime(
                "%Y-%m-%d"
            )
            return {code: default_date for code in codes}

    def save_quotes_batch(
        self, db_path: str, quotes_data: list[tuple[str, pd.DataFrame]]
    ) -> None:
        if not self.db_processor:
            self.db_processor = BatchDatabaseProcessor(db_path)

        all_records: list[dict[str, Any]] = []
        for _result_code, df in quotes_data:
            if not df.empty:
                all_records.extend(df.to_dict("records"))

        if all_records:
            for record in all_records:
                record["source"] = "jquants"
            inserted = self.db_processor.batch_insert(
                "daily_quotes", all_records, on_conflict="REPLACE"
            )
            self.logger.info(f"Batch inserted {inserted} records")

    @measure_performance
    def get_all_prices_for_past_5_years_to_db_optimized(
        self, db_path: str
    ) -> dict[str, int]:
        """過去 5 年分の日次株価を全銘柄取得して DB に投入する。"""
        self.logger.info("Starting optimized 5-year data fetch")
        start_time = time.time()

        self._initialize_database(db_path)
        listed_info_df = self.get_listed_info_cached()

        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - relativedelta(years=5)).strftime("%Y-%m-%d")

        codes = [str(code) for code in listed_info_df["Code"].tolist()]
        total_codes = len(codes)

        self.logger.info(
            f"Processing {total_codes} codes from {from_date} to {to_date}"
        )

        successful_codes = 0
        failed_codes: list[str] = []
        total_processed = 0
        total_records_saved = 0

        for i in range(0, total_codes, self.batch_size):
            batch_codes = codes[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_codes + self.batch_size - 1) // self.batch_size
            batch_end = min(i + self.batch_size, total_codes)
            progress_pct = (batch_end / total_codes) * 100

            elapsed_time = time.time() - start_time
            if total_processed > 0:
                avg = elapsed_time / total_processed
                eta = avg * (total_codes - batch_end)
                time_str = f", Elapsed: {elapsed_time:.1f}s, ETA: {eta:.1f}s"
            else:
                time_str = ""

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} - "
                f"Codes {i + 1}-{batch_end}/{total_codes} ({progress_pct:.1f}%){time_str}"
            )

            try:
                results = asyncio.run(
                    self.process_codes_batch(batch_codes, from_date, to_date)
                )
                batch_successful = []
                for result_code, df in results:
                    if not df.empty:
                        batch_successful.append((result_code, df))
                        successful_codes += 1
                    else:
                        failed_codes.append(result_code)

                if batch_successful:
                    self.save_quotes_batch(db_path, batch_successful)
                    batch_records = sum(len(df) for _, df in batch_successful)
                    total_records_saved += batch_records
                    self.logger.info(
                        f"Batch {batch_num}: Saved {batch_records} records for "
                        f"{len(batch_successful)} codes | "
                        f"Progress: {successful_codes}/{total_codes} codes, "
                        f"{total_records_saved} total records"
                    )

                total_processed = batch_end

            except Exception as exc:  # noqa: BLE001
                self.logger.error(f"Error processing batch {batch_num}: {exc}")
                failed_codes.extend(batch_codes)
                total_processed = batch_end

        total_time = time.time() - start_time
        self.logger.info(
            f"Completed in {total_time:.1f}s: {successful_codes}/{total_codes} successful, "
            f"{len(failed_codes)} failed, {total_records_saved} records saved"
        )

        return {
            "total_listed": total_codes,
            "codes_to_update": total_codes,
            "codes_updated": successful_codes,
            "records_inserted": total_records_saved,
            "codes_failed": len(failed_codes),
        }

    @measure_performance
    def update_prices_to_db_optimized(self, db_path: str) -> dict[str, int]:
        """差分更新: DB の最新日以降のみ取得して投入。"""
        self.logger.info("Starting optimized price update")
        start_time = time.time()

        self._initialize_database(db_path)
        listed_info_df = self.get_listed_info_cached()

        codes = [str(code) for code in listed_info_df["Code"].tolist()]
        total_codes = len(codes)
        to_date = datetime.now().strftime("%Y-%m-%d")

        self.logger.info(f"Getting last dates for {total_codes} codes...")
        last_dates = self.get_last_dates_batch(db_path, codes)

        codes_to_update: list[tuple[str, str]] = []
        for code in codes:
            last_date = last_dates.get(code)
            if last_date:
                try:
                    last_dt = datetime.strptime(last_date, "%Y-%m-%d")
                    from_date = (last_dt + relativedelta(days=1)).strftime("%Y-%m-%d")
                    if from_date <= to_date:
                        codes_to_update.append((code, from_date))
                except ValueError:
                    from_date = (datetime.now() - relativedelta(years=5)).strftime(
                        "%Y-%m-%d"
                    )
                    codes_to_update.append((code, from_date))

        self.logger.info(
            f"{len(codes_to_update)}/{total_codes} codes need updates "
            f"({(len(codes_to_update) / total_codes * 100):.1f}%)"
        )

        if not codes_to_update:
            self.logger.info("All data is up to date")
            return {
                "total_listed": total_codes,
                "codes_to_update": 0,
                "codes_updated": 0,
                "records_inserted": 0,
                "codes_failed": 0,
            }

        # Group codes by from_date for efficient batching
        date_groups: dict[str, list[str]] = {}
        for code, from_date in codes_to_update:
            date_groups.setdefault(from_date, []).append(code)

        successful_codes = 0
        failed_codes: list[str] = []
        updated_codes = 0
        total_records_updated = 0
        codes_processed = 0

        for group_idx, (from_date, group_codes) in enumerate(date_groups.items()):
            group_start = codes_processed
            self.logger.info(
                f"Date group {group_idx + 1}/{len(date_groups)}: "
                f"{len(group_codes)} codes from {from_date}"
            )

            for i in range(0, len(group_codes), self.batch_size):
                batch_codes = group_codes[i : i + self.batch_size]
                batch_start = group_start + i
                batch_end = min(batch_start + len(batch_codes), len(codes_to_update))
                progress_pct = (batch_end / len(codes_to_update)) * 100

                elapsed_time = time.time() - start_time
                if codes_processed > 0:
                    avg = elapsed_time / codes_processed
                    eta = avg * (len(codes_to_update) - batch_end)
                    time_str = f", Elapsed: {elapsed_time:.1f}s, ETA: {eta:.1f}s"
                else:
                    time_str = ""

                self.logger.info(
                    f"Processing codes {batch_start + 1}-{batch_end}/"
                    f"{len(codes_to_update)} ({progress_pct:.1f}%){time_str}"
                )

                try:
                    results = asyncio.run(
                        self.process_codes_batch(batch_codes, from_date, to_date)
                    )
                    batch_successful = []
                    for result_code, df in results:
                        if not df.empty:
                            batch_successful.append((result_code, df))
                            successful_codes += 1
                            updated_codes += 1
                        else:
                            successful_codes += 1  # 取得 0 件は失敗ではない

                    if batch_successful:
                        self.save_quotes_batch(db_path, batch_successful)
                        batch_records = sum(len(df) for _, df in batch_successful)
                        total_records_updated += batch_records
                        self.logger.info(
                            f"Batch updated: {batch_records} records for "
                            f"{len(batch_successful)} codes | "
                            f"Progress: {successful_codes}/{len(codes_to_update)} codes, "
                            f"{total_records_updated} total records"
                        )

                    codes_processed += len(batch_codes)

                except Exception as exc:  # noqa: BLE001
                    self.logger.error(f"Error processing batch: {exc}")
                    failed_codes.extend(batch_codes)
                    codes_processed += len(batch_codes)

            codes_processed = group_start + len(group_codes)

        total_time = time.time() - start_time
        self.logger.info(
            f"Update completed in {total_time:.1f}s: "
            f"{successful_codes}/{len(codes_to_update)} processed, "
            f"{updated_codes} updated, {total_records_updated} records, "
            f"{len(failed_codes)} failed"
        )

        return {
            "total_listed": total_codes,
            "codes_to_update": len(codes_to_update),
            "codes_updated": updated_codes,
            "records_inserted": total_records_updated,
            "codes_failed": len(failed_codes),
        }

    # ----------------------------------------------------- date-major (S2a 橋)
    def get_trading_days(self, from_date: str, to_date: str) -> list[str]:
        """取引カレンダーから営業日を ISO 昇順で返す（1 論理取得）。"""
        days: list[str] = []
        for page in self.client.paginate(
            CALENDAR_PATH, params={"from": from_date, "to": to_date}
        ):
            for row in page:
                if str(row.get("HolDiv", "")) not in TRADING_HOL_DIVS:
                    continue
                d = str(row.get("Date", "")).strip()
                if d:
                    days.append(d)
        return sorted(set(days))

    def fetch_daily_quotes_by_date(self, day: str) -> tuple[pd.DataFrame, int]:
        """1 営業日分の全銘柄を全ページ取り終えてから V1 互換 DataFrame で返す。

        Returns:
            (DataFrame, ページ数)。途中のページで落ちたら例外（その日は保存しない）。
        """
        rows: list[dict[str, Any]] = []
        pages = 0
        for page in self.client.paginate(DAILY_BARS_PATH, params={"date": day}):
            rows.extend(page)
            pages += 1
        return normalize_daily_quotes(rows), pages

    def save_day(self, db_path: str, day: str, df: pd.DataFrame) -> int:
        """1 日分の保存と失敗日の削除を 1 トランザクションで行う。

        ⚠️ 既存の `save_quotes_batch` は接続を受け取らず内部で commit するので、失敗日の
        削除を同じトランザクションに含められない（別モデルレビュー T1）。列の変換・
        `source='jquants'`・`INSERT OR REPLACE` は `save_quotes_batch` と同じ。
        """
        records = df.to_dict("records") if not df.empty else []
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("BEGIN")
            if records:
                columns = list(records[0].keys()) + ["source"]
                placeholders = ",".join("?" for _ in columns)
                query = (
                    f"INSERT OR REPLACE INTO daily_quotes ({','.join(columns)}) "
                    f"VALUES ({placeholders})"
                )
                conn.executemany(
                    query,
                    [tuple(r[c] for c in columns[:-1]) + ("jquants",) for r in records],
                )
            conn.execute(f"DELETE FROM {FAILED_DATES_TABLE} WHERE date = ?", (day,))
            conn.commit()
            return len(records)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_failed_date(self, db_path: str, day: str, error: str) -> None:
        """落ちた日を記録する（保存とは別のトランザクション）。"""
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {FAILED_DATES_TABLE} (date, last_error, failed_at) "
                "VALUES (?, ?, ?)",
                (day, error[:500], datetime.now().isoformat(timespec="seconds")),
            )
            conn.commit()

    def _read_fetch_state(
        self, db_path: str, since: str
    ) -> tuple[Optional[str], list[str], dict[str, int]]:
        """(jquants 行の最終日, 失敗日, since 以降の日ごとの jquants 行数)。"""
        with sqlite3.connect(db_path) as conn:
            last = conn.execute(
                "SELECT MAX(Date) FROM daily_quotes WHERE source = 'jquants'"
            ).fetchone()[0]
            failed = [
                r[0]
                for r in conn.execute(
                    f"SELECT date FROM {FAILED_DATES_TABLE} ORDER BY date"
                )
            ]
            counts = {
                r[0]: int(r[1])
                for r in conn.execute(
                    "SELECT Date, COUNT(*) FROM daily_quotes "
                    "WHERE source = 'jquants' AND Date >= ? GROUP BY Date",
                    (since,),
                )
            }
        return last, failed, counts

    def update_prices_by_date(
        self, db_path: str, today: Optional[str] = None
    ) -> dict[str, Any]:
        """日付メジャーの差分更新（S2a 橋・旧 `update_prices_to_db_optimized` の置き換え）。

        取り直す日は `select_dates_to_fetch` の和集合。1 日は全ページを取り終えてから
        `save_day` で保存し、落ちた日は保存せず失敗日へ記録して続ける。
        """
        start_time = time.time()
        self._initialize_database(db_path)
        req0, retry0 = self.client.http_requests, self.client.retries
        today = today or date.today().isoformat()

        # カレンダーの範囲: 薄い日の判定（30 営業日≒45 暦日）・最終日以降・最古の失敗日を覆う
        since_dt = date.fromisoformat(today) - timedelta(days=45)
        last, failed, _ = self._read_fetch_state(db_path, since_dt.isoformat())
        candidates = [since_dt]
        if last:
            candidates.append(date.fromisoformat(last) + timedelta(days=1))
        if failed:
            candidates.append(date.fromisoformat(failed[0]))
        cal_from = min(candidates).isoformat()

        trading_days = self.get_trading_days(cal_from, today)
        logical = {"calendar": 1, "daily_bars": 0}
        _, _, counts = self._read_fetch_state(db_path, cal_from)
        reasons = select_dates_to_fetch(
            trading_days=trading_days,
            today=today,
            last_jquants_date=last,
            failed_dates=failed,
            row_counts=counts,
        )
        targets = sorted(set().union(*reasons.values()))
        self.logger.info(
            "取り直す日 %d 日（窓 %d・最終日以降 %d・失敗日 %d・薄い日 %d）",
            len(targets),
            len(reasons["window"]),
            len(reasons["after_last"]),
            len(reasons["failed"]),
            len(reasons["thin"]),
        )

        pages_total = 0
        records_total = 0
        fetched: list[str] = []
        failed_now: dict[str, str] = {}
        empty_today = False
        for day in targets:
            logical["daily_bars"] += 1
            try:
                df, pages = self.fetch_daily_quotes_by_date(day)
                pages_total += pages
                n = len(df)
                if n == 0 and day == today:
                    # 当日分の公開前（手動の昼実行など）。次の実行の窓が拾うので失敗にしない。
                    self.logger.warning(
                        "%s: 当日分がまだ空です（次の実行で取り直します）", day
                    )
                    empty_today = True
                    continue
                if n < self.MIN_EXPECTED_COMPANIES:
                    raise ValueError(
                        f"行数が異常に少ない: {n} 行（{self.MIN_EXPECTED_COMPANIES} 未満）"
                    )
                records_total += self.save_day(db_path, day, df)
                fetched.append(day)
                self.logger.info("%s: %d 行（%d ページ）", day, n, pages)
            except Exception as exc:  # noqa: BLE001 - 1 日の失敗で止めない
                msg = f"{type(exc).__name__}: {exc}"
                self.logger.error("%s: 取得または保存に失敗 %s", day, msg)
                failed_now[day] = msg
                try:
                    self.record_failed_date(db_path, day, msg)
                except Exception as rec_exc:  # noqa: BLE001
                    self.logger.error("%s: 失敗日の記録にも失敗 %s", day, rec_exc)

        http_requests = self.client.http_requests - req0
        retries = self.client.retries - retry0
        total_time = time.time() - start_time
        self.logger.info(
            "日付メジャー更新 %.1f 秒: 論理取得 カレンダー %d・日次バー %d／ページ %d／"
            "実 HTTP %d／再試行 %d／保存 %d 日・%d 行／失敗 %d 日%s",
            total_time,
            logical["calendar"],
            logical["daily_bars"],
            pages_total,
            http_requests,
            retries,
            len(fetched),
            records_total,
            len(failed_now),
            f"（{', '.join(sorted(failed_now))}）" if failed_now else "",
        )
        return {
            "dates_to_fetch": len(targets),
            "dates_fetched": len(fetched),
            "dates_failed": len(failed_now),
            "failed_dates": sorted(failed_now),
            "records_inserted": records_total,
            "logical_fetches": logical,
            "pages": pages_total,
            "http_requests": http_requests,
            "retries": retries,
            "empty_today": empty_today,
        }

    def _initialize_database(self, db_path: str) -> None:
        """daily_quotes テーブルを作成(V1 と同一スキーマ)。"""
        if not self.db_processor:
            self.db_processor = BatchDatabaseProcessor(db_path)

        with sqlite3.connect(db_path) as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA cache_size=10000")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_quotes (
                    Code TEXT,
                    Date TEXT,
                    Open REAL,
                    High REAL,
                    Low REAL,
                    Close REAL,
                    Volume INTEGER,
                    TurnoverValue REAL,
                    AdjustmentFactor REAL,
                    AdjustmentOpen REAL,
                    AdjustmentHigh REAL,
                    AdjustmentLow REAL,
                    AdjustmentClose REAL,
                    AdjustmentVolume INTEGER,
                    source TEXT,
                    PRIMARY KEY (Code, Date)
                )
                """
            )
            # S2a 橋: 日付メジャー取得で落ちた日（成功するまで取り直す）。daily_quotes の列は変えない。
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {FAILED_DATES_TABLE} (
                    date TEXT PRIMARY KEY,
                    last_error TEXT,
                    failed_at TEXT
                )
                """
            )
            con.commit()

    def get_database_stats(self, db_path: str) -> dict[str, Any]:
        try:
            with sqlite3.connect(db_path) as con:
                count_df = pd.read_sql(
                    "SELECT COUNT(*) as count FROM daily_quotes", con
                )
                record_count = count_df.iloc[0]["count"]

                codes_df = pd.read_sql(
                    "SELECT COUNT(DISTINCT Code) as code_count FROM daily_quotes", con
                )
                code_count = codes_df.iloc[0]["code_count"]

                date_range_df = pd.read_sql(
                    "SELECT MIN(Date) as min_date, MAX(Date) as max_date FROM daily_quotes",
                    con,
                )
                min_date = date_range_df.iloc[0]["min_date"]
                max_date = date_range_df.iloc[0]["max_date"]

                return {
                    "record_count": record_count,
                    "code_count": code_count,
                    "date_range": f"{min_date} - {max_date}",
                }
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"Error getting database stats: {exc}")
            return {}


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                f"jquants_optimized_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            ),
        ],
    )
    return logging.getLogger(__name__)


def main() -> None:
    logger = setup_logging()
    try:
        processor = JQuantsDataProcessor(
            max_concurrent_requests=10,
            batch_size=100,
            request_delay=0.05,
            timeout_seconds=30,
        )

        output_dir = Path(__file__).parent.parent.parent / "data"
        output_dir.mkdir(exist_ok=True)
        db_path = str(output_dir / "jquants.db")

        if not Path(db_path).exists():
            logger.info("Database does not exist. Fetching 5 years of data...")
            processor.get_all_prices_for_past_5_years_to_db_optimized(db_path)
        else:
            logger.info("Database exists. Performing incremental update...")
            processor.update_prices_to_db_optimized(db_path)

        stats = processor.get_database_stats(db_path)
        if stats:
            logger.info("Database statistics:")
            logger.info(f"  Records: {stats.get('record_count', 'N/A')}")
            logger.info(f"  Codes: {stats.get('code_count', 'N/A')}")
            logger.info(f"  Date range: {stats.get('date_range', 'N/A')}")

    except Exception as exc:  # noqa: BLE001
        logger.error(f"Error occurred: {exc}")
        logger.error("Please check your .env file and JQUANTS_API_KEY")


if __name__ == "__main__":
    main()
