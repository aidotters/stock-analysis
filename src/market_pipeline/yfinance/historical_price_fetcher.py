"""
Historical Price Fetcher

Fetches historical daily price data (up to 20 years) from yfinance
and inserts into jquants.db daily_quotes table. Uses J-Quants data
as the primary source; only fetches yfinance data for periods before
the earliest J-Quants record for each stock.
"""

import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import pandas as pd
import yfinance as yf

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)


class HistoricalPriceFetcher:
    """Fetches and stores historical price data from yfinance."""

    def __init__(
        self,
        jquants_db_path: Optional[str] = None,
        master_db_path: Optional[str] = None,
        max_workers: int = 4,
        wait_seconds: float = 0.5,
        batch_size: int = 1000,
        years: int = 20,
    ):
        settings = get_settings()
        self.jquants_db_path = jquants_db_path or str(settings.paths.jquants_db)
        self.master_db_path = master_db_path or str(settings.paths.master_db)
        self.max_workers = max_workers
        self.wait_seconds = wait_seconds
        self.batch_size = batch_size
        self.years = years

    def get_earliest_date(self, code: str) -> Optional[str]:
        """指定銘柄のjquants.dbにおける最古日を返す。データがなければNone。"""
        code_5digit = f"{code}0" if len(code) == 4 else code
        with sqlite3.connect(self.jquants_db_path) as conn:
            row = conn.execute(
                "SELECT MIN(Date) FROM daily_quotes WHERE Code = ?",
                (code_5digit,),
            ).fetchone()
        if row and row[0]:
            return row[0]
        return None

    def get_target_codes(self, symbols: Optional[list[str]] = None) -> list[dict]:
        """アクティブ銘柄一覧を取得し、各銘柄のjquants.db最古日を付与。

        Returns:
            list of dict with keys: code, yfinance_symbol, earliest_date
        """
        with sqlite3.connect(self.master_db_path) as conn:
            if symbols:
                placeholders = ",".join(["?" for _ in symbols])
                rows = conn.execute(
                    f"SELECT code, yfinance_symbol FROM stocks_master WHERE is_active = 1 AND code IN ({placeholders})",
                    symbols,
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT code, yfinance_symbol FROM stocks_master WHERE is_active = 1"
                ).fetchall()

        targets = []
        for code, yf_symbol in rows:
            if not yf_symbol:
                continue
            earliest = self.get_earliest_date(code)
            targets.append(
                {
                    "code": code,
                    "yfinance_symbol": yf_symbol,
                    "earliest_date": earliest,
                }
            )
        return targets

    def map_columns(self, df: pd.DataFrame, code: str) -> pd.DataFrame:
        """yfinance DataFrame → daily_quotesカラム形式に変換。

        Args:
            df: yfinance history DataFrame (Date index, Open/High/Low/Close/Volume columns)
            code: 4桁銘柄コード

        Returns:
            daily_quotes形式のDataFrame
        """
        if df.empty:
            return pd.DataFrame()

        code_5digit = f"{code}0" if len(code) == 4 else code

        # MultiIndex解消後に重複カラムがある場合は最初のものを使用
        if df.columns.duplicated().any():
            df = df.loc[:, ~df.columns.duplicated()]

        result = pd.DataFrame()
        result["Date"] = pd.to_datetime(df.index).strftime("%Y-%m-%d")
        result["Code"] = code_5digit
        result["Open"] = None
        result["High"] = None
        result["Low"] = None
        result["Close"] = None
        result["Volume"] = None
        result["TurnoverValue"] = None
        result["AdjustmentFactor"] = None
        result["AdjustmentOpen"] = df["Open"].values if "Open" in df.columns else None
        result["AdjustmentHigh"] = df["High"].values if "High" in df.columns else None
        result["AdjustmentLow"] = df["Low"].values if "Low" in df.columns else None
        result["AdjustmentClose"] = (
            df["Close"].values if "Close" in df.columns else None
        )
        if "Volume" in df.columns:
            vol = df["Volume"].copy()
            vol = vol.fillna(0).astype(int)
            result["AdjustmentVolume"] = vol.values
        else:
            result["AdjustmentVolume"] = None
        result["source"] = "yfinance"
        result = result.reset_index(drop=True)
        return result

    def fetch_single(self, target: dict) -> Optional[pd.DataFrame]:
        """1銘柄分のyfinanceデータを取得しマッピング済みDataFrameを返す。

        最大3回リトライ（1秒間隔）。
        J-Quantsデータの最古日以前のデータのみを取得する。
        """
        code = target["code"]
        symbol = target["yfinance_symbol"]
        earliest_date = target["earliest_date"]

        # 取得期間の決定
        now = datetime.now()
        start_date = datetime(now.year - self.years, now.month, now.day).strftime(
            "%Y-%m-%d"
        )

        if earliest_date:
            end_date = earliest_date  # yfinance endは排他的なのでこれでちょうど前日まで
        else:
            # J-Quantsデータがない場合は現在日まで取得
            end_date = now.strftime("%Y-%m-%d")

        if start_date >= end_date:
            logger.debug(
                f"{code}: 取得対象期間なし (start={start_date}, end={end_date})"
            )
            return None

        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = yf.download(
                    symbol,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=True,
                )
                if df.empty:
                    logger.debug(f"{code}: yfinanceデータなし")
                    return None

                # MultiIndex columns (ticker, field) → flat columns
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                return self.map_columns(df, code)

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"{code}: yfinance取得失敗 (試行{attempt + 1}/{max_retries}): {e}"
                    )
                    time.sleep(1)
                else:
                    logger.error(f"{code}: yfinance取得失敗 (リトライ上限到達): {e}")
                    return None
        return None

    def save_batch(self, records: list[dict]) -> int:
        """daily_quotesにバッチINSERT（INSERT OR IGNORE）。

        Returns:
            挿入されたレコード数
        """
        if not records:
            return 0

        with sqlite3.connect(self.jquants_db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            before = conn.execute("SELECT COUNT(*) FROM daily_quotes").fetchone()[0]
            conn.executemany(
                """
                INSERT OR IGNORE INTO daily_quotes
                (Code, Date, Open, High, Low, Close, Volume, TurnoverValue,
                 AdjustmentFactor, AdjustmentOpen, AdjustmentHigh, AdjustmentLow,
                 AdjustmentClose, AdjustmentVolume, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["Code"],
                        r["Date"],
                        r.get("Open"),
                        r.get("High"),
                        r.get("Low"),
                        r.get("Close"),
                        r.get("Volume"),
                        r.get("TurnoverValue"),
                        r.get("AdjustmentFactor"),
                        r.get("AdjustmentOpen"),
                        r.get("AdjustmentHigh"),
                        r.get("AdjustmentLow"),
                        r.get("AdjustmentClose"),
                        r.get("AdjustmentVolume"),
                        r.get("source", "yfinance"),
                    )
                    for r in records
                ],
            )
            conn.commit()
            after = conn.execute("SELECT COUNT(*) FROM daily_quotes").fetchone()[0]
        return after - before

    def run(
        self,
        symbols: Optional[list[str]] = None,
        dry_run: bool = False,
    ) -> dict:
        """全銘柄を処理し、結果サマリを返す。

        Args:
            symbols: 指定銘柄のみ取得（Noneで全アクティブ銘柄）
            dry_run: Trueの場合、DB書き込みをスキップ

        Returns:
            dict with keys: success, failed, skipped, total_records, elapsed
        """
        start_time = time.time()

        # yfinanceの冗長なエラーログ（データなし銘柄のリトライ等）を抑制
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)

        targets = self.get_target_codes(symbols)
        total = len(targets)
        if total == 0:
            logger.info("対象銘柄なし")
            return {
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "total_records": 0,
                "elapsed": 0.0,
            }

        logger.info(f"対象銘柄数: {total}")

        success = 0
        failed = 0
        skipped = 0
        all_records: list[dict] = []

        def _fetch_with_wait(target: dict) -> tuple[dict, Optional[pd.DataFrame]]:
            result = self.fetch_single(target)
            time.sleep(self.wait_seconds)
            return target, result

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(_fetch_with_wait, t): t for t in targets}

            for i, future in enumerate(as_completed(futures), 1):
                target = futures[future]
                code = target["code"]
                try:
                    _, result_df = future.result()
                    if result_df is None:
                        skipped += 1
                    elif result_df.empty:
                        skipped += 1
                    else:
                        records = result_df.to_dict("records")
                        all_records.extend(records)
                        success += 1
                except Exception as e:
                    logger.error(f"{code}: 処理エラー: {e}")
                    failed += 1

                # 進捗ログ（100銘柄ごと）
                if i % 100 == 0 or i == total:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"進捗: {i}/{total} (成功={success}, 失敗={failed}, スキップ={skipped}, "
                        f"レコード={len(all_records)}, 経過={elapsed:.1f}s)"
                    )

                # バッチINSERT
                if not dry_run and len(all_records) >= self.batch_size:
                    self.save_batch(all_records)
                    all_records = []

        # 残りのレコードをINSERT
        if not dry_run and all_records:
            self.save_batch(all_records)

        elapsed = time.time() - start_time

        # 最終レコード数をDBから取得
        record_count = 0
        if not dry_run:
            with sqlite3.connect(self.jquants_db_path) as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM daily_quotes WHERE source = 'yfinance'"
                ).fetchone()
                record_count = row[0] if row else 0

        logger.info(
            f"完了: 成功={success}, 失敗={failed}, スキップ={skipped}, "
            f"yfinanceレコード総数={record_count}, 経過={elapsed:.1f}s"
        )

        return {
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "total_records": record_count,
            "elapsed": elapsed,
        }
