"""J-Quants API V2 経由の財務諸表データプロセッサ。

V2 では `/v1/fins/statements` が `/v2/fins/summary` に置換され、フィールド名も短縮化された。
本モジュールでは `JQuantsClient` で HTTP 通信を、`_v2_translator.normalize_statements`
でカラム変換を行い、DB スキーマと外向け関数シグネチャは維持する。

旧 V1 実装は `_old/v1_statements_processor.py` に退避済み。
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, cast

import aiohttp
import pandas as pd
from dotenv import load_dotenv

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from market_pipeline.jquants._v2_translator import (  # noqa: E402
    normalize_listed_info,
    normalize_statements,
)
from market_pipeline.jquants.client import JQuantsClient  # noqa: E402
from market_pipeline.utils.cache_manager import get_cache  # noqa: E402

load_dotenv()


class JQuantsStatementsProcessor:
    """V2 API 経由の財務諸表プロセッサ。

    `JQuantsClient` を DI で受け取り、`/v2/fins/summary` から取得したレコードを
    `_v2_translator.normalize_statements` で V1 互換フィールド名に変換した上で
    `_map_statement_to_record` でテーブル列名へマップする。
    """

    def __init__(
        self,
        client: Optional[JQuantsClient] = None,
        max_concurrent_requests: int = 3,
        batch_size: int = 100,
        request_delay: float = 0.1,
    ):
        self.client = client if client is not None else JQuantsClient()
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.request_delay = request_delay

        self.logger = logging.getLogger(__name__)
        self.cache = get_cache()

    # ------------------------------------------------------------------ master
    def get_listed_info_cached(self) -> pd.DataFrame:
        """data_processor と共有のキャッシュキーで上場銘柄一覧を取得。"""
        cache_key = "jquants_listed_info"
        cached_data = self.cache.get(cache_key)

        if cached_data is not None:
            self.logger.info("Using cached listed info")
            return pd.DataFrame(cached_data)

        self.logger.info("Fetching listed company info from J-Quants V2 API...")
        rows: list[dict[str, Any]] = []
        for page in self.client.paginate("/v2/equities/master"):
            rows.extend(page)

        df = normalize_listed_info(rows)
        # キャッシュは V1 互換カラム名のレコード列
        self.cache.put(cache_key, df.to_dict("records"), ttl_hours=24)
        self.logger.info(f"Retrieved {len(df)} company listings")
        return df

    # --------------------------------------------------------------- statements
    async def get_statements_async(
        self, session: aiohttp.ClientSession, code: str
    ) -> tuple[str, list[dict[str, Any]]]:
        """単一銘柄の財務諸表を V2 API で取得し、V1 互換 dict 配列で返す。"""
        rows: list[dict[str, Any]] = []
        try:
            async for page in self.client.paginate_async(
                session, "/v2/fins/summary", params={"code": code}
            ):
                rows.extend(page)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"Failed to get statements for {code}: {exc}")
            return code, []

        if not rows:
            return code, []

        df = normalize_statements(rows)
        if df.empty:
            return code, []
        # 下流の _map_statement_to_record は V1 ロング名フィールドを期待
        records: list[dict[str, Any]] = [
            {str(k): v for k, v in row.items()} for row in df.to_dict("records")
        ]
        return code, records

    async def process_codes_batch(
        self, codes: list[str]
    ) -> list[tuple[str, list[dict[str, Any]]]]:
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def process_with_semaphore(session, code):
            async with semaphore:
                result = await self.get_statements_async(session, code)
                if self.request_delay > 0:
                    await asyncio.sleep(self.request_delay)
                return result

        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            tasks = [process_with_semaphore(session, code) for code in codes]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            valid_results: list[tuple[str, list[dict[Any, Any]]]] = []
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Task failed with exception: {result}")
                else:
                    valid_results.append(cast(tuple[str, list[dict[Any, Any]]], result))
            return valid_results

    # -------------------------------------------------------------------- DB ops
    def _initialize_database(self, db_path: str) -> None:
        """V1 と同一スキーマで financial_statements / calculated_fundamentals を作成。"""
        with sqlite3.connect(db_path) as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA cache_size=10000")

            con.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_statements (
                    -- Primary Key
                    local_code TEXT NOT NULL,
                    disclosed_date TEXT NOT NULL,
                    type_of_current_period TEXT NOT NULL,

                    -- Disclosure Info
                    disclosure_number TEXT,
                    type_of_document TEXT,

                    -- Accounting Period
                    current_period_start_date TEXT,
                    current_period_end_date TEXT,
                    current_fiscal_year_start_date TEXT,
                    current_fiscal_year_end_date TEXT,

                    -- Income Statement (Consolidated)
                    net_sales REAL,
                    operating_profit REAL,
                    ordinary_profit REAL,
                    profit REAL,
                    earnings_per_share REAL,
                    diluted_earnings_per_share REAL,

                    -- Balance Sheet (Consolidated)
                    total_assets REAL,
                    equity REAL,
                    equity_to_asset_ratio REAL,
                    book_value_per_share REAL,

                    -- Cash Flow (Consolidated)
                    cf_operating REAL,
                    cf_investing REAL,
                    cf_financing REAL,
                    cash_and_equivalents REAL,

                    -- Dividends
                    result_dividend_per_share_annual REAL,
                    forecast_dividend_per_share_annual REAL,
                    payout_ratio_annual REAL,

                    -- Share Information
                    number_of_shares REAL,
                    number_of_treasury_stock REAL,

                    -- Forecast
                    forecast_net_sales REAL,
                    forecast_operating_profit REAL,
                    forecast_ordinary_profit REAL,
                    forecast_profit REAL,
                    forecast_earnings_per_share REAL,

                    -- Metadata
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,

                    PRIMARY KEY (local_code, disclosed_date, type_of_current_period)
                )
                """
            )

            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_statements_code "
                "ON financial_statements (local_code)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_statements_date "
                "ON financial_statements (disclosed_date)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_statements_period "
                "ON financial_statements (type_of_current_period)"
            )

            con.execute(
                """
                CREATE TABLE IF NOT EXISTS calculated_fundamentals (
                    code TEXT PRIMARY KEY,
                    company_name TEXT,
                    sector_33 TEXT,
                    sector_17 TEXT,
                    market_segment TEXT,
                    latest_period TEXT,
                    latest_fiscal_year_end TEXT,
                    latest_disclosed_date TEXT,
                    market_cap REAL,
                    per REAL,
                    forward_per REAL,
                    pbr REAL,
                    dividend_yield REAL,
                    roe REAL,
                    roa REAL,
                    equity_ratio REAL,
                    operating_margin REAL,
                    profit_margin REAL,
                    eps REAL,
                    bps REAL,
                    dps REAL,
                    total_assets REAL,
                    equity REAL,
                    operating_cf REAL,
                    free_cash_flow REAL,
                    net_sales REAL,
                    operating_profit REAL,
                    ordinary_profit REAL,
                    profit REAL,
                    payout_ratio REAL,
                    reference_price REAL,
                    reference_date TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_fundamentals_sector "
                "ON calculated_fundamentals (sector_33)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_fundamentals_market "
                "ON calculated_fundamentals (market_segment)"
            )

            con.commit()
            self.logger.info(
                "Database initialized with financial_statements and "
                "calculated_fundamentals tables"
            )

    def _map_statement_to_record(self, statement: dict[str, Any]) -> dict[str, Any]:
        """V1 ロング名フィールドを DB 列名にマップする。

        normalize_statements 通過後の dict を受け取る前提。
        """
        return {
            "local_code": statement.get("LocalCode"),
            "disclosed_date": statement.get("DisclosedDate"),
            "type_of_current_period": statement.get("TypeOfCurrentPeriod"),
            "disclosure_number": statement.get("DisclosureNumber"),
            "type_of_document": statement.get("TypeOfDocument"),
            "current_period_start_date": statement.get("CurrentPeriodStartDate"),
            "current_period_end_date": statement.get("CurrentPeriodEndDate"),
            "current_fiscal_year_start_date": statement.get(
                "CurrentFiscalYearStartDate"
            ),
            "current_fiscal_year_end_date": statement.get("CurrentFiscalYearEndDate"),
            "net_sales": statement.get("NetSales"),
            "operating_profit": statement.get("OperatingProfit"),
            "ordinary_profit": statement.get("OrdinaryProfit"),
            "profit": statement.get("Profit"),
            "earnings_per_share": statement.get("EarningsPerShare"),
            "diluted_earnings_per_share": statement.get("DilutedEarningsPerShare"),
            "total_assets": statement.get("TotalAssets"),
            "equity": statement.get("Equity"),
            "equity_to_asset_ratio": statement.get("EquityToAssetRatio"),
            "book_value_per_share": statement.get("BookValuePerShare"),
            "cf_operating": statement.get("CashFlowsFromOperatingActivities"),
            "cf_investing": statement.get("CashFlowsFromInvestingActivities"),
            "cf_financing": statement.get("CashFlowsFromFinancingActivities"),
            "cash_and_equivalents": statement.get("CashAndEquivalents"),
            "result_dividend_per_share_annual": statement.get(
                "ResultDividendPerShareAnnual"
            ),
            "forecast_dividend_per_share_annual": statement.get(
                "ForecastDividendPerShareAnnual"
            ),
            "payout_ratio_annual": statement.get("PayoutRatioAnnual"),
            "number_of_shares": statement.get(
                "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"
            ),
            "number_of_treasury_stock": statement.get(
                "NumberOfTreasuryStockAtTheEndOfFiscalYear"
            ),
            "forecast_net_sales": statement.get("ForecastNetSales"),
            "forecast_operating_profit": statement.get("ForecastOperatingProfit"),
            "forecast_ordinary_profit": statement.get("ForecastOrdinaryProfit"),
            "forecast_profit": statement.get("ForecastProfit"),
            "forecast_earnings_per_share": statement.get("ForecastEarningsPerShare"),
        }

    def save_statements_batch(
        self, db_path: str, statements_data: list[tuple[str, list[dict[str, Any]]]]
    ) -> int:
        all_records: list[dict[str, Any]] = []
        for _code, statements in statements_data:
            for statement in statements:
                record = self._map_statement_to_record(statement)
                if record["local_code"] and record["disclosed_date"]:
                    all_records.append(record)

        if not all_records:
            return 0

        with sqlite3.connect(db_path) as con:
            con.execute("PRAGMA journal_mode=WAL")
            columns = list(all_records[0].keys())
            placeholders = ",".join(["?" for _ in columns])
            column_names = ",".join(columns)

            query = (
                f"INSERT OR REPLACE INTO financial_statements "
                f"({column_names}) VALUES ({placeholders})"
            )
            values = [
                tuple(record.get(col) for col in columns) for record in all_records
            ]
            con.executemany(query, values)
            con.commit()

        return len(all_records)

    def get_all_statements(self, db_path: str) -> None:
        """全銘柄の財務諸表を取得して DB へ投入する。"""
        self.logger.info("Starting statements data fetch for all listed companies")
        start_time = time.time()

        self._initialize_database(db_path)
        listed_info_df = self.get_listed_info_cached()

        codes = [str(code) for code in listed_info_df["Code"].tolist()]
        total_codes = len(codes)
        self.logger.info(f"Processing {total_codes} codes")

        successful_codes = 0
        failed_codes: list[str] = []
        total_records_saved = 0

        for i in range(0, total_codes, self.batch_size):
            batch_codes = codes[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_codes + self.batch_size - 1) // self.batch_size
            batch_end = min(i + self.batch_size, total_codes)
            progress_pct = (batch_end / total_codes) * 100

            elapsed = time.time() - start_time
            if i > 0:
                avg = elapsed / batch_num
                eta = avg * (total_batches - batch_num)
                time_str = f", Elapsed: {elapsed:.1f}s, ETA: {eta:.1f}s"
            else:
                time_str = ""

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} - "
                f"Codes {i + 1}-{batch_end}/{total_codes} ({progress_pct:.1f}%){time_str}"
            )

            try:
                results = asyncio.run(self.process_codes_batch(batch_codes))

                batch_successful = []
                batch_failed = 0
                for result_code, statements in results:
                    if statements:
                        batch_successful.append((result_code, statements))
                        successful_codes += 1
                    else:
                        failed_codes.append(result_code)
                        batch_failed += 1

                self.logger.info(
                    f"Batch {batch_num}: Fetched {len(batch_successful)}/"
                    f"{len(batch_codes)} codes ({batch_failed} failed/no data)"
                )

                if batch_successful:
                    records_saved = self.save_statements_batch(
                        db_path, batch_successful
                    )
                    total_records_saved += records_saved
                    self.logger.info(
                        f"Batch {batch_num}: Saved {records_saved} records"
                    )

            except Exception as exc:  # noqa: BLE001
                self.logger.error(f"Error processing batch {batch_num}: {exc}")
                failed_codes.extend(batch_codes)

        total_time = time.time() - start_time
        self.logger.info(
            f"Completed in {total_time:.1f}s: {successful_codes}/{total_codes} successful, "
            f"{len(failed_codes)} failed/no data, {total_records_saved} records saved"
        )

    def get_database_stats(self, db_path: str) -> dict[str, Any]:
        try:
            with sqlite3.connect(db_path) as con:
                stmt_count_df = pd.read_sql(
                    "SELECT COUNT(*) as count FROM financial_statements", con
                )
                stmt_count = stmt_count_df.iloc[0]["count"]

                codes_df = pd.read_sql(
                    "SELECT COUNT(DISTINCT local_code) as code_count "
                    "FROM financial_statements",
                    con,
                )
                code_count = codes_df.iloc[0]["code_count"]

                date_range_df = pd.read_sql(
                    "SELECT MIN(disclosed_date) as min_date, "
                    "MAX(disclosed_date) as max_date FROM financial_statements",
                    con,
                )
                min_date = date_range_df.iloc[0]["min_date"]
                max_date = date_range_df.iloc[0]["max_date"]

                fund_count_df = pd.read_sql(
                    "SELECT COUNT(*) as count FROM calculated_fundamentals", con
                )
                fund_count = fund_count_df.iloc[0]["count"]

                return {
                    "statement_record_count": stmt_count,
                    "statement_code_count": code_count,
                    "statement_date_range": f"{min_date} - {max_date}",
                    "fundamentals_count": fund_count,
                }
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"Error getting database stats: {exc}")
            return {}


def setup_logging():
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                log_dir / f"statements_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            ),
        ],
    )
    return logging.getLogger(__name__)


def main() -> None:
    logger = setup_logging()
    try:
        processor = JQuantsStatementsProcessor(
            max_concurrent_requests=3, batch_size=100, request_delay=0.1
        )

        output_dir = project_root / "data"
        output_dir.mkdir(exist_ok=True)
        db_path = str(output_dir / "statements.db")

        logger.info("Starting statements data fetch...")
        processor.get_all_statements(db_path)

        stats = processor.get_database_stats(db_path)
        if stats:
            logger.info("Database statistics:")
            logger.info(
                f"  Statement Records: {stats.get('statement_record_count', 'N/A')}"
            )
            logger.info(
                f"  Codes with Statements: {stats.get('statement_code_count', 'N/A')}"
            )
            logger.info(f"  Date range: {stats.get('statement_date_range', 'N/A')}")
            logger.info(
                f"  Calculated Fundamentals: {stats.get('fundamentals_count', 'N/A')}"
            )

    except Exception as exc:  # noqa: BLE001
        logger.error(f"Error occurred: {exc}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
