"""
J-Quants Statements API Processor for fetching financial statement data.
"""

import os
import json
import asyncio
import aiohttp
import requests
import pandas as pd
import sqlite3
import logging
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Tuple, Any, cast
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from market_pipeline.utils.cache_manager import get_cache  # noqa: E402

load_dotenv()

API_URL = "https://api.jquants.com"


class JQuantsStatementsProcessor:
    """
    Processor for fetching financial statements from J-Quants Statements API.
    Follows the same patterns as JQuantsDataProcessor for consistency.
    """

    def __init__(
        self,
        max_concurrent_requests: int = 3,
        batch_size: int = 100,
        request_delay: float = 0.1,
    ):
        """
        Initialize J-Quants Statements processor.

        Args:
            max_concurrent_requests: Maximum concurrent API requests
            batch_size: Batch size for database operations
            request_delay: Delay between requests in seconds
        """
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.request_delay = request_delay

        # Setup logging
        self.logger = logging.getLogger(__name__)

        # Cache manager
        self.cache = get_cache()

        # Initialize tokens
        self._refresh_token = self._get_refresh_token()
        self._id_token = self._get_id_token()

    def _get_refresh_token(self) -> str:
        """Get refresh token from JQuants API."""
        data = {"mailaddress": os.getenv("EMAIL"), "password": os.getenv("PASSWORD")}
        res = requests.post(f"{API_URL}/v1/token/auth_user", data=json.dumps(data))
        if res.status_code == 200:
            self.logger.info("Successfully obtained refresh token")
            return res.json()["refreshToken"]
        else:
            raise Exception(
                f"Failed to get refresh token: {res.json().get('message', res.text)}"
            )

    def _get_id_token(self) -> str:
        """Get ID token using refresh token."""
        res = requests.post(
            f"{API_URL}/v1/token/auth_refresh?refreshtoken={self._refresh_token}"
        )
        if res.status_code == 200:
            self.logger.info("Successfully obtained ID token")
            return res.json()["idToken"]
        else:
            raise Exception(f"Failed to get ID token: {res.text}")

    @property
    def _headers(self) -> dict:
        """Return headers for API requests."""
        return {"Authorization": f"Bearer {self._id_token}"}

    def get_listed_info_cached(self) -> pd.DataFrame:
        """
        Get listed company info with caching.
        Reuses the same cache as JQuantsDataProcessor.
        """
        cache_key = "jquants_listed_info"
        cached_data = self.cache.get(cache_key)

        if cached_data is not None:
            self.logger.info("Using cached listed info")
            return pd.DataFrame(cached_data)

        self.logger.info("Fetching listed company info from API...")
        params: Dict[str, str] = {}
        res = requests.get(
            f"{API_URL}/v1/listed/info", params=params, headers=self._headers
        )

        if res.status_code != 200:
            raise Exception(f"Failed to get listed info: {res.text}")

        d = res.json()
        data = d["info"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            res = requests.get(
                f"{API_URL}/v1/listed/info", params=params, headers=self._headers
            )
            if res.status_code != 200:
                raise Exception(f"Failed to get paginated listed info: {res.text}")
            d = res.json()
            data += d["info"]

        df = pd.DataFrame(data)

        # Cache for 24 hours
        self.cache.put(cache_key, data, ttl_hours=24)

        self.logger.info(f"Retrieved {len(df)} company listings")
        return df

    async def get_statements_async(
        self, session: aiohttp.ClientSession, code: str
    ) -> Tuple[str, List[Dict]]:
        """
        Async fetch statements for a single code with pagination handling.

        Args:
            session: aiohttp session
            code: Stock code (5 digits)

        Returns:
            Tuple of (code, list of statement records)
        """
        params = {"code": code}

        try:
            async with session.get(
                f"{API_URL}/v1/fins/statements", params=params, headers=self._headers
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    self.logger.warning(
                        f"Failed to get statements for {code}: {error_text}"
                    )
                    return code, []

                d = await response.json()
                data = d.get("statements", [])

                # Handle pagination
                while "pagination_key" in d:
                    params["pagination_key"] = d["pagination_key"]
                    async with session.get(
                        f"{API_URL}/v1/fins/statements",
                        params=params,
                        headers=self._headers,
                    ) as page_response:
                        if page_response.status != 200:
                            self.logger.warning(
                                f"Failed to get paginated statements for {code}"
                            )
                            break
                        d = await page_response.json()
                        data += d.get("statements", [])

                return code, data

        except Exception as e:
            self.logger.error(f"Error getting statements for {code}: {e}")
            return code, []

    async def process_codes_batch(
        self, codes: List[str]
    ) -> List[Tuple[str, List[Dict]]]:
        """
        Process a batch of stock codes concurrently.

        Args:
            codes: List of stock codes to process

        Returns:
            List of (code, list of statement records) tuples
        """
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def process_with_semaphore(session, code):
            async with semaphore:
                result = await self.get_statements_async(session, code)
                # Add delay to respect rate limits
                await asyncio.sleep(self.request_delay)
                return result

        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            tasks = [process_with_semaphore(session, code) for code in codes]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Filter out exceptions
            valid_results: List[Tuple[str, List[Dict[Any, Any]]]] = []
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Task failed with exception: {result}")
                else:
                    valid_results.append(cast(Tuple[str, List[Dict[Any, Any]]], result))

            return valid_results

    def _initialize_database(self, db_path: str):
        """Initialize database with schema for financial statements."""
        with sqlite3.connect(db_path) as con:
            # Enable optimizations
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA cache_size=10000")

            # Create financial_statements table for raw API data
            con.execute("""
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
            """)

            # Create indexes
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_statements_code ON financial_statements (local_code)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_statements_date ON financial_statements (disclosed_date)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_statements_period ON financial_statements (type_of_current_period)"
            )

            # Create calculated_fundamentals table for derived metrics
            con.execute("""
                CREATE TABLE IF NOT EXISTS calculated_fundamentals (
                    -- Primary Key
                    code TEXT PRIMARY KEY,

                    -- Company Info (from listed_info API)
                    company_name TEXT,
                    sector_33 TEXT,
                    sector_17 TEXT,
                    market_segment TEXT,

                    -- Latest Financial Data Reference
                    latest_period TEXT,
                    latest_fiscal_year_end TEXT,
                    latest_disclosed_date TEXT,

                    -- Valuation Metrics (calculated)
                    market_cap REAL,
                    per REAL,
                    forward_per REAL,
                    pbr REAL,
                    dividend_yield REAL,

                    -- Profitability Metrics
                    roe REAL,
                    roa REAL,
                    equity_ratio REAL,
                    operating_margin REAL,
                    profit_margin REAL,

                    -- Per Share Data
                    eps REAL,
                    bps REAL,
                    dps REAL,

                    -- Balance Sheet Highlights
                    total_assets REAL,
                    equity REAL,

                    -- Cash Flow Highlights
                    operating_cf REAL,
                    free_cash_flow REAL,

                    -- Raw Financial Data
                    net_sales REAL,
                    operating_profit REAL,
                    ordinary_profit REAL,
                    profit REAL,

                    -- Payout
                    payout_ratio REAL,

                    -- Reference Price (for calculations)
                    reference_price REAL,
                    reference_date TEXT,

                    -- Metadata
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for calculated_fundamentals
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_fundamentals_sector ON calculated_fundamentals (sector_33)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_fundamentals_market ON calculated_fundamentals (market_segment)"
            )

            con.commit()
            self.logger.info(
                "Database initialized with financial_statements and calculated_fundamentals tables"
            )

    def _map_statement_to_record(self, statement: Dict) -> Dict:
        """
        Map J-Quants API response to database record.

        Args:
            statement: Raw statement from API

        Returns:
            Dictionary with database column names
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
            # Income Statement
            "net_sales": statement.get("NetSales"),
            "operating_profit": statement.get("OperatingProfit"),
            "ordinary_profit": statement.get("OrdinaryProfit"),
            "profit": statement.get("Profit"),
            "earnings_per_share": statement.get("EarningsPerShare"),
            "diluted_earnings_per_share": statement.get("DilutedEarningsPerShare"),
            # Balance Sheet
            "total_assets": statement.get("TotalAssets"),
            "equity": statement.get("Equity"),
            "equity_to_asset_ratio": statement.get("EquityToAssetRatio"),
            "book_value_per_share": statement.get("BookValuePerShare"),
            # Cash Flow
            "cf_operating": statement.get("CashFlowsFromOperatingActivities"),
            "cf_investing": statement.get("CashFlowsFromInvestingActivities"),
            "cf_financing": statement.get("CashFlowsFromFinancingActivities"),
            "cash_and_equivalents": statement.get("CashAndEquivalents"),
            # Dividends
            "result_dividend_per_share_annual": statement.get(
                "ResultDividendPerShareAnnual"
            ),
            "forecast_dividend_per_share_annual": statement.get(
                "ForecastDividendPerShareAnnual"
            ),
            "payout_ratio_annual": statement.get("PayoutRatioAnnual"),
            # Share Info
            "number_of_shares": statement.get(
                "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"
            ),
            "number_of_treasury_stock": statement.get(
                "NumberOfTreasuryStockAtTheEndOfFiscalYear"
            ),
            # Forecast
            "forecast_net_sales": statement.get("ForecastNetSales"),
            "forecast_operating_profit": statement.get("ForecastOperatingProfit"),
            "forecast_ordinary_profit": statement.get("ForecastOrdinaryProfit"),
            "forecast_profit": statement.get("ForecastProfit"),
            "forecast_earnings_per_share": statement.get("ForecastEarningsPerShare"),
        }

    def save_statements_batch(
        self, db_path: str, statements_data: List[Tuple[str, List[Dict]]]
    ) -> int:
        """
        Batch insert statements into database.

        Args:
            db_path: Database path
            statements_data: List of (code, list of statements) tuples

        Returns:
            Number of records inserted
        """
        all_records = []
        for code, statements in statements_data:
            for statement in statements:
                record = self._map_statement_to_record(statement)
                if record["local_code"] and record["disclosed_date"]:
                    all_records.append(record)

        if not all_records:
            return 0

        with sqlite3.connect(db_path) as con:
            con.execute("PRAGMA journal_mode=WAL")

            # Use INSERT OR REPLACE for upsert behavior
            columns = list(all_records[0].keys())
            placeholders = ",".join(["?" for _ in columns])
            column_names = ",".join(columns)

            query = f"INSERT OR REPLACE INTO financial_statements ({column_names}) VALUES ({placeholders})"

            values = [
                tuple(record.get(col) for col in columns) for record in all_records
            ]
            con.executemany(query, values)
            con.commit()

        return len(all_records)

    def get_all_statements(self, db_path: str):
        """
        Fetch statements for all listed companies and save to database.

        Args:
            db_path: Path to statements database
        """
        self.logger.info("Starting statements data fetch for all listed companies")
        start_time = time.time()

        # Initialize database
        self._initialize_database(db_path)

        # Get listed companies (cached)
        listed_info_df = self.get_listed_info_cached()

        codes = [str(code) for code in listed_info_df["Code"].tolist()]
        total_codes = len(codes)

        self.logger.info(f"Processing {total_codes} codes")

        # Process codes in batches
        successful_codes = 0
        failed_codes = []
        total_records_saved = 0

        for i in range(0, total_codes, self.batch_size):
            batch_codes = codes[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_codes + self.batch_size - 1) // self.batch_size
            batch_end = min(i + self.batch_size, total_codes)
            progress_pct = (batch_end / total_codes) * 100

            # Calculate elapsed time and estimate remaining time
            elapsed_time = time.time() - start_time
            if i > 0:
                avg_time_per_batch = elapsed_time / batch_num
                remaining_batches = total_batches - batch_num
                estimated_remaining = avg_time_per_batch * remaining_batches
                time_str = (
                    f", Elapsed: {elapsed_time:.1f}s, ETA: {estimated_remaining:.1f}s"
                )
            else:
                time_str = ""

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} - Codes {i + 1}-{batch_end}/{total_codes} ({progress_pct:.1f}%){time_str}"
            )

            try:
                # Process batch asynchronously
                results = asyncio.run(self.process_codes_batch(batch_codes))

                # Separate successful and failed results
                batch_successful = []
                batch_failed = 0
                for result_code, statements in results:
                    if statements:
                        batch_successful.append((result_code, statements))
                        successful_codes += 1
                    else:
                        failed_codes.append(result_code)
                        batch_failed += 1

                # Log batch fetch results
                self.logger.info(
                    f"Batch {batch_num}: Fetched {len(batch_successful)}/{len(batch_codes)} codes ({batch_failed} failed/no data)"
                )

                # Save successful results in batch
                if batch_successful:
                    records_saved = self.save_statements_batch(
                        db_path, batch_successful
                    )
                    total_records_saved += records_saved
                    self.logger.info(
                        f"Batch {batch_num}: Saved {records_saved} records to database"
                    )

            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num}: {e}")
                failed_codes.extend(batch_codes)

        # Final summary
        total_time = time.time() - start_time
        self.logger.info(
            f"Completed in {total_time:.1f}s: {successful_codes}/{total_codes} successful, {len(failed_codes)} failed/no data, {total_records_saved} total records saved"
        )

    def get_database_stats(self, db_path: str) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            with sqlite3.connect(db_path) as con:
                # Financial statements stats
                stmt_count_df = pd.read_sql(
                    "SELECT COUNT(*) as count FROM financial_statements", con
                )
                stmt_count = stmt_count_df.iloc[0]["count"]

                codes_df = pd.read_sql(
                    "SELECT COUNT(DISTINCT local_code) as code_count FROM financial_statements",
                    con,
                )
                code_count = codes_df.iloc[0]["code_count"]

                date_range_df = pd.read_sql(
                    "SELECT MIN(disclosed_date) as min_date, MAX(disclosed_date) as max_date FROM financial_statements",
                    con,
                )
                min_date = date_range_df.iloc[0]["min_date"]
                max_date = date_range_df.iloc[0]["max_date"]

                # Calculated fundamentals stats
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
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}


def setup_logging():
    """Setup logging for the statements processor."""
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


def main():
    """Main function for statements data processing."""
    logger = setup_logging()

    try:
        # Initialize processor
        processor = JQuantsStatementsProcessor(
            max_concurrent_requests=3, batch_size=100, request_delay=0.1
        )

        # Database path
        output_dir = project_root / "data"
        output_dir.mkdir(exist_ok=True)
        db_path = str(output_dir / "statements.db")

        logger.info("Starting statements data fetch...")
        processor.get_all_statements(db_path)

        # Display statistics
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

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
