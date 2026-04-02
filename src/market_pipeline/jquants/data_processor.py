"""
Optimized JQuants Data Processor with async processing and performance improvements.
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
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from typing import Optional, List, Dict, Tuple, Any, cast
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from market_pipeline.utils.parallel_processor import (
    BatchDatabaseProcessor,
    measure_performance,
)  # noqa: E402
from market_pipeline.utils.cache_manager import get_cache  # noqa: E402

load_dotenv()

API_URL = "https://api.jquants.com"


class JQuantsDataProcessor:
    """
    Optimized JQuants data processor with async processing, connection pooling,
    and batch operations for improved performance.
    """

    def __init__(
        self,
        refresh_token: Optional[str] = None,
        max_concurrent_requests: int = 3,
        batch_size: int = 100,
        request_delay: float = 0.1,
    ):
        """
        Initialize optimized JQuants data processor.

        Args:
            refresh_token: JQuants API refresh token
            max_concurrent_requests: Maximum concurrent API requests
            batch_size: Batch size for database operations
            request_delay: Delay between requests in seconds
        """
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.request_delay = request_delay

        # Setup logging first
        self.logger = logging.getLogger(__name__)

        # Cache manager
        self.cache = get_cache()

        # Database processor
        self.db_processor: Optional[BatchDatabaseProcessor] = None

        # Initialize tokens
        self._refresh_token = self._get_refresh_token()
        self._id_token = self._get_id_token()

    def _get_refresh_token(self) -> str:
        """Get refresh token from JQuants API."""
        data = {"mailaddress": os.getenv("EMAIL"), "password": os.getenv("PASSWORD")}
        res = requests.post(
            "https://api.jquants.com/v1/token/auth_user", data=json.dumps(data)
        )
        if res.status_code == 200:
            self.logger.info("Successfully obtained refresh token")
            return res.json()["refreshToken"]
        else:
            raise Exception(f"Failed to get refresh token: {res.json()['message']}")

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

        Returns:
            DataFrame with listed company information
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

    async def get_daily_quotes_async(
        self, session: aiohttp.ClientSession, code: str, from_date: str, to_date: str
    ) -> Tuple[str, pd.DataFrame]:
        """
        Async version of get_daily_quotes.

        Returns:
            Tuple of (code, DataFrame)
        """
        params = {
            "code": code,
            "from": from_date,
            "to": to_date,
        }

        try:
            async with session.get(
                f"{API_URL}/v1/prices/daily_quotes",
                params=params,
                headers=self._headers,
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    self.logger.warning(
                        f"Failed to get quotes for {code}: {error_text}"
                    )
                    return code, pd.DataFrame()

                d = await response.json()
                data = d["daily_quotes"]

                # Handle pagination
                while "pagination_key" in d:
                    params["pagination_key"] = d["pagination_key"]
                    async with session.get(
                        f"{API_URL}/v1/prices/daily_quotes",
                        params=params,
                        headers=self._headers,
                    ) as page_response:
                        if page_response.status != 200:
                            self.logger.warning(
                                f"Failed to get paginated quotes for {code}"
                            )
                            break
                        d = await page_response.json()
                        data += d["daily_quotes"]

                return code, pd.DataFrame(data)

        except Exception as e:
            self.logger.error(f"Error getting quotes for {code}: {e}")
            return code, pd.DataFrame()

    async def process_codes_batch(
        self, codes: List[str], from_date: str, to_date: str
    ) -> List[Tuple[str, pd.DataFrame]]:
        """
        Process a batch of stock codes concurrently.

        Args:
            codes: List of stock codes to process
            from_date: Start date
            to_date: End date

        Returns:
            List of (code, DataFrame) tuples
        """
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def process_with_semaphore(session, code):
            async with semaphore:
                result = await self.get_daily_quotes_async(
                    session, code, from_date, to_date
                )
                # Add delay to respect rate limits
                await asyncio.sleep(self.request_delay)
                return result

        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            tasks = [process_with_semaphore(session, code) for code in codes]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Filter out exceptions
            valid_results: List[Tuple[str, pd.DataFrame]] = []
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Task failed with exception: {result}")
                else:
                    valid_results.append(cast(Tuple[str, pd.DataFrame], result))

            return valid_results

    def get_last_dates_batch(self, db_path: str, codes: List[str]) -> Dict[str, str]:
        """
        Get last dates for multiple stock codes in a single query.

        Args:
            db_path: Database path
            codes: List of stock codes

        Returns:
            Dictionary mapping codes to their last dates
        """
        if not self.db_processor:
            self.db_processor = BatchDatabaseProcessor(db_path)

        try:
            # Get last dates for all codes in a single query
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

            # Create mapping
            last_dates = {}
            for _, row in results_df.iterrows():
                last_dates[row["Code"]] = row["last_date"]

            # Fill in missing codes with 5 years ago
            default_date = (datetime.now() - relativedelta(years=5)).strftime(
                "%Y-%m-%d"
            )
            for code in codes:
                if code not in last_dates:
                    last_dates[code] = default_date

            return last_dates

        except Exception as e:
            self.logger.warning(f"Error getting last dates batch: {e}")
            # Return default dates for all codes
            default_date = (datetime.now() - relativedelta(years=5)).strftime(
                "%Y-%m-%d"
            )
            return {code: default_date for code in codes}

    def save_quotes_batch(
        self, db_path: str, quotes_data: List[Tuple[str, pd.DataFrame]]
    ):
        """
        Save quotes data using batch operations.

        Args:
            db_path: Database path
            quotes_data: List of (code, DataFrame) tuples
        """
        if not self.db_processor:
            self.db_processor = BatchDatabaseProcessor(db_path)

        # Flatten all data into a single list
        all_records = []
        for result_code, df in quotes_data:
            if not df.empty:
                records = df.to_dict("records")
                all_records.extend(records)

        if all_records:
            # Batch insert all records
            inserted = self.db_processor.batch_insert(
                "daily_quotes", all_records, on_conflict="REPLACE"
            )
            self.logger.info(f"Batch inserted {inserted} records")

    @measure_performance
    def get_all_prices_for_past_5_years_to_db_optimized(self, db_path: str):
        """
        Optimized version of get_all_prices_for_past_5_years_to_db.

        Args:
            db_path: SQLite database path
        """
        self.logger.info("Starting optimized 5-year data fetch")
        start_time = time.time()

        # Initialize database
        self._initialize_database(db_path)

        # Get listed companies (cached)
        listed_info_df = self.get_listed_info_cached()

        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - relativedelta(years=5)).strftime("%Y-%m-%d")

        codes = [str(code) for code in listed_info_df["Code"].tolist()]
        total_codes = len(codes)

        self.logger.info(
            f"Processing {total_codes} codes from {from_date} to {to_date}"
        )

        # Process codes in batches
        successful_codes = 0
        failed_codes = []
        total_processed = 0
        total_records_saved = 0

        for i in range(0, total_codes, self.batch_size):
            batch_codes = codes[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_codes + self.batch_size - 1) // self.batch_size
            batch_start = i
            batch_end = min(i + self.batch_size, total_codes)
            progress_pct = (batch_end / total_codes) * 100

            # Calculate elapsed time and estimate remaining time
            elapsed_time = time.time() - start_time
            if total_processed > 0:
                avg_time_per_code = elapsed_time / total_processed
                remaining_codes = total_codes - batch_end
                estimated_remaining = avg_time_per_code * remaining_codes
                time_str = (
                    f", Elapsed: {elapsed_time:.1f}s, ETA: {estimated_remaining:.1f}s"
                )
            else:
                time_str = ""

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} - Codes {batch_start + 1}-{batch_end}/{total_codes} ({progress_pct:.1f}%){time_str}"
            )

            try:
                # Process batch asynchronously
                results = asyncio.run(
                    self.process_codes_batch(batch_codes, from_date, to_date)
                )

                # Separate successful and failed results
                batch_successful = []
                for result_code, df in results:
                    if not df.empty:
                        batch_successful.append((result_code, df))
                        successful_codes += 1
                    else:
                        failed_codes.append(result_code)

                # Save successful results in batch
                if batch_successful:
                    self.save_quotes_batch(db_path, batch_successful)
                    batch_records = sum(len(df) for _, df in batch_successful)
                    total_records_saved += batch_records
                    self.logger.info(
                        f"Batch {batch_num}: Saved {batch_records} records for {len(batch_successful)} codes | Total progress: {successful_codes}/{total_codes} codes ({(successful_codes / total_codes) * 100:.1f}%), {total_records_saved} total records"
                    )

                total_processed = batch_end

            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num}: {e}")
                failed_codes.extend(batch_codes)
                total_processed = batch_end

        # Final summary
        total_time = time.time() - start_time
        self.logger.info(
            f"Completed in {total_time:.1f}s: {successful_codes}/{total_codes} successful ({(successful_codes / total_codes) * 100:.1f}%), {len(failed_codes)} failed, {total_records_saved} total records saved"
        )
        if failed_codes:
            self.logger.warning(
                f"Failed codes: {failed_codes[:10]}{'...' if len(failed_codes) > 10 else ''}"
            )

    @measure_performance
    def update_prices_to_db_optimized(self, db_path: str):
        """
        Optimized version of update_prices_to_db.

        Args:
            db_path: SQLite database path
        """
        self.logger.info("Starting optimized price update")
        start_time = time.time()

        # Initialize database
        self._initialize_database(db_path)

        # Get listed companies (cached)
        listed_info_df = self.get_listed_info_cached()

        codes = [str(code) for code in listed_info_df["Code"].tolist()]
        total_codes = len(codes)
        to_date = datetime.now().strftime("%Y-%m-%d")

        # Get last dates for all codes in batch
        self.logger.info(f"Getting last dates for {total_codes} codes...")
        last_dates = self.get_last_dates_batch(db_path, codes)

        # Filter codes that need updates
        codes_to_update = []
        for code in codes:
            last_date = last_dates.get(code)
            if last_date:
                try:
                    last_datetime = datetime.strptime(last_date, "%Y-%m-%d")
                    from_date = (last_datetime + relativedelta(days=1)).strftime(
                        "%Y-%m-%d"
                    )
                    if from_date <= to_date:
                        codes_to_update.append((code, from_date))
                except ValueError:
                    # Invalid date, update from 5 years ago
                    from_date = (datetime.now() - relativedelta(years=5)).strftime(
                        "%Y-%m-%d"
                    )
                    codes_to_update.append((code, from_date))

        self.logger.info(
            f"{len(codes_to_update)}/{total_codes} codes need updates ({(len(codes_to_update) / total_codes * 100):.1f}%)"
        )

        if not codes_to_update:
            self.logger.info("All data is up to date")
            return

        # Group codes by date range for efficient processing
        date_groups: Dict[str, List[str]] = {}
        for code, from_date in codes_to_update:
            if from_date not in date_groups:
                date_groups[from_date] = []
            date_groups[from_date].append(code)

        successful_codes = 0
        failed_codes = []
        updated_codes = 0
        total_records_updated = 0
        codes_processed = 0

        # Process each date group
        for group_idx, (from_date, group_codes) in enumerate(date_groups.items()):
            group_start_codes = codes_processed
            self.logger.info(
                f"Date group {group_idx + 1}/{len(date_groups)}: Processing {len(group_codes)} codes from {from_date}"
            )

            # Process in batches
            for i in range(0, len(group_codes), self.batch_size):
                batch_codes = group_codes[i : i + self.batch_size]
                batch_start = group_start_codes + i
                batch_end = min(batch_start + len(batch_codes), len(codes_to_update))
                progress_pct = (batch_end / len(codes_to_update)) * 100

                # Calculate elapsed time and estimate remaining time
                elapsed_time = time.time() - start_time
                if codes_processed > 0:
                    avg_time_per_code = elapsed_time / codes_processed
                    remaining_codes = len(codes_to_update) - batch_end
                    estimated_remaining = avg_time_per_code * remaining_codes
                    time_str = f", Elapsed: {elapsed_time:.1f}s, ETA: {estimated_remaining:.1f}s"
                else:
                    time_str = ""

                self.logger.info(
                    f"Processing codes {batch_start + 1}-{batch_end}/{len(codes_to_update)} ({progress_pct:.1f}%){time_str}"
                )

                try:
                    # Process batch asynchronously
                    results = asyncio.run(
                        self.process_codes_batch(batch_codes, from_date, to_date)
                    )

                    # Separate successful and failed results
                    batch_successful = []
                    for result_code, df in results:
                        if not df.empty:
                            batch_successful.append((result_code, df))
                            successful_codes += 1
                            updated_codes += 1
                        else:
                            successful_codes += 1  # No data is not an error

                    # Save successful results in batch
                    if batch_successful:
                        self.save_quotes_batch(db_path, batch_successful)
                        batch_records = sum(len(df) for _, df in batch_successful)
                        total_records_updated += batch_records
                        self.logger.info(
                            f"Batch updated: {batch_records} records for {len(batch_successful)} codes | Total progress: {successful_codes}/{len(codes_to_update)} codes ({(successful_codes / len(codes_to_update)) * 100:.1f}%), {total_records_updated} total records"
                        )

                    codes_processed += len(batch_codes)

                except Exception as e:
                    self.logger.error(f"Error processing batch: {e}")
                    failed_codes.extend(batch_codes)
                    codes_processed += len(batch_codes)

            codes_processed = group_start_codes + len(group_codes)

        # Final summary
        total_time = time.time() - start_time
        self.logger.info(
            f"Update completed in {total_time:.1f}s: {successful_codes}/{len(codes_to_update)} processed ({(successful_codes / len(codes_to_update)) * 100:.1f}%), {updated_codes} updated with {total_records_updated} records, {len(failed_codes)} failed"
        )

    def _initialize_database(self, db_path: str):
        """Initialize database with optimized settings."""
        if not self.db_processor:
            self.db_processor = BatchDatabaseProcessor(db_path)

        with sqlite3.connect(db_path) as con:
            # Enable optimizations
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA cache_size=10000")

            # Create table if not exists
            con.execute("""
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
                    PRIMARY KEY (Code, Date)
                )
            """)
            con.commit()

    def get_database_stats(self, db_path: str) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            with sqlite3.connect(db_path) as con:
                # Record count
                count_df = pd.read_sql(
                    "SELECT COUNT(*) as count FROM daily_quotes", con
                )
                record_count = count_df.iloc[0]["count"]

                # Unique codes count
                codes_df = pd.read_sql(
                    "SELECT COUNT(DISTINCT Code) as code_count FROM daily_quotes", con
                )
                code_count = codes_df.iloc[0]["code_count"]

                # Date range
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
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}


def setup_logging():
    """Setup logging for the optimized processor."""
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


def main():
    """
    Main function for optimized JQuants data processing.
    """
    logger = setup_logging()

    try:
        # Initialize optimized processor
        processor = JQuantsDataProcessor(
            max_concurrent_requests=3,  # Adjust based on API limits
            batch_size=100,
            request_delay=0.1,
        )

        # Database path
        output_dir = Path(__file__).parent.parent.parent / "data"
        output_dir.mkdir(exist_ok=True)
        db_path = str(output_dir / "jquants.db")

        # Check if database exists
        db_exists = os.path.exists(db_path)

        if not db_exists:
            logger.info("Database does not exist. Fetching 5 years of data...")
            processor.get_all_prices_for_past_5_years_to_db_optimized(db_path)
        else:
            logger.info("Database exists. Performing incremental update...")
            processor.update_prices_to_db_optimized(db_path)

        # Display statistics
        stats = processor.get_database_stats(db_path)
        if stats:
            logger.info("Database statistics:")
            logger.info(f"  Records: {stats.get('record_count', 'N/A')}")
            logger.info(f"  Codes: {stats.get('code_count', 'N/A')}")
            logger.info(f"  Date range: {stats.get('date_range', 'N/A')}")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        logger.error("Please check your .env file and JQUANTS credentials")


if __name__ == "__main__":
    main()
