"""
yfinance Valuation Fetcher

Fetches balance sheet data (cash, debt), market cap, and PER from yfinance
for all active stocks in master.db. Uses rolling updates to process a subset
of stocks each day, prioritizing stocks with stale or missing data.
"""

import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)


class ValuationFetcher:
    """Fetches and stores valuation metrics from yfinance."""

    def __init__(
        self,
        master_db_path: Optional[str] = None,
        statements_db_path: Optional[str] = None,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
        wait_seconds: Optional[float] = None,
    ):
        settings = get_settings()
        self.master_db_path = master_db_path or str(settings.paths.master_db)
        self.statements_db_path = statements_db_path or str(
            settings.paths.statements_db
        )
        self.batch_size = (
            batch_size
            if batch_size is not None
            else settings.yfinance.valuation_batch_size
        )
        self.max_workers = (
            max_workers
            if max_workers is not None
            else settings.yfinance.valuation_max_workers
        )
        self.wait_seconds = (
            wait_seconds
            if wait_seconds is not None
            else settings.yfinance.valuation_wait_seconds
        )

    def initialize_table(self) -> None:
        """Create yfinance_valuation table if it does not exist."""
        with sqlite3.connect(self.statements_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS yfinance_valuation (
                    code TEXT PRIMARY KEY,
                    cash_and_equivalents REAL,
                    interest_bearing_debt REAL,
                    bs_period_end TEXT,
                    market_cap REAL,
                    per REAL,
                    net_cash_ratio REAL,
                    cash_neutral_per REAL,
                    bs_updated_at TEXT,
                    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
                )
                """
            )
            conn.commit()

    def select_target_codes(self, limit: Optional[int] = None) -> list[str]:
        """Select target stock codes for rolling update.

        Priority:
        1. Not yet in yfinance_valuation (ordered by PER asc, NULL last)
        2. bs_period_end older than 90 days (ordered by PER asc, NULL last)
        3. bs_updated_at oldest first
        """
        limit = limit or self.batch_size

        # Get all active stocks from master.db
        with sqlite3.connect(self.master_db_path) as conn:
            master_df_rows = conn.execute(
                "SELECT code, yfinance_symbol FROM stocks_master WHERE is_active = 1"
            ).fetchall()
        all_codes = {row[0]: row[1] for row in master_df_rows}

        if not all_codes:
            return []

        # Get existing valuation data
        with sqlite3.connect(self.statements_db_path) as conn:
            # Ensure table exists
            self.initialize_table()

            existing_rows = conn.execute(
                "SELECT code, bs_period_end, bs_updated_at FROM yfinance_valuation"
            ).fetchall()
        existing = {row[0]: (row[1], row[2]) for row in existing_rows}

        # Get PER from calculated_fundamentals for sorting
        per_map: dict[str, Optional[float]] = {}
        try:
            with sqlite3.connect(self.statements_db_path) as conn:
                per_rows = conn.execute(
                    "SELECT code, per FROM calculated_fundamentals"
                ).fetchall()
                per_map = {row[0][:4]: row[1] for row in per_rows}
        except sqlite3.OperationalError:
            pass  # Table may not exist

        def per_sort_key(code: str) -> tuple[int, float]:
            """Sort by PER ascending, NULL last."""
            per = per_map.get(code)
            if per is None or per <= 0:
                return (1, 0.0)
            return (0, per)

        now = datetime.now()
        threshold = now - timedelta(days=90)
        threshold_str = threshold.strftime("%Y-%m-%d")

        # Group 1: Not yet in yfinance_valuation
        group1 = [c for c in all_codes if c not in existing]
        group1.sort(key=per_sort_key)

        # Group 2: bs_period_end older than 90 days
        group2 = []
        for code, (bs_end, _) in existing.items():
            if code in all_codes and bs_end and bs_end < threshold_str:
                group2.append(code)
        group2.sort(key=per_sort_key)

        # Group 3: Rest, sorted by bs_updated_at ascending
        group3_codes = set(all_codes) - set(group1) - set(group2)
        group3 = sorted(
            group3_codes,
            key=lambda c: existing.get(c, (None, "9999-12-31"))[1] or "9999-12-31",
        )

        result = (group1 + group2 + group3)[:limit]
        return result

    def fetch_single(self, symbol: str) -> Optional[dict]:
        """Fetch BS data, market cap, and PER for a single stock from yfinance.

        Args:
            symbol: yfinance symbol (e.g., "7203.T")

        Returns:
            Dict with fetched data, or None on failure.
        """
        code = symbol.replace(".T", "")
        try:
            ticker = yf.Ticker(symbol)

            # Get balance sheet
            bs = ticker.balance_sheet
            cash = None
            debt = None
            bs_period_end = None

            if bs is not None and not bs.empty:
                # Latest column is most recent period
                latest = bs.iloc[:, 0]
                col = bs.columns[0]
                bs_period_end = str(col.date()) if hasattr(col, "date") else str(col)

                # Cash And Cash Equivalents
                for key in ["Cash And Cash Equivalents", "CashAndCashEquivalents"]:
                    if key in latest.index:
                        val = latest[key]
                        if val is not None and not pd.isna(val):
                            cash = float(val)
                        break

                # Total Debt
                for key in ["Total Debt", "TotalDebt"]:
                    if key in latest.index:
                        val = latest[key]
                        if val is not None and not pd.isna(val):
                            debt = float(val)
                        break

            # Get info
            info = ticker.info or {}
            market_cap = info.get("marketCap")
            per = info.get("trailingPE")

            # Convert to float safely
            if market_cap is not None:
                market_cap = float(market_cap)
            if per is not None:
                per = float(per)

            return {
                "code": code,
                "cash_and_equivalents": cash,
                "interest_bearing_debt": debt,
                "bs_period_end": bs_period_end,
                "market_cap": market_cap,
                "per": per,
            }

        except Exception as e:
            logger.error(f"Failed to fetch data for {symbol}: {e}")
            return None

    def calculate_metrics(self, row: dict) -> dict:
        """Calculate net_cash_ratio and cash_neutral_per.

        - market_cap=0/None -> net_cash_ratio=None
        - cash_and_equivalents=None -> net_cash_ratio=None
        - per=None -> cash_neutral_per=None
        """
        row = dict(row)  # Don't mutate input

        cash = row.get("cash_and_equivalents")
        debt = row.get("interest_bearing_debt")
        market_cap = row.get("market_cap")
        per = row.get("per")

        net_cash_ratio = None
        cash_neutral_per = None

        if cash is not None and market_cap and market_cap > 0:
            debt_val = debt if debt is not None else 0.0
            net_cash_ratio = (cash - debt_val) / market_cap

        if net_cash_ratio is not None and per is not None:
            cash_neutral_per = per * (1 - net_cash_ratio)

        row["net_cash_ratio"] = net_cash_ratio
        row["cash_neutral_per"] = cash_neutral_per
        return row

    def save_batch(self, records: list[dict]) -> int:
        """Save records to yfinance_valuation table using INSERT OR REPLACE.

        Returns:
            Number of records saved.
        """
        if not records:
            return 0

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.statements_db_path) as conn:
            for record in records:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO yfinance_valuation
                    (code, cash_and_equivalents, interest_bearing_debt,
                     bs_period_end, market_cap, per,
                     net_cash_ratio, cash_neutral_per, bs_updated_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["code"],
                        record.get("cash_and_equivalents"),
                        record.get("interest_bearing_debt"),
                        record.get("bs_period_end"),
                        record.get("market_cap"),
                        record.get("per"),
                        record.get("net_cash_ratio"),
                        record.get("cash_neutral_per"),
                        now,
                        now,
                    ),
                )
            conn.commit()
        return len(records)

    def run(self, limit: Optional[int] = None) -> dict:
        """Run the rolling valuation update.

        Returns:
            Dict with keys: success, failed, skipped, elapsed
        """
        start_time = time.time()

        self.initialize_table()

        target_codes = self.select_target_codes(limit=limit)
        if not target_codes:
            logger.info("No target codes for valuation update")
            return {"success": 0, "failed": 0, "skipped": 0, "elapsed": 0.0}

        # Build code -> symbol mapping
        with sqlite3.connect(self.master_db_path) as conn:
            rows = conn.execute(
                "SELECT code, yfinance_symbol FROM stocks_master WHERE is_active = 1"
            ).fetchall()
        symbol_map = {row[0]: row[1] for row in rows}

        success = 0
        failed = 0
        skipped = 0
        records: list[dict] = []

        def _fetch_with_wait(symbol: str) -> Optional[dict]:
            result = self.fetch_single(symbol)
            time.sleep(self.wait_seconds)
            return result

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            for code in target_codes:
                symbol = symbol_map.get(code)
                if not symbol:
                    skipped += 1
                    continue
                future = executor.submit(_fetch_with_wait, symbol)
                futures[future] = code

            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    if result is None:
                        failed += 1
                        continue
                    result = self.calculate_metrics(result)
                    records.append(result)
                    success += 1
                except Exception as e:
                    logger.error(f"Error processing {code}: {e}")
                    failed += 1

        saved = self.save_batch(records)
        elapsed = time.time() - start_time

        logger.info(
            f"Valuation update completed: success={success}, failed={failed}, "
            f"skipped={skipped}, saved={saved}, elapsed={elapsed:.1f}s"
        )

        return {
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "elapsed": elapsed,
        }
