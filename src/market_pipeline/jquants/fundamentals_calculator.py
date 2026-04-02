"""
Fundamentals Calculator for computing derived financial metrics from statements data.
"""

import sqlite3
import pandas as pd
import logging
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Tuple
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from market_pipeline.utils.cache_manager import get_cache  # noqa: E402


class FundamentalsCalculator:
    """
    Calculate derived financial metrics from raw statement data.
    Combines statement data with price data from jquants.db.
    """

    def __init__(
        self, statements_db_path: str, jquants_db_path: str, master_db_path: str = None
    ):
        """
        Initialize the fundamentals calculator.

        Args:
            statements_db_path: Path to statements.db (financial_statements table)
            jquants_db_path: Path to jquants.db (daily_quotes table)
            master_db_path: Path to master.db (stocks_master table). If None, inferred from jquants_db_path.
        """
        self.statements_db_path = statements_db_path
        self.jquants_db_path = jquants_db_path
        # Infer master_db_path from jquants_db_path if not provided
        if master_db_path is None:
            data_dir = Path(jquants_db_path).parent
            master_db_path = str(data_dir / "master.db")
        self.master_db_path = master_db_path
        self.logger = logging.getLogger(__name__)
        self.cache = get_cache()

    def get_latest_price(self, code: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Get the latest closing price for a stock from jquants.db.

        Args:
            code: Stock code (5 digits)

        Returns:
            Tuple of (price, date) or (None, None) if not found
        """
        try:
            with sqlite3.connect(self.jquants_db_path) as conn:
                query = """
                SELECT AdjustmentClose, Date
                FROM daily_quotes
                WHERE Code = ?
                ORDER BY Date DESC
                LIMIT 1
                """
                result = conn.execute(query, (code,)).fetchone()
                if result:
                    return result[0], result[1]
                return None, None
        except Exception as e:
            self.logger.error(f"Error getting latest price for {code}: {e}")
            return None, None

    def get_latest_prices_batch(self) -> Dict[str, Tuple[float, str]]:
        """
        Get latest closing prices for all stocks in batch.

        Returns:
            Dictionary mapping code to (price, date) tuple
        """
        try:
            with sqlite3.connect(self.jquants_db_path) as conn:
                query = """
                SELECT Code, AdjustmentClose, Date
                FROM daily_quotes dq1
                WHERE Date = (
                    SELECT MAX(Date) FROM daily_quotes dq2 WHERE dq2.Code = dq1.Code
                )
                """
                df = pd.read_sql(query, conn)

                prices = {}
                for _, row in df.iterrows():
                    prices[row["Code"]] = (row["AdjustmentClose"], row["Date"])
                return prices
        except Exception as e:
            self.logger.error(f"Error getting batch prices: {e}")
            return {}

    def get_latest_statement(self, code: str) -> Optional[Dict]:
        """
        Get the most recent financial statement for a stock.
        Prefers FY (full year) data, falls back to latest quarterly.

        Args:
            code: Stock code (5 digits)

        Returns:
            Dictionary with statement data or None if not found
        """
        try:
            with sqlite3.connect(self.statements_db_path) as conn:
                # First try to get the latest FY statement
                query = """
                SELECT *
                FROM financial_statements
                WHERE local_code = ? AND type_of_current_period = 'FY'
                ORDER BY disclosed_date DESC
                LIMIT 1
                """
                result = conn.execute(query, (code,)).fetchone()

                if result:
                    columns = [
                        desc[0] for desc in conn.execute(query, (code,)).description
                    ]
                    return dict(zip(columns, result))

                # Fall back to latest quarterly
                query = """
                SELECT *
                FROM financial_statements
                WHERE local_code = ?
                ORDER BY disclosed_date DESC
                LIMIT 1
                """
                result = conn.execute(query, (code,)).fetchone()

                if result:
                    columns = [
                        desc[0] for desc in conn.execute(query, (code,)).description
                    ]
                    return dict(zip(columns, result))

                return None
        except Exception as e:
            self.logger.error(f"Error getting latest statement for {code}: {e}")
            return None

    def get_all_latest_statements(self) -> pd.DataFrame:
        """
        Get latest statements for all codes in batch.
        Prefers FY data, falls back to latest quarterly.

        Returns:
            DataFrame with latest statements per code
        """
        try:
            with sqlite3.connect(self.statements_db_path) as conn:
                # Get latest statement per code, preferring FY
                query = """
                WITH ranked_statements AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY local_code
                            ORDER BY
                                CASE WHEN type_of_current_period = 'FY' THEN 0 ELSE 1 END,
                                disclosed_date DESC
                        ) as rn
                    FROM financial_statements
                )
                SELECT * FROM ranked_statements WHERE rn = 1
                """
                df = pd.read_sql(query, conn)
                return df
        except Exception as e:
            self.logger.error(f"Error getting batch statements: {e}")
            return pd.DataFrame()

    def get_shares_for_codes(self, codes: list) -> Dict[str, float]:
        """
        Get number_of_shares from historical data for codes missing this value.

        For each code, finds the most recent non-null number_of_shares value
        from any statement (FY or quarterly).

        Args:
            codes: List of stock codes to look up

        Returns:
            Dictionary mapping code to number_of_shares
        """
        if not codes:
            return {}

        try:
            with sqlite3.connect(self.statements_db_path) as conn:
                placeholders = ",".join(["?" for _ in codes])
                query = f"""
                WITH ranked_shares AS (
                    SELECT
                        local_code,
                        number_of_shares,
                        ROW_NUMBER() OVER (
                            PARTITION BY local_code
                            ORDER BY disclosed_date DESC
                        ) as rn
                    FROM financial_statements
                    WHERE local_code IN ({placeholders})
                      AND number_of_shares IS NOT NULL
                      AND number_of_shares != ''
                )
                SELECT local_code, number_of_shares
                FROM ranked_shares
                WHERE rn = 1
                """
                result = conn.execute(query, codes).fetchall()
                return {row[0]: float(row[1]) for row in result if row[1]}
        except Exception as e:
            self.logger.error(f"Error getting shares for codes: {e}")
            return {}

    def get_listed_info_cached(self) -> pd.DataFrame:
        """
        Get listed company info from cache, falling back to master.db.

        Returns:
            DataFrame with company info (Code, CompanyName, Sector33CodeName, Sector17CodeName, MarketCodeName)
        """
        cache_key = "jquants_listed_info"
        cached_data = self.cache.get(cache_key)

        if cached_data is not None:
            return pd.DataFrame(cached_data)

        # Fallback: load from master.db
        self.logger.info("Listed info not in cache - loading from master.db")
        return self._get_listed_info_from_master()

    def _get_listed_info_from_master(self) -> pd.DataFrame:
        """
        Get listed company info directly from master.db.

        Note: master.db uses 4-digit codes, we convert to 5-digit (append '0').

        Returns:
            DataFrame with company info
        """
        try:
            with sqlite3.connect(self.master_db_path) as conn:
                query = """
                SELECT
                    code || '0' as Code,
                    name as CompanyName,
                    sector as Sector33CodeName,
                    sector as Sector17CodeName,
                    market as MarketCodeName
                FROM stocks_master
                WHERE is_active = 1
                """
                df = pd.read_sql(query, conn)
                self.logger.info(f"Loaded {len(df)} records from master.db")
                return df
        except Exception as e:
            self.logger.error(f"Error loading from master.db: {e}")
            return pd.DataFrame()

    @staticmethod
    def _to_float(value) -> Optional[float]:
        """
        Safely convert a value to float.
        Handles strings like "-", "", None, and other non-numeric values.

        Args:
            value: Value to convert

        Returns:
            Float value or None if conversion not possible
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip()
            if value in ("", "-", "--", "N/A", "NA", "null", "None"):
                return None
            try:
                return float(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def calculate_per(price: Optional[float], eps: Optional[float]) -> Optional[float]:
        """
        Calculate Price-to-Earnings Ratio.
        PER = Price / EPS

        Args:
            price: Stock price
            eps: Earnings per share

        Returns:
            PER or None if calculation not possible
        """
        price = FundamentalsCalculator._to_float(price)
        eps = FundamentalsCalculator._to_float(eps)
        if price is None or eps is None or eps == 0:
            return None
        return round(price / eps, 2)

    @staticmethod
    def calculate_pbr(price: Optional[float], bps: Optional[float]) -> Optional[float]:
        """
        Calculate Price-to-Book Ratio.
        PBR = Price / BPS

        Args:
            price: Stock price
            bps: Book value per share

        Returns:
            PBR or None if calculation not possible
        """
        price = FundamentalsCalculator._to_float(price)
        bps = FundamentalsCalculator._to_float(bps)
        if price is None or bps is None or bps == 0:
            return None
        return round(price / bps, 2)

    @staticmethod
    def calculate_roe(
        profit: Optional[float], equity: Optional[float]
    ) -> Optional[float]:
        """
        Calculate Return on Equity.
        ROE = Profit / Equity * 100

        Args:
            profit: Net profit
            equity: Shareholders' equity

        Returns:
            ROE (%) or None if calculation not possible
        """
        profit = FundamentalsCalculator._to_float(profit)
        equity = FundamentalsCalculator._to_float(equity)
        if profit is None or equity is None or equity == 0:
            return None
        return round((profit / equity) * 100, 2)

    @staticmethod
    def calculate_roa(
        profit: Optional[float], total_assets: Optional[float]
    ) -> Optional[float]:
        """
        Calculate Return on Assets.
        ROA = Profit / Total Assets * 100

        Args:
            profit: Net profit
            total_assets: Total assets

        Returns:
            ROA (%) or None if calculation not possible
        """
        profit = FundamentalsCalculator._to_float(profit)
        total_assets = FundamentalsCalculator._to_float(total_assets)
        if profit is None or total_assets is None or total_assets == 0:
            return None
        return round((profit / total_assets) * 100, 2)

    @staticmethod
    def calculate_dividend_yield(
        dps: Optional[float], price: Optional[float]
    ) -> Optional[float]:
        """
        Calculate Dividend Yield.
        Yield = DPS / Price * 100

        Args:
            dps: Dividend per share (annual)
            price: Stock price

        Returns:
            Dividend yield (%) or None if calculation not possible
        """
        dps = FundamentalsCalculator._to_float(dps)
        price = FundamentalsCalculator._to_float(price)
        if dps is None or price is None or price == 0:
            return None
        return round((dps / price) * 100, 2)

    @staticmethod
    def calculate_market_cap(
        price: Optional[float], shares: Optional[float]
    ) -> Optional[float]:
        """
        Calculate Market Capitalization.
        Market Cap = Price * Shares Outstanding

        Args:
            price: Stock price
            shares: Number of shares outstanding

        Returns:
            Market cap or None if calculation not possible
        """
        price = FundamentalsCalculator._to_float(price)
        shares = FundamentalsCalculator._to_float(shares)
        if price is None or shares is None:
            return None
        return price * shares

    @staticmethod
    def calculate_operating_margin(
        operating_profit: Optional[float], net_sales: Optional[float]
    ) -> Optional[float]:
        """
        Calculate Operating Margin.
        Operating Margin = Operating Profit / Net Sales * 100

        Args:
            operating_profit: Operating profit
            net_sales: Net sales (revenue)

        Returns:
            Operating margin (%) or None if calculation not possible
        """
        operating_profit = FundamentalsCalculator._to_float(operating_profit)
        net_sales = FundamentalsCalculator._to_float(net_sales)
        if operating_profit is None or net_sales is None or net_sales == 0:
            return None
        return round((operating_profit / net_sales) * 100, 2)

    @staticmethod
    def calculate_profit_margin(
        profit: Optional[float], net_sales: Optional[float]
    ) -> Optional[float]:
        """
        Calculate Net Profit Margin.
        Profit Margin = Profit / Net Sales * 100

        Args:
            profit: Net profit
            net_sales: Net sales (revenue)

        Returns:
            Profit margin (%) or None if calculation not possible
        """
        profit = FundamentalsCalculator._to_float(profit)
        net_sales = FundamentalsCalculator._to_float(net_sales)
        if profit is None or net_sales is None or net_sales == 0:
            return None
        return round((profit / net_sales) * 100, 2)

    @staticmethod
    def calculate_free_cash_flow(
        cf_operating: Optional[float], cf_investing: Optional[float]
    ) -> Optional[float]:
        """
        Calculate Free Cash Flow.
        FCF = Operating CF + Investing CF
        (Investing CF is typically negative)

        Args:
            cf_operating: Cash flow from operating activities
            cf_investing: Cash flow from investing activities

        Returns:
            Free cash flow or None if calculation not possible
        """
        cf_operating = FundamentalsCalculator._to_float(cf_operating)
        cf_investing = FundamentalsCalculator._to_float(cf_investing)
        if cf_operating is None or cf_investing is None:
            return None
        return cf_operating + cf_investing

    def calculate_all_fundamentals(
        self,
        code: str,
        statement: Dict,
        price: float,
        price_date: str,
        listed_info: Optional[Dict],
    ) -> Dict:
        """
        Calculate all fundamental metrics for a single stock.

        Args:
            code: Stock code
            statement: Statement data dictionary
            price: Latest stock price
            price_date: Date of the price
            listed_info: Company info from listed_info API

        Returns:
            Dictionary with all calculated metrics
        """
        # Extract values from statement
        eps = statement.get("earnings_per_share")
        bps = statement.get("book_value_per_share")
        profit = statement.get("profit")
        equity = statement.get("equity")
        total_assets = statement.get("total_assets")
        net_sales = statement.get("net_sales")
        operating_profit = statement.get("operating_profit")
        ordinary_profit = statement.get("ordinary_profit")
        dps = statement.get("result_dividend_per_share_annual")
        shares = statement.get("number_of_shares")
        cf_operating = statement.get("cf_operating")
        cf_investing = statement.get("cf_investing")
        equity_ratio = statement.get("equity_to_asset_ratio")
        payout_ratio = statement.get("payout_ratio_annual")
        forecast_eps = statement.get("forecast_earnings_per_share")

        # Company info
        company_name = listed_info.get("CompanyName") if listed_info else None
        sector_33 = listed_info.get("Sector33CodeName") if listed_info else None
        sector_17 = listed_info.get("Sector17CodeName") if listed_info else None
        market_segment = listed_info.get("MarketCodeName") if listed_info else None

        return {
            "code": code,
            "company_name": company_name,
            "sector_33": sector_33,
            "sector_17": sector_17,
            "market_segment": market_segment,
            "latest_period": statement.get("type_of_current_period"),
            "latest_fiscal_year_end": statement.get("current_fiscal_year_end_date"),
            "latest_disclosed_date": statement.get("disclosed_date"),
            # Valuation
            "market_cap": self.calculate_market_cap(price, shares),
            "per": self.calculate_per(price, eps),
            "forward_per": self.calculate_per(price, forecast_eps),
            "pbr": self.calculate_pbr(price, bps),
            "dividend_yield": self.calculate_dividend_yield(dps, price),
            # Profitability
            "roe": self.calculate_roe(profit, equity),
            "roa": self.calculate_roa(profit, total_assets),
            "equity_ratio": equity_ratio,
            "operating_margin": self.calculate_operating_margin(
                operating_profit, net_sales
            ),
            "profit_margin": self.calculate_profit_margin(profit, net_sales),
            # Per Share
            "eps": eps,
            "bps": bps,
            "dps": dps,
            # Balance Sheet
            "total_assets": total_assets,
            "equity": equity,
            # Cash Flow
            "operating_cf": cf_operating,
            "free_cash_flow": self.calculate_free_cash_flow(cf_operating, cf_investing),
            # Raw Financial Data
            "net_sales": net_sales,
            "operating_profit": operating_profit,
            "ordinary_profit": ordinary_profit,
            "profit": profit,
            # Payout
            "payout_ratio": payout_ratio,
            # Reference
            "reference_price": price,
            "reference_date": price_date,
        }

    def update_all_fundamentals(self, output_db_path: str) -> int:
        """
        Calculate fundamentals for all stocks and save to database.

        Args:
            output_db_path: Path to output database (statements.db)

        Returns:
            Number of stocks processed
        """
        self.logger.info("Starting fundamentals calculation for all stocks")
        start_time = time.time()

        # Get all data in batch
        self.logger.info("Fetching latest prices...")
        prices = self.get_latest_prices_batch()
        self.logger.info(f"Got prices for {len(prices)} stocks")

        self.logger.info("Fetching latest statements...")
        statements_df = self.get_all_latest_statements()
        self.logger.info(f"Got statements for {len(statements_df)} stocks")

        # Find codes with missing number_of_shares and fetch from historical data
        missing_shares_codes = []
        for _, row in statements_df.iterrows():
            shares = self._to_float(row.get("number_of_shares"))
            if shares is None:
                missing_shares_codes.append(row["local_code"])

        shares_lookup = {}
        if missing_shares_codes:
            self.logger.info(
                f"Fetching historical shares data for {len(missing_shares_codes)} codes with missing number_of_shares..."
            )
            shares_lookup = self.get_shares_for_codes(missing_shares_codes)
            self.logger.info(
                f"Found historical shares data for {len(shares_lookup)} codes"
            )

        self.logger.info("Fetching listed info...")
        listed_info_df = self.get_listed_info_cached()
        # Create lookup dict
        listed_info_dict = {}
        if not listed_info_df.empty:
            for _, row in listed_info_df.iterrows():
                listed_info_dict[str(row["Code"])] = row.to_dict()
        self.logger.info(f"Got listed info for {len(listed_info_dict)} stocks")

        # Calculate fundamentals for each stock with statement data
        results = []
        processed = 0
        skipped_no_price = 0
        shares_supplemented_count = 0

        for _, row in statements_df.iterrows():
            code = row["local_code"]

            # Get price
            if code not in prices:
                skipped_no_price += 1
                continue

            price, price_date = prices[code]
            if price is None:
                skipped_no_price += 1
                continue

            # Get listed info
            listed_info = listed_info_dict.get(code)

            # Convert row to dict for modification
            statement_dict = row.to_dict()

            # 補完: number_of_shares がNULLの場合、過去のデータから取得
            current_shares = self._to_float(statement_dict.get("number_of_shares"))
            if current_shares is None and code in shares_lookup:
                statement_dict["number_of_shares"] = shares_lookup[code]
                shares_supplemented_count += 1

            # Calculate fundamentals
            fundamentals = self.calculate_all_fundamentals(
                code=code,
                statement=statement_dict,
                price=price,
                price_date=price_date,
                listed_info=listed_info,
            )
            results.append(fundamentals)
            processed += 1

        self.logger.info(
            f"Calculated fundamentals for {processed} stocks (skipped {skipped_no_price} with no price)"
        )
        self.logger.info(
            f"Supplemented number_of_shares from historical data for {shares_supplemented_count} stocks"
        )

        # Save to database
        if results:
            self._save_fundamentals_batch(output_db_path, results)

        total_time = time.time() - start_time
        self.logger.info(f"Fundamentals calculation completed in {total_time:.1f}s")

        return processed

    def _save_fundamentals_batch(self, db_path: str, fundamentals: list) -> int:
        """
        Batch save calculated fundamentals to database.

        Args:
            db_path: Database path
            fundamentals: List of fundamentals dictionaries

        Returns:
            Number of records saved
        """
        if not fundamentals:
            return 0

        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")

            columns = list(fundamentals[0].keys())
            placeholders = ",".join(["?" for _ in columns])
            column_names = ",".join(columns)

            query = f"INSERT OR REPLACE INTO calculated_fundamentals ({column_names}) VALUES ({placeholders})"

            values = [tuple(f.get(col) for col in columns) for f in fundamentals]
            conn.executemany(query, values)
            conn.commit()

        self.logger.info(f"Saved {len(fundamentals)} calculated fundamentals records")
        return len(fundamentals)


def setup_logging():
    """Setup logging for the fundamentals calculator."""
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                log_dir / f"fundamentals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            ),
        ],
    )
    return logging.getLogger(__name__)


def main():
    """Main function for fundamentals calculation."""
    logger = setup_logging()

    try:
        # Database paths
        data_dir = project_root / "data"
        statements_db_path = str(data_dir / "statements.db")
        jquants_db_path = str(data_dir / "jquants.db")

        logger.info("Starting fundamentals calculation...")

        calculator = FundamentalsCalculator(
            statements_db_path=statements_db_path, jquants_db_path=jquants_db_path
        )

        processed = calculator.update_all_fundamentals(statements_db_path)
        logger.info(f"Processed {processed} stocks")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
