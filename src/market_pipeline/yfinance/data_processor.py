"""
TSE Stock Data Processor

Fetches TSE stock data from yfinance and stores it in SQLite database.
"""

import datetime
import logging
import sqlite3
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from market_pipeline.config import get_settings

warnings.filterwarnings("ignore")

# Get settings
_settings = get_settings()
DATA_DIR = str(_settings.paths.data_dir)
DB_PATH = str(_settings.paths.yfinance_db)

# Logging
logging.basicConfig(
    level=getattr(logging, _settings.logging.level),
    format=_settings.logging.format,
)
logger = logging.getLogger(__name__)

# Ensure data directory exists
if _settings.paths.data_dir is not None:
    _settings.paths.data_dir.mkdir(parents=True, exist_ok=True)


def init_db(db_path: str):
    """データベースとテーブルを初期化する"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            ticker TEXT PRIMARY KEY,
            longName TEXT,
            sector TEXT,
            industry TEXT,
            marketCap INTEGER,
            trailingPE REAL,
            forwardPE REAL,
            dividendYield REAL,
            website TEXT,
            currentPrice REAL,
            regularMarketPrice REAL,
            currency TEXT,
            exchange TEXT,
            shortName TEXT,
            previousClose REAL,
            open REAL,
            dayLow REAL,
            dayHigh REAL,
            volume INTEGER,
            averageDailyVolume10Day INTEGER,
            averageDailyVolume3Month INTEGER,
            fiftyTwoWeekLow REAL,
            fiftyTwoWeekHigh REAL,
            fiftyDayAverage REAL,
            twoHundredDayAverage REAL,
            beta REAL,
            priceToBook REAL,
            enterpriseValue INTEGER,
            profitMargins REAL,
            grossMargins REAL,
            operatingMargins REAL,
            returnOnAssets REAL,
            returnOnEquity REAL,
            freeCashflow INTEGER,
            totalCash INTEGER,
            totalDebt INTEGER,
            earningsGrowth REAL,
            revenueGrowth REAL,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        logger.info(f"Database initialized at {db_path}")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
    finally:
        if conn:
            conn.close()


def save_stock_info_to_db(info: Dict[str, Any], db_path: Optional[str] = None) -> None:
    """yfinanceのticker.infoをデータベースに保存（または更新）する"""
    if db_path is None:
        db_path = DB_PATH

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        stock_data = (
            info.get("symbol"),
            info.get("longName"),
            info.get("sector"),
            info.get("industry"),
            info.get("marketCap"),
            info.get("trailingPE"),
            info.get("forwardPE"),
            info.get("dividendYield"),
            info.get("website"),
            info.get("currentPrice"),
            info.get("regularMarketPrice"),
            info.get("currency"),
            info.get("exchange"),
            info.get("shortName"),
            info.get("previousClose"),
            info.get("open"),
            info.get("dayLow"),
            info.get("dayHigh"),
            info.get("volume"),
            info.get("averageDailyVolume10Day"),
            info.get("averageDailyVolume3Month"),
            info.get("fiftyTwoWeekLow"),
            info.get("fiftyTwoWeekHigh"),
            info.get("fiftyDayAverage"),
            info.get("twoHundredDayAverage"),
            info.get("beta"),
            info.get("priceToBook"),
            info.get("enterpriseValue"),
            info.get("profitMargins"),
            info.get("grossMargins"),
            info.get("operatingMargins"),
            info.get("returnOnAssets"),
            info.get("returnOnEquity"),
            info.get("freeCashflow"),
            info.get("totalCash"),
            info.get("totalDebt"),
            info.get("earningsGrowth"),
            info.get("revenueGrowth"),
            datetime.datetime.now(),
        )

        cursor.execute(
            """
        INSERT OR REPLACE INTO stocks (
            ticker, longName, sector, industry, marketCap,
            trailingPE, forwardPE, dividendYield, website,
            currentPrice, regularMarketPrice, currency, exchange, shortName,
            previousClose, open, dayLow, dayHigh, volume,
            averageDailyVolume10Day, averageDailyVolume3Month,
            fiftyTwoWeekLow, fiftyTwoWeekHigh, fiftyDayAverage, twoHundredDayAverage,
            beta, priceToBook, enterpriseValue, profitMargins, grossMargins,
            operatingMargins, returnOnAssets, returnOnEquity, freeCashflow,
            totalCash, totalDebt, earningsGrowth, revenueGrowth, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            stock_data,
        )

        conn.commit()
        logger.debug(f"[{info.get('symbol')}] Successfully saved/updated to DB.")

    except Exception as e:
        logger.error(f"Error saving stock info for {info.get('symbol')}: {e}")
    finally:
        if conn:
            conn.close()


def download_tse_listed_stocks():
    """Downloads the TSE listed stocks excel file from the JPX website."""
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        excel_link = soup.find(
            "a", href=lambda href: href and (".xls" in href or ".xlsx" in href)
        )

        if excel_link:
            excel_url = urljoin(url, excel_link["href"])
            excel_response = requests.get(excel_url)
            excel_response.raise_for_status()
            file_extension = ".xlsx" if excel_url.endswith(".xlsx") else ".xls"
            file_path = str(
                _settings.paths.data_dir / f"tse_listed_stocks{file_extension}"
            )
            with open(file_path, "wb") as f:
                f.write(excel_response.content)
            logger.info(f"Successfully downloaded TSE listed stocks to {file_path}")
            return file_path
        else:
            logger.error("Could not find the link to the excel file.")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading TSE listed stocks: {e}")
        return None


def fetch_and_store_tse_data(
    max_workers: Optional[int] = None,
    delay: Optional[float] = None,
    db_path: Optional[str] = None,
) -> None:
    """Fetch all TSE stock data and store in SQLite database."""
    settings = get_settings()

    # Use config defaults if not specified
    if max_workers is None:
        max_workers = settings.yfinance.max_workers
    if delay is None:
        delay = settings.yfinance.rate_limit_delay
    if db_path is None:
        db_path = DB_PATH

    logger.info("Fetching all TSE data and storing in database...")
    logger.info(f"Settings: max_workers={max_workers}, delay={delay}s")
    start_time = time.time()

    file_path = download_tse_listed_stocks()
    if not file_path:
        return

    try:
        df = pd.read_excel(file_path, header=0)
        df = df[~df["市場・商品区分"].str.contains("ETF")]
        df.rename(
            columns={"コード": "symbol", "銘柄名": "name", "33業種区分": "sector"},
            inplace=True,
        )
        df["symbol"] = df["symbol"].astype(str)

    except Exception as e:
        logger.error(f"Failed to load TSE stock list: {e}")
        return

    all_stocks = []
    for _, row in df.iterrows():
        symbol = row["symbol"] + ".T"
        name = row["name"]
        sector = row["sector"]
        all_stocks.append((symbol, name, sector))

    logger.info(f"Fetching data for {len(all_stocks)} stocks...")

    # Track success/failure counts
    success_count = 0
    failure_count = 0

    def fetch_single(
        symbol: str, name: str, sector: str
    ) -> bool:  # name and sector are used for validation
        nonlocal success_count, failure_count
        try:
            time.sleep(delay)
            ticker = yf.Ticker(symbol)
            info = ticker.info

            if info and "symbol" in info:
                save_stock_info_to_db(info, db_path)
                success_count += 1
                return True
            else:
                failure_count += 1
                return False

        except Exception as e:
            logger.error(f"Fetch failed for {symbol}: {e}")
            failure_count += 1
            return False

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_single, *stock): stock for stock in all_stocks
            }
            completed_count = 0
            total_count = len(futures)

            for future in as_completed(futures):
                future.result()
                completed_count += 1

                if completed_count % 100 == 0 or completed_count == total_count:
                    logger.info(
                        f"Progress: {completed_count}/{total_count} stocks processed "
                        f"(success: {success_count}, failed: {failure_count})"
                    )

    except Exception as e:
        logger.error(f"Error fetching stock data: {e}")

    load_time = time.time() - start_time
    logger.info(
        f"Completed: {success_count} saved, {failure_count} failed "
        f"in {load_time:.2f} seconds"
    )


# ==== Main Functions ====


class TSEDataProcessor:
    """TSE stock data processor with configurable rate limiting."""

    def __init__(
        self,
        max_workers: Optional[int] = None,
        rate_limit_delay: Optional[float] = None,
        db_path: Optional[str] = None,
    ):
        """
        Initialize the processor.

        Args:
            max_workers: Number of concurrent workers. Defaults to config value.
            rate_limit_delay: Delay between requests in seconds. Defaults to config value.
            db_path: Database path. Defaults to config value.
        """
        settings = get_settings()

        self.max_workers = max_workers or settings.yfinance.max_workers
        self.rate_limit_delay = rate_limit_delay or settings.yfinance.rate_limit_delay
        self.db_path = db_path or str(settings.paths.yfinance_db)

        if settings.paths.data_dir is not None:
            settings.paths.data_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path)

    def run(self) -> None:
        """Run the data fetching and storage process."""
        logger.info("Starting TSE data processing...")
        fetch_and_store_tse_data(
            max_workers=self.max_workers,
            delay=self.rate_limit_delay,
            db_path=self.db_path,
        )
        logger.info("TSE data processing completed.")
