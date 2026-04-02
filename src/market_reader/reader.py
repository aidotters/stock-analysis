"""DataReader class for accessing stock price data."""

import sqlite3
import warnings
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator

import pandas as pd

from .exceptions import (
    DatabaseConnectionError,
    InvalidDateRangeError,
    StockNotFoundError,
)
from .utils import (
    get_default_end_date,
    get_default_start_date,
    normalize_code,
    to_5digit_code,
    validate_date,
)


class DataReader:
    """Main interface for retrieving stock price data.

    Provides pandas_datareader-like API for accessing J-Quants price data
    stored in SQLite database.

    Attributes:
        SIMPLE_COLUMNS: Default column set (OHLCV + AdjustmentClose)
        FULL_COLUMNS: All available columns from daily_quotes table
    """

    SIMPLE_COLUMNS = ["Open", "High", "Low", "Close", "Volume", "AdjustmentClose"]

    FULL_COLUMNS = [
        "Date",
        "Code",
        "Open",
        "High",
        "Low",
        "Close",
        "UpperLimit",
        "LowerLimit",
        "Volume",
        "TurnoverValue",
        "AdjustmentFactor",
        "AdjustmentOpen",
        "AdjustmentHigh",
        "AdjustmentLow",
        "AdjustmentClose",
        "AdjustmentVolume",
    ]

    def __init__(
        self,
        db_path: str | Path | None = None,
        strict: bool = False,
    ) -> None:
        """Initialize DataReader.

        Args:
            db_path: Path to jquants.db. If None, uses core/config settings.
            strict: If True, raises exceptions on errors.
                   If False (default), returns empty DataFrame with warning.

        Raises:
            DatabaseConnectionError: If database file doesn't exist or can't be accessed.
        """
        self.strict = strict

        if db_path is None:
            from market_pipeline.config import get_settings

            settings = get_settings()
            config_db_path = settings.paths.jquants_db
            if config_db_path is None:
                raise DatabaseConnectionError("jquants_db path not configured")
            self.db_path = Path(config_db_path)
        else:
            self.db_path = Path(db_path)

        # Verify database exists and is accessible
        self._verify_database()

    def _verify_database(self) -> None:
        """Verify database file exists and can be opened."""
        if not self.db_path.exists():
            raise DatabaseConnectionError(str(self.db_path))

        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("SELECT 1")
            conn.close()
        except sqlite3.Error as e:
            raise DatabaseConnectionError(str(self.db_path), e)

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get database connection with PRAGMA settings applied.

        Yields:
            SQLite connection with optimized settings
        """
        conn = sqlite3.connect(self.db_path)
        try:
            # Apply PRAGMA settings for read performance
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            yield conn
        finally:
            conn.close()

    def _resolve_columns(self, columns: str | list[str]) -> list[str]:
        """Resolve column specification to actual column list.

        Args:
            columns: "simple", "full", or list of column names

        Returns:
            List of column names to select

        Raises:
            ValueError: If invalid column names are specified
        """
        if columns == "simple":
            return self.SIMPLE_COLUMNS.copy()
        elif columns == "full":
            # Exclude Date and Code as they become index
            return [c for c in self.FULL_COLUMNS if c not in ("Date", "Code")]
        elif isinstance(columns, list):
            # Validate column names against whitelist
            valid_columns = set(self.FULL_COLUMNS) - {"Date", "Code"}
            invalid = set(columns) - valid_columns
            if invalid:
                raise ValueError(f"Invalid column names: {invalid}")
            return columns
        else:
            return self.SIMPLE_COLUMNS.copy()

    def _build_query(
        self,
        codes: list[str],
        start: str,
        end: str,
        columns: list[str],
        is_multiple: bool,
    ) -> tuple[str, tuple[str, ...]]:
        """Build SQL query with parameter binding.

        Args:
            codes: List of 5-digit stock codes for database
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            columns: List of columns to select
            is_multiple: Whether querying multiple codes

        Returns:
            Tuple of (SQL query string, parameter list)
        """
        # Always include Date for index
        select_cols = ["Date"]
        if is_multiple:
            select_cols.append("Code")
        select_cols.extend(columns)

        # Build SELECT clause
        select_clause = ", ".join(select_cols)

        # Build WHERE clause with parameter binding
        params: tuple[str, ...]
        if len(codes) == 1:
            where_code = "Code = ?"
            params = (codes[0], start, end)
        else:
            placeholders = ", ".join("?" * len(codes))
            where_code = f"Code IN ({placeholders})"
            params = tuple(codes) + (start, end)

        query = f"""
            SELECT {select_clause}
            FROM daily_quotes
            WHERE {where_code}
              AND Date BETWEEN ? AND ?
            ORDER BY Date, Code
        """

        return query, params

    def get_prices(
        self,
        code: str | list[str],
        start: str | None = None,
        end: str | None = None,
        columns: str | list[str] = "simple",
    ) -> pd.DataFrame:
        """Retrieve stock price data.

        Args:
            code: Stock code (4-digit or 5-digit) or list of codes
            start: Start date (YYYY-MM-DD). Defaults to 5 years before end.
            end: End date (YYYY-MM-DD). Defaults to latest date in database.
            columns: Column selection - "simple" (default), "full", or list of names

        Returns:
            DataFrame with Date index (single code) or
            MultiIndex DataFrame with (Date, Code) index (multiple codes)

        Raises:
            StockNotFoundError: If stock code not found (strict=True)
            InvalidDateRangeError: If start > end
            DatabaseConnectionError: If database error occurs
        """
        # Normalize to list
        is_multiple = isinstance(code, list)
        codes: list[str]
        if is_multiple:
            codes = list(code)  # Ensure it's a fresh list
        else:
            codes = [str(code)]

        # Normalize codes (user input to 4-digit for output)
        normalized_codes = [normalize_code(c) for c in codes]
        # Convert to 5-digit for database query
        db_codes = [to_5digit_code(c) for c in normalized_codes]

        with self._get_connection() as conn:
            # Resolve dates
            end_date: datetime
            start_date: datetime

            if end is None:
                end_date = get_default_end_date(conn)
            else:
                validated_end = validate_date(end)
                if validated_end is None:
                    raise ValueError(f"Invalid end date: {end}")
                end_date = validated_end

            if start is None:
                start_date = get_default_start_date(end_date)
            else:
                validated_start = validate_date(start)
                if validated_start is None:
                    raise ValueError(f"Invalid start date: {start}")
                start_date = validated_start

            # Validate date range
            if start_date > end_date:
                raise InvalidDateRangeError(
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                )

            # Format dates for query
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            # Resolve columns
            select_columns = self._resolve_columns(columns)

            # Build and execute query
            query, params = self._build_query(
                db_codes, start_str, end_str, select_columns, is_multiple
            )

            df = pd.read_sql_query(
                query,
                conn,
                params=params,
                parse_dates=["Date"],
            )

        # Check for empty results
        if df.empty:
            if self.strict:
                # Find which codes have no data
                missing = normalized_codes[0] if not is_multiple else normalized_codes
                raise StockNotFoundError(str(missing))
            else:
                warnings.warn(
                    f"No data found for stock code: {normalized_codes}",
                    UserWarning,
                    stacklevel=2,
                )
                return pd.DataFrame()

        # Check for partial results in multiple code query
        if is_multiple:
            found_codes = df["Code"].apply(normalize_code).unique()
            missing_codes = set(normalized_codes) - set(found_codes)
            if missing_codes:
                if self.strict:
                    raise StockNotFoundError(str(list(missing_codes)))
                else:
                    warnings.warn(
                        f"No data found for: {list(missing_codes)}",
                        UserWarning,
                        stacklevel=2,
                    )

        # Process DataFrame
        if is_multiple:
            # Normalize Code column to 4-digit
            df["Code"] = df["Code"].apply(normalize_code)
            # Set MultiIndex
            df = df.set_index(["Date", "Code"])
        else:
            # Single code - just Date index
            df = df.set_index("Date")

        return df
