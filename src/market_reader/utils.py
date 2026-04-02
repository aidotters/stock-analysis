"""Utility functions for stock_reader package."""

import sqlite3
from datetime import datetime

from dateutil.relativedelta import relativedelta


def normalize_code(code: str) -> str:
    """Normalize stock code to 4-digit format.

    J-Quants API stores codes as 5-digit (e.g., '72030'), but users typically
    use 4-digit format (e.g., '7203'). This function converts 5-digit codes
    ending with '0' to 4-digit format.

    Args:
        code: Stock code (4-digit or 5-digit)

    Returns:
        4-digit stock code if input was 5-digit ending with '0',
        otherwise returns input unchanged.

    Examples:
        >>> normalize_code("7203")
        '7203'
        >>> normalize_code("72030")
        '7203'
        >>> normalize_code("72031")
        '72031'
    """
    code = str(code).strip()
    if len(code) == 5 and code.endswith("0"):
        return code[:-1]
    return code


def to_5digit_code(code: str) -> str:
    """Convert 4-digit stock code to 5-digit format for database query.

    J-Quants database stores codes as 5-digit (e.g., '72030').
    This function converts 4-digit codes to 5-digit by appending '0'.

    Args:
        code: Stock code (4-digit or 5-digit)

    Returns:
        5-digit stock code

    Examples:
        >>> to_5digit_code("7203")
        '72030'
        >>> to_5digit_code("72030")
        '72030'
    """
    code = str(code).strip()
    if len(code) == 4:
        return code + "0"
    return code


def validate_date(date_str: str | None) -> datetime | None:
    """Validate and convert date string to datetime.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format, or None

    Returns:
        datetime object if valid date string, None if input is None

    Raises:
        ValueError: If date string format is invalid or date is impossible

    Examples:
        >>> validate_date("2024-01-01")
        datetime.datetime(2024, 1, 1, 0, 0)
        >>> validate_date(None)
        None
    """
    if date_str is None:
        return None

    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(
            f"Invalid date format: {date_str}. Expected YYYY-MM-DD."
        ) from e


def get_default_end_date(conn: sqlite3.Connection) -> datetime:
    """Get the latest date from database.

    Args:
        conn: SQLite database connection

    Returns:
        datetime of the latest date in daily_quotes table

    Raises:
        ValueError: If no data found in database
    """
    cursor = conn.execute("SELECT MAX(Date) FROM daily_quotes")
    result = cursor.fetchone()[0]

    if result is None:
        raise ValueError("No data found in database")

    return datetime.strptime(result, "%Y-%m-%d")


def get_default_start_date(end_date: datetime) -> datetime:
    """Calculate default start date (5 years before end date).

    Args:
        end_date: End date

    Returns:
        datetime 5 years before end_date
    """
    return end_date - relativedelta(years=5)
