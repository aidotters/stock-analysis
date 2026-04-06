"""
Centralized configuration management for Stock-Analysis project.

Uses Pydantic Settings for type-safe configuration with environment variable support.

Usage:
    from market_pipeline.config import get_settings

    settings = get_settings()
    db_path = settings.paths.jquants_db
    rate_limit = settings.yfinance.rate_limit_delay
"""

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Absolute path to .env for nested settings created via default_factory.
# Nested BaseSettings models don't inherit the parent's env_file, so each
# needs its own reference to ensure .env values are loaded regardless of cwd.
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"


class PathSettings(BaseSettings):
    """Directory and file path settings."""

    model_config = SettingsConfigDict(
        env_prefix="STOCK_ANALYSIS_", env_file=_ENV_FILE, extra="ignore"
    )

    base_dir: Path = Path(__file__).parent.parent.parent.parent  # Project root

    # Directories (computed from base_dir)
    data_dir: Optional[Path] = None
    logs_dir: Optional[Path] = None
    output_dir: Optional[Path] = None

    # Database files (computed from data_dir)
    jquants_db: Optional[Path] = None
    analysis_db: Optional[Path] = None
    yfinance_db: Optional[Path] = None
    master_db: Optional[Path] = None
    statements_db: Optional[Path] = None

    @model_validator(mode="after")
    def set_computed_paths(self) -> "PathSettings":
        """Compute derived paths from base_dir."""
        if self.data_dir is None:
            self.data_dir = self.base_dir / "data"
        if self.logs_dir is None:
            self.logs_dir = self.base_dir / "logs"
        if self.output_dir is None:
            self.output_dir = self.base_dir / "output"
        if self.jquants_db is None:
            self.jquants_db = self.data_dir / "jquants.db"
        if self.analysis_db is None:
            self.analysis_db = self.data_dir / "analysis_results.db"
        if self.yfinance_db is None:
            self.yfinance_db = self.data_dir / "yfinance.db"
        if self.master_db is None:
            self.master_db = self.data_dir / "master.db"
        if self.statements_db is None:
            self.statements_db = self.data_dir / "statements.db"
        return self


class JQuantsAPISettings(BaseSettings):
    """J-Quants API configuration."""

    model_config = SettingsConfigDict(
        env_prefix="JQUANTS_", env_file=_ENV_FILE, extra="ignore"
    )

    # Credentials (from environment)
    email: str = Field(default="", validation_alias="EMAIL")
    password: str = Field(default="", validation_alias="PASSWORD")

    # Rate limiting
    max_concurrent_requests: int = 10
    batch_size: int = 100
    request_delay: float = 0.05  # seconds between requests
    timeout_seconds: int = 10  # per-request timeout

    # Caching
    cache_ttl_hours: int = 24


class YFinanceSettings(BaseSettings):
    """yfinance API configuration."""

    model_config = SettingsConfigDict(
        env_prefix="YFINANCE_", env_file=_ENV_FILE, extra="ignore"
    )

    # Rate limiting (conservative defaults to avoid 429 errors)
    max_workers: int = 1  # Sequential processing recommended
    rate_limit_delay: float = 2.0  # seconds between requests

    # Valuation fetcher settings
    valuation_batch_size: int = 150
    valuation_wait_seconds: float = 0.5
    valuation_max_workers: int = 4


class AnalysisSettings(BaseSettings):
    """Technical analysis parameters."""

    model_config = SettingsConfigDict(
        env_prefix="ANALYSIS_", env_file=_ENV_FILE, extra="ignore"
    )

    # Period settings
    rsp_period_days: int = 500  # RSP calculation lookback
    update_window_days: int = 5  # Recent period update window
    hl_ratio_weeks: int = 52  # High-low ratio period
    trading_days_per_year: int = 260  # ~52 weeks

    # Moving average periods
    sma_short: int = 50
    sma_medium: int = 150
    sma_long: int = 200

    # Minervini thresholds
    type6_threshold: float = 1.3  # 30% above 52-week low
    type7_threshold: float = 0.75  # 75% of 52-week high (25% from high)
    type8_rsi_threshold: int = 70  # RSI >= 70 for Type 8

    # RSP calculation weights
    rsp_weight_q1_q3: float = 0.2  # Weight for Q1, Q2, Q3
    rsp_weight_q4: float = 0.4  # Weight for Q4 (most recent)

    # Composite score weights
    composite_weight_hl: float = 0.4  # HL ratio weight
    composite_weight_rsi: float = 0.4  # RSI weight
    composite_weight_minervini: float = 0.2  # Minervini score weight

    # Filtering thresholds
    high_rsi_threshold: float = 70.0
    strong_composite_threshold: float = 70.0

    # Chart classification windows
    chart_windows: list[int] = Field(default=[20, 60, 120, 240])
    chart_long_windows: list[int] = Field(default=[480, 1200, 2400, 4800])


class DatabaseSettings(BaseSettings):
    """SQLite optimization settings."""

    model_config = SettingsConfigDict(
        env_prefix="SQLITE_", env_file=_ENV_FILE, extra="ignore"
    )

    journal_mode: str = "WAL"  # Write-Ahead Logging
    synchronous: str = "NORMAL"  # Balance speed/safety
    cache_size: int = 10000  # Pages (~10-40MB)
    mmap_size: int = 268435456  # 256MB memory-mapped I/O

    def get_pragma_statements(self) -> list[str]:
        """Return list of PRAGMA statements to execute."""
        return [
            f"PRAGMA journal_mode={self.journal_mode}",
            f"PRAGMA synchronous={self.synchronous}",
            f"PRAGMA cache_size={self.cache_size}",
            f"PRAGMA mmap_size={self.mmap_size}",
        ]


class SlackSettings(BaseSettings):
    """Slack notification settings."""

    model_config = SettingsConfigDict(
        env_prefix="SLACK_", env_file=_ENV_FILE, extra="ignore"
    )

    webhook_url: str = ""
    error_webhook_url: str = ""
    enabled: bool = True
    timeout_seconds: int = 10
    max_retries: int = 3

    @property
    def is_configured(self) -> bool:
        """Return True if webhook_url is set and notifications are enabled."""
        return bool(self.webhook_url) and self.enabled


class LoggingSettings(BaseSettings):
    """Logging configuration."""

    model_config = SettingsConfigDict(
        env_prefix="LOG_", env_file=_ENV_FILE, extra="ignore"
    )

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"


class Settings(BaseSettings):
    """Main application settings container."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Nested settings
    paths: PathSettings = Field(default_factory=PathSettings)
    jquants: JQuantsAPISettings = Field(default_factory=JQuantsAPISettings)
    yfinance: YFinanceSettings = Field(default_factory=YFinanceSettings)
    analysis: AnalysisSettings = Field(default_factory=AnalysisSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    slack: SlackSettings = Field(default_factory=SlackSettings)

    # Global processing settings
    n_workers: Optional[int] = None  # None = auto-detect CPU count
    batch_size: int = 100


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance (singleton pattern).

    The settings are loaded once and cached for the lifetime of the application.
    To reload settings, call `get_settings.cache_clear()` first.

    Returns:
        Settings: The application settings instance.
    """
    return Settings()


def reload_settings() -> Settings:
    """
    Force reload of settings (clears cache and returns new instance).

    Returns:
        Settings: Fresh settings instance.
    """
    get_settings.cache_clear()
    return get_settings()
