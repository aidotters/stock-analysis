"""
Tests for HistoricalPriceFetcher class.
Tests column mapping, earliest date lookup, overlap exclusion,
batch saving with source column, dry-run mode, migration, and DataReader compatibility.
"""

import sqlite3
import tempfile
import os
from unittest.mock import patch

import pandas as pd
import pytest


def _create_master_db(path: str) -> None:
    """Create a temporary master database with test data."""
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE stocks_master (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            sector TEXT,
            market TEXT,
            yfinance_symbol TEXT,
            jquants_code TEXT,
            is_active BOOLEAN DEFAULT 1
        )
        """
    )
    stocks = [
        ("7203", "Toyota", "輸送用機器", "プライム", "7203.T", "72030", 1),
        ("9984", "SoftBank", "情報通信", "プライム", "9984.T", "99840", 1),
        ("1234", "Inactive Co", "その他", "スタンダード", "1234.T", "12340", 0),
    ]
    conn.executemany("INSERT INTO stocks_master VALUES (?, ?, ?, ?, ?, ?, ?)", stocks)
    conn.commit()
    conn.close()


def _create_jquants_db(path: str, with_source: bool = True) -> None:
    """Create a temporary jquants database with daily_quotes table."""
    conn = sqlite3.connect(path)
    source_col = ", source TEXT" if with_source else ""
    conn.execute(
        f"""
        CREATE TABLE daily_quotes (
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
            AdjustmentVolume INTEGER
            {source_col},
            PRIMARY KEY (Code, Date)
        )
        """
    )
    # Insert J-Quants data starting from 2021-06-01
    jquants_data = [
        (
            "72030",
            "2021-06-01",
            2000,
            2050,
            1990,
            2040,
            1000000,
            2e9,
            1.0,
            2000,
            2050,
            1990,
            2040,
            1000000,
        ),
        (
            "72030",
            "2021-06-02",
            2040,
            2060,
            2020,
            2050,
            900000,
            1.8e9,
            1.0,
            2040,
            2060,
            2020,
            2050,
            900000,
        ),
        (
            "99840",
            "2022-01-04",
            6000,
            6100,
            5900,
            6050,
            500000,
            3e9,
            1.0,
            6000,
            6100,
            5900,
            6050,
            500000,
        ),
    ]
    if with_source:
        conn.executemany(
            "INSERT INTO daily_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'jquants')",
            jquants_data,
        )
    else:
        conn.executemany(
            "INSERT INTO daily_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            jquants_data,
        )
    conn.commit()
    conn.close()


class TestHistoricalPriceFetcher:
    """Tests for HistoricalPriceFetcher class."""

    @pytest.fixture
    def temp_dbs(self):
        """Create temporary master and jquants databases."""
        master_fd = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        jquants_fd = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        master_fd.close()
        jquants_fd.close()

        _create_master_db(master_fd.name)
        _create_jquants_db(jquants_fd.name)

        yield {"master": master_fd.name, "jquants": jquants_fd.name}

        os.unlink(master_fd.name)
        os.unlink(jquants_fd.name)

    @pytest.fixture
    def fetcher(self, temp_dbs):
        """Create a HistoricalPriceFetcher instance with test databases."""
        from market_pipeline.yfinance.historical_price_fetcher import (
            HistoricalPriceFetcher,
        )

        return HistoricalPriceFetcher(
            jquants_db_path=temp_dbs["jquants"],
            master_db_path=temp_dbs["master"],
            max_workers=1,
            wait_seconds=0.0,
            batch_size=100,
            years=20,
        )

    def test_map_columns(self, fetcher):
        """全カラムの変換を検証（AdjustmentOpen/High/Low/Close/Volume に値、他はNULL）"""
        yf_df = pd.DataFrame(
            {
                "Open": [100.0, 102.0],
                "High": [105.0, 108.0],
                "Low": [99.0, 101.0],
                "Close": [104.0, 107.0],
                "Volume": [1000000, 1200000],
            },
            index=pd.DatetimeIndex(["2020-01-06", "2020-01-07"]),
        )

        result = fetcher.map_columns(yf_df, "7203")

        assert len(result) == 2
        row = result.iloc[0]

        # Code is 5-digit
        assert row["Code"] == "72030"
        assert row["Date"] == "2020-01-06"

        # Adjustment columns have values
        assert row["AdjustmentOpen"] == 100.0
        assert row["AdjustmentHigh"] == 105.0
        assert row["AdjustmentLow"] == 99.0
        assert row["AdjustmentClose"] == 104.0
        assert row["AdjustmentVolume"] == 1000000

        # Raw columns are NULL
        assert row["Open"] is None
        assert row["High"] is None
        assert row["Low"] is None
        assert row["Close"] is None
        assert row["Volume"] is None
        assert row["TurnoverValue"] is None
        assert row["AdjustmentFactor"] is None

        # source is 'yfinance'
        assert row["source"] == "yfinance"

    def test_map_columns_empty(self, fetcher):
        """空のDataFrameを渡した場合、空のDataFrameを返す"""
        result = fetcher.map_columns(pd.DataFrame(), "7203")
        assert result.empty

    def test_get_earliest_date(self, fetcher):
        """インメモリDBで最古日取得を検証"""
        earliest = fetcher.get_earliest_date("7203")
        assert earliest == "2021-06-01"

        earliest_sb = fetcher.get_earliest_date("9984")
        assert earliest_sb == "2022-01-04"

    def test_get_earliest_date_no_data(self, fetcher):
        """データがない銘柄はNoneを返す"""
        earliest = fetcher.get_earliest_date("9999")
        assert earliest is None

    def test_get_target_codes(self, fetcher):
        """アクティブ銘柄の一覧を取得"""
        targets = fetcher.get_target_codes()

        # 2 active stocks (1234 is inactive)
        assert len(targets) == 2
        codes = [t["code"] for t in targets]
        assert "7203" in codes
        assert "9984" in codes
        assert "1234" not in codes

        # Each target has earliest_date
        toyota = next(t for t in targets if t["code"] == "7203")
        assert toyota["earliest_date"] == "2021-06-01"
        assert toyota["yfinance_symbol"] == "7203.T"

    def test_get_target_codes_filtered(self, fetcher):
        """symbols指定で対象銘柄を絞り込み"""
        targets = fetcher.get_target_codes(symbols=["7203"])
        assert len(targets) == 1
        assert targets[0]["code"] == "7203"

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    def test_fetch_excludes_overlap(self, mock_download, fetcher):
        """J-Quantsデータ期間との重複除外を検証"""
        mock_download.return_value = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [105.0],
                "Low": [99.0],
                "Close": [104.0],
                "Volume": [1000000],
            },
            index=pd.DatetimeIndex(["2020-06-01"]),
        )

        target = {
            "code": "7203",
            "yfinance_symbol": "7203.T",
            "earliest_date": "2021-06-01",
        }
        result = fetcher.fetch_single(target)

        # Verify yf.download was called with end=earliest_date
        call_kwargs = mock_download.call_args
        assert call_kwargs.kwargs["end"] == "2021-06-01"

        assert result is not None
        assert len(result) == 1

    def test_save_batch_with_source(self, fetcher, temp_dbs):
        """sourceカラム付きINSERTを検証"""
        records = [
            {
                "Code": "72030",
                "Date": "2020-01-06",
                "Open": None,
                "High": None,
                "Low": None,
                "Close": None,
                "Volume": None,
                "TurnoverValue": None,
                "AdjustmentFactor": None,
                "AdjustmentOpen": 100.0,
                "AdjustmentHigh": 105.0,
                "AdjustmentLow": 99.0,
                "AdjustmentClose": 104.0,
                "AdjustmentVolume": 1000000,
                "source": "yfinance",
            }
        ]

        saved = fetcher.save_batch(records)
        assert saved == 1

        # Verify in DB
        with sqlite3.connect(temp_dbs["jquants"]) as conn:
            row = conn.execute(
                "SELECT source, AdjustmentOpen FROM daily_quotes WHERE Code = '72030' AND Date = '2020-01-06'"
            ).fetchone()
        assert row[0] == "yfinance"
        assert row[1] == 100.0

    def test_save_batch_ignore_duplicates(self, fetcher, temp_dbs):
        """INSERT OR IGNOREで重複をスキップすることを検証"""
        # Try to insert on existing J-Quants date
        records = [
            {
                "Code": "72030",
                "Date": "2021-06-01",  # Already exists as jquants
                "Open": None,
                "High": None,
                "Low": None,
                "Close": None,
                "Volume": None,
                "TurnoverValue": None,
                "AdjustmentFactor": None,
                "AdjustmentOpen": 999.0,
                "AdjustmentHigh": 999.0,
                "AdjustmentLow": 999.0,
                "AdjustmentClose": 999.0,
                "AdjustmentVolume": 999,
                "source": "yfinance",
            }
        ]

        saved = fetcher.save_batch(records)
        assert saved == 0  # INSERT OR IGNORE: duplicate skipped

        # Original jquants data should be preserved
        with sqlite3.connect(temp_dbs["jquants"]) as conn:
            row = conn.execute(
                "SELECT source, AdjustmentOpen FROM daily_quotes WHERE Code = '72030' AND Date = '2021-06-01'"
            ).fetchone()
        assert row[0] == "jquants"
        assert row[1] == 2000  # Original value, not 999

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    def test_run_dry_run(self, mock_download, fetcher, temp_dbs):
        """dry-runモードでDB書き込みが発生しないことを検証"""
        mock_download.return_value = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [105.0],
                "Low": [99.0],
                "Close": [104.0],
                "Volume": [1000000],
            },
            index=pd.DatetimeIndex(["2020-01-06"]),
        )

        # Count records before
        with sqlite3.connect(temp_dbs["jquants"]) as conn:
            before_count = conn.execute("SELECT COUNT(*) FROM daily_quotes").fetchone()[
                0
            ]

        result = fetcher.run(dry_run=True)

        # Count records after
        with sqlite3.connect(temp_dbs["jquants"]) as conn:
            after_count = conn.execute("SELECT COUNT(*) FROM daily_quotes").fetchone()[
                0
            ]

        # No new records should be inserted
        assert before_count == after_count
        assert result["elapsed"] >= 0

    def test_source_column_migration(self):
        """マイグレーション後にsourceカラムが存在することを検証"""
        from scripts.migrate_add_source_column import migrate, has_source_column

        temp_fd = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_fd.close()
        try:
            # Create DB without source column
            _create_jquants_db(temp_fd.name, with_source=False)

            # Verify no source column
            with sqlite3.connect(temp_fd.name) as conn:
                assert not has_source_column(conn)

            # Run migration
            migrate(temp_fd.name)

            # Verify source column exists
            with sqlite3.connect(temp_fd.name) as conn:
                assert has_source_column(conn)

                # All existing records should have source='jquants'
                rows = conn.execute(
                    "SELECT COUNT(*) FROM daily_quotes WHERE source = 'jquants'"
                ).fetchone()
                total = conn.execute("SELECT COUNT(*) FROM daily_quotes").fetchone()
                assert rows[0] == total[0]
                assert total[0] == 3  # 3 test records
        finally:
            os.unlink(temp_fd.name)

    def test_source_column_migration_idempotent(self):
        """マイグレーションの冪等性を検証"""
        from scripts.migrate_add_source_column import migrate

        temp_fd = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_fd.close()
        try:
            _create_jquants_db(temp_fd.name, with_source=False)
            migrate(temp_fd.name)
            migrate(temp_fd.name)  # Should not raise

            with sqlite3.connect(temp_fd.name) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM daily_quotes WHERE source = 'jquants'"
                ).fetchone()[0]
                assert count == 3
        finally:
            os.unlink(temp_fd.name)

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    @patch("market_pipeline.yfinance.historical_price_fetcher.time.sleep")
    def test_fetch_single_retry_then_success(self, mock_sleep, mock_download, fetcher):
        """リトライ後に成功するパスを検証"""
        yf_df = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [105.0],
                "Low": [99.0],
                "Close": [104.0],
                "Volume": [1000000],
            },
            index=pd.DatetimeIndex(["2020-06-01"]),
        )
        # 1回目: 例外、2回目: 成功
        mock_download.side_effect = [Exception("rate limited"), yf_df]

        target = {
            "code": "7203",
            "yfinance_symbol": "7203.T",
            "earliest_date": "2021-06-01",
        }
        result = fetcher.fetch_single(target)

        assert result is not None
        assert len(result) == 1
        assert mock_download.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    @patch("market_pipeline.yfinance.historical_price_fetcher.time.sleep")
    def test_fetch_single_retry_all_fail(self, mock_sleep, mock_download, fetcher):
        """全リトライ失敗でNoneを返すパスを検証"""
        mock_download.side_effect = Exception("persistent error")

        target = {
            "code": "7203",
            "yfinance_symbol": "7203.T",
            "earliest_date": "2021-06-01",
        }
        result = fetcher.fetch_single(target)

        assert result is None
        assert mock_download.call_count == 3
        assert mock_sleep.call_count == 2  # 1回目と2回目の失敗後にsleep

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    def test_fetch_single_no_period(self, mock_download, fetcher):
        """start_date >= end_dateで取得対象期間なしの場合Noneを返す"""
        target = {
            "code": "7203",
            "yfinance_symbol": "7203.T",
            "earliest_date": "2006-01-01",  # 20年前より前なのでstart >= end
        }
        # yearsを1に設定して確実にstart >= endにする
        fetcher.years = 1
        result = fetcher.fetch_single(target)

        assert result is None
        mock_download.assert_not_called()

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    def test_run_with_save(self, mock_download, fetcher, temp_dbs):
        """非dry-runパスでバッチINSERTが実行されることを検証"""
        yf_df = pd.DataFrame(
            {
                "Open": [100.0, 102.0],
                "High": [105.0, 108.0],
                "Low": [99.0, 101.0],
                "Close": [104.0, 107.0],
                "Volume": [1000000, 1200000],
            },
            index=pd.DatetimeIndex(["2020-01-06", "2020-01-07"]),
        )
        mock_download.return_value = yf_df

        result = fetcher.run(dry_run=False)

        assert result["success"] >= 1
        assert result["total_records"] >= 2

        # Verify records were actually inserted
        with sqlite3.connect(temp_dbs["jquants"]) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM daily_quotes WHERE source = 'yfinance'"
            ).fetchone()[0]
        assert count >= 2

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    def test_run_batch_insert_threshold(self, mock_download, fetcher, temp_dbs):
        """batch_size到達時にバッチINSERTが実行されることを検証"""
        # batch_size=100なので、200件のデータで少なくとも1回バッチINSERTが走る
        dates = pd.date_range("2019-01-01", periods=150, freq="B")
        yf_df = pd.DataFrame(
            {
                "Open": [100.0] * len(dates),
                "High": [105.0] * len(dates),
                "Low": [99.0] * len(dates),
                "Close": [104.0] * len(dates),
                "Volume": [1000000] * len(dates),
            },
            index=dates,
        )
        mock_download.return_value = yf_df

        result = fetcher.run(symbols=["7203"], dry_run=False)

        assert result["success"] == 1
        assert result["total_records"] >= 100

        with sqlite3.connect(temp_dbs["jquants"]) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM daily_quotes WHERE source = 'yfinance'"
            ).fetchone()[0]
        assert count >= 100

    @patch("market_pipeline.yfinance.historical_price_fetcher.yf.download")
    def test_run_no_targets(self, mock_download, fetcher, temp_dbs):
        """対象銘柄が0件の場合の早期リターンを検証"""
        result = fetcher.run(symbols=["9999"])  # 存在しない銘柄

        assert result["success"] == 0
        assert result["failed"] == 0
        assert result["skipped"] == 0
        assert result["total_records"] == 0
        mock_download.assert_not_called()

    def test_data_reader_compatibility(self, temp_dbs):
        """DataReaderがsourceカラム追加後も正常に動作することを検証"""
        from market_reader import DataReader

        # Add a yfinance record
        with sqlite3.connect(temp_dbs["jquants"]) as conn:
            conn.execute(
                """
                INSERT INTO daily_quotes
                (Code, Date, AdjustmentOpen, AdjustmentHigh, AdjustmentLow, AdjustmentClose, AdjustmentVolume, source)
                VALUES ('72030', '2020-01-06', 100, 105, 99, 104, 1000000, 'yfinance')
                """
            )
            conn.commit()

        reader = DataReader(db_path=temp_dbs["jquants"])
        df = reader.get_prices("7203", start="2020-01-01", end="2022-01-01")

        # Should include both jquants and yfinance data
        assert len(df) >= 2  # At least yfinance + jquants records
        assert "2020-01-06" in df.index.strftime("%Y-%m-%d").tolist()
        assert "2021-06-01" in df.index.strftime("%Y-%m-%d").tolist()
