"""日付メジャーの日次取得（S2a 橋）の単体テスト。

取り直す日の規則・1 日単位の原子的な保存・失敗日の記録と回収を固定する。
"""

from __future__ import annotations

import sqlite3
from datetime import date
from unittest.mock import MagicMock

import pytest

from market_pipeline.jquants.data_processor import (
    FAILED_DATES_TABLE,
    JQuantsDataProcessor,
    recent_weekdays_start,
    select_dates_to_fetch,
)

N_CODES = 120  # MIN_EXPECTED_COMPANIES(100) を超える銘柄数


def _bar(code: str, day: str, close: float | None = 100.0) -> dict:
    return {
        "Date": day,
        "Code": code,
        "O": close,
        "H": close,
        "L": close,
        "C": close,
        "Vo": 1000,
        "Va": 100000.0,
        "AdjFactor": 1.0,
        "AdjO": close,
        "AdjH": close,
        "AdjL": close,
        "AdjC": close,
        "AdjVo": 1000,
    }


def _day_rows(day: str, n: int = N_CODES, close: float | None = 100.0) -> list[dict]:
    return [_bar(f"{1000 + i:04d}0", day, close) for i in range(n)]


class FakeClient:
    """paginate を日付ごとに制御できるフェイク。

    ``days``: 営業日（カレンダー）。``bars``: 日付 → ページのリスト。値が Exception なら
    そのページで送出する（途中のページで落ちる経路）。
    """

    def __init__(self, days: list[str], bars: dict[str, list]):
        self.days = days
        self.bars = bars
        self.http_requests = 0
        self.retries = 0
        self.bar_calls: list[str] = []

    def paginate(self, path, params=None):
        params = params or {}
        if path == "/v2/markets/calendar":
            self.http_requests += 1
            yield [
                {"Date": d, "HolDiv": "1"}
                for d in self.days
                if params["from"] <= d <= params["to"]
            ] + [{"Date": "2026-01-01", "HolDiv": "0"}]
            return
        day = params["date"]
        self.bar_calls.append(day)
        for page in self.bars.get(day, [[]]):
            self.http_requests += 1
            if isinstance(page, Exception):
                raise page
            yield page


def _processor(client) -> JQuantsDataProcessor:
    return JQuantsDataProcessor(client=client)


def _rows(db: str, sql: str, params=()) -> list:
    with sqlite3.connect(db) as conn:
        return conn.execute(sql, params).fetchall()


def _seed(db: str, days: list[str]) -> None:
    """既存の jquants 行を入れる（初期化後に呼ぶ）。"""
    with sqlite3.connect(db) as conn:
        for d in days:
            conn.executemany(
                "INSERT INTO daily_quotes (Code, Date, Close, source) VALUES (?, ?, ?, 'jquants')",
                [(f"{1000 + i:04d}0", d, 1.0) for i in range(N_CODES)],
            )
        conn.commit()


# --------------------------------------------------------------- 純関数


class TestRecentWeekdaysStart:
    def test_counts_today_and_skips_weekend(self):
        # 2026-10-07 は水曜。直近 7 平日は 9/29(火)〜10/7(水)
        assert recent_weekdays_start(date(2026, 10, 7)) == date(2026, 9, 29)

    def test_weekend_today(self):
        # 2026-10-10 は土曜。直近 7 平日は 10/1(木)〜10/9(金)
        assert recent_weekdays_start(date(2026, 10, 10)) == date(2026, 10, 1)


class TestSelectDatesToFetch:
    DAYS = [f"2026-09-{d:02d}" for d in (1, 2, 3, 4, 7, 8, 9, 10, 11, 14, 15, 16)]

    def _select(self, **kw):
        base = dict(
            trading_days=self.DAYS,
            today="2026-09-16",
            last_jquants_date="2026-09-16",
            failed_dates=[],
            row_counts={d: 100 for d in self.DAYS},
        )
        base.update(kw)
        return select_dates_to_fetch(**base)

    def test_window_is_recent_seven_weekdays(self):
        r = self._select()
        assert r["window"] == {
            "2026-09-08",
            "2026-09-09",
            "2026-09-10",
            "2026-09-11",
            "2026-09-14",
            "2026-09-15",
            "2026-09-16",
        }
        assert r["after_last"] == set()
        assert r["thin"] == set()

    def test_after_last_covers_long_outage(self):
        r = self._select(last_jquants_date="2026-09-02")
        assert {"2026-09-03", "2026-09-04", "2026-09-07"} <= r["after_last"]

    def test_failed_dates_stay_beyond_window_and_lookback(self):
        # 窓（7 平日）にも薄い日の 30 営業日にも入らない古い失敗日も残る
        r = self._select(failed_dates=["2026-06-01"])
        assert "2026-06-01" in r["failed"]

    def test_thin_day_and_zero_row_day(self):
        counts = {d: 100 for d in self.DAYS}
        counts["2026-09-02"] = 80  # 中央値 100 の 90% 未満
        counts.pop("2026-09-03")  # 行数 0
        counts["2026-09-04"] = 95  # 90% 以上は薄くない
        r = self._select(row_counts=counts)
        assert r["thin"] == {"2026-09-02", "2026-09-03"}

    def test_median_zero_skips_thin(self):
        r = self._select(row_counts={})
        assert r["thin"] == set()


# ------------------------------------------------------------ 取得と保存


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "jquants.db")
    JQuantsDataProcessor(client=MagicMock())._initialize_database(path)
    return path


class TestUpdatePricesByDate:
    DAYS = [
        "2026-09-28",
        "2026-09-29",
        "2026-09-30",
        "2026-10-01",
        "2026-10-02",
        "2026-10-05",
        "2026-10-06",
        "2026-10-07",
    ]

    def test_fetches_window_saves_and_counts(self, db):
        _seed(db, self.DAYS[:-1])
        client = FakeClient(self.DAYS, {d: [_day_rows(d)] for d in self.DAYS})
        r = _processor(client).update_prices_by_date(db, today="2026-10-07")
        assert r["dates_failed"] == 0
        # 窓は 9/29〜10/7 の 7 営業日（9/28 は窓外・薄くもない）
        assert client.bar_calls == self.DAYS[1:]
        assert r["logical_fetches"] == {"calendar": 1, "daily_bars": 7}
        assert r["http_requests"] == 8
        n = _rows(
            db,
            "SELECT COUNT(*) FROM daily_quotes WHERE Date='2026-10-07' AND source='jquants'",
        )
        assert n == [(N_CODES,)]

    def test_second_page_failure_saves_nothing_for_that_day(self, db):
        _seed(db, self.DAYS[:-1])
        bars = {d: [_day_rows(d)] for d in self.DAYS}
        bars["2026-10-07"] = [_day_rows("2026-10-07")[:60], RuntimeError("page 2 down")]
        r = _processor(FakeClient(self.DAYS, bars)).update_prices_by_date(
            db, today="2026-10-07"
        )
        assert r["failed_dates"] == ["2026-10-07"]
        assert _rows(
            db, "SELECT COUNT(*) FROM daily_quotes WHERE Date='2026-10-07'"
        ) == [(0,)]
        assert _rows(db, f"SELECT date FROM {FAILED_DATES_TABLE}") == [("2026-10-07",)]

    def test_failed_day_recovered_next_run_even_after_later_success(self, db):
        # 9/29 が落ち、それより後の日は成功した → 次の実行は失敗日として 9/29 を取り直す
        _seed(db, self.DAYS[:1])
        bars = {d: [_day_rows(d)] for d in self.DAYS}
        bars["2026-09-29"] = [RuntimeError("timeout")]
        _processor(FakeClient(self.DAYS, bars)).update_prices_by_date(
            db, today="2026-10-07"
        )
        assert _rows(db, f"SELECT date FROM {FAILED_DATES_TABLE}") == [("2026-09-29",)]

        # 窓から外れた後（11/20）でも失敗日として取り直され、成功で記録が消える
        later = self.DAYS + ["2026-11-20"]
        bars2 = {d: [_day_rows(d)] for d in later}
        client2 = FakeClient(later, bars2)
        r = _processor(client2).update_prices_by_date(db, today="2026-11-20")
        assert "2026-09-29" in client2.bar_calls
        assert r["dates_failed"] == 0
        assert _rows(db, f"SELECT COUNT(*) FROM {FAILED_DATES_TABLE}") == [(0,)]
        assert _rows(
            db, "SELECT COUNT(*) FROM daily_quotes WHERE Date='2026-09-29'"
        ) == [(N_CODES,)]

    def test_rerun_keeps_all_columns(self, db):
        _seed(db, self.DAYS[:-1])
        bars = {d: [_day_rows(d)] for d in self.DAYS}
        p = _processor(FakeClient(self.DAYS, bars))
        p.update_prices_by_date(db, today="2026-10-07")
        before = _rows(
            db, "SELECT * FROM daily_quotes WHERE Date='2026-10-07' ORDER BY Code"
        )
        _processor(FakeClient(self.DAYS, bars)).update_prices_by_date(
            db, today="2026-10-07"
        )
        after = _rows(
            db, "SELECT * FROM daily_quotes WHERE Date='2026-10-07' ORDER BY Code"
        )
        assert before == after
        assert before[0][-1] == "jquants"

    def test_null_close_rows_are_saved(self, db):
        _seed(db, self.DAYS[:-1])
        rows = _day_rows("2026-10-07")
        rows[0] = _bar(rows[0]["Code"], "2026-10-07", close=None)
        rows[0]["AdjFactor"] = 0.5
        bars = {d: [_day_rows(d)] for d in self.DAYS}
        bars["2026-10-07"] = [rows]
        _processor(FakeClient(self.DAYS, bars)).update_prices_by_date(
            db, today="2026-10-07"
        )
        got = _rows(
            db,
            "SELECT Close, AdjustmentFactor FROM daily_quotes WHERE Code=? AND Date='2026-10-07'",
            (rows[0]["Code"],),
        )
        assert got == [(None, 0.5)]

    def test_empty_today_is_not_failure_but_past_empty_is(self, db):
        _seed(db, self.DAYS[:-2])
        bars = {d: [_day_rows(d)] for d in self.DAYS}
        bars["2026-10-07"] = [[]]  # 当日分の公開前
        bars["2026-10-06"] = [[]]  # 過去の営業日が空＝異常
        r = _processor(FakeClient(self.DAYS, bars)).update_prices_by_date(
            db, today="2026-10-07"
        )
        assert r["empty_today"] is True
        assert r["failed_dates"] == ["2026-10-06"]

    def test_save_failure_rolls_back_and_records_failure(self, db, monkeypatch):
        _seed(db, self.DAYS[:-1])
        bars = {d: [_day_rows(d)] for d in self.DAYS}
        p = _processor(FakeClient(self.DAYS, bars))
        # 保存の途中（失敗日の削除の直前）で落ちる → その日の行は残らない
        real_connect = sqlite3.connect

        class BoomConn:
            def __init__(self, conn):
                self._c = conn

            def execute(self, sql, *a):
                if sql.startswith(f"DELETE FROM {FAILED_DATES_TABLE}"):
                    raise sqlite3.OperationalError("disk I/O error")
                return self._c.execute(sql, *a)

            def __getattr__(self, name):
                return getattr(self._c, name)

        def connect(path, *a, **kw):
            return BoomConn(real_connect(path, *a, **kw))

        monkeypatch.setattr(
            "market_pipeline.jquants.data_processor.sqlite3.connect", connect
        )
        with pytest.raises(sqlite3.OperationalError):
            p.save_day(db, "2026-10-07", p.fetch_daily_quotes_by_date("2026-10-07")[0])
        monkeypatch.undo()
        assert _rows(
            db, "SELECT COUNT(*) FROM daily_quotes WHERE Date='2026-10-07'"
        ) == [(0,)]


# ------------------------------------------------------------ 日次の入口


def _load_entry():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "scripts" / "run_daily_jquants.py"
    spec = importlib.util.spec_from_file_location("run_daily_jquants_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("failed, expect_exit", [(["2026-10-06"], 1), ([], None)])
def test_entry_runs_chain_then_exits_nonzero_on_failed_days(
    tmp_path, monkeypatch, failed, expect_exit
):
    entry = _load_entry()
    db_path = tmp_path / "jquants.db"
    db_path.write_bytes(b"")  # 既存 DB あり＝差分更新の経路

    settings = MagicMock()
    settings.paths.data_dir = tmp_path
    settings.paths.logs_dir = tmp_path / "logs"
    settings.paths.jquants_db = db_path
    settings.logging.level = "INFO"
    settings.logging.format = "%(message)s"
    monkeypatch.setattr(entry, "get_settings", lambda: settings)
    monkeypatch.setattr(entry, "JQuantsClient", MagicMock())
    monkeypatch.setattr(entry, "JobContext", MagicMock())

    processor = MagicMock()
    processor.update_prices_by_date.return_value = {
        "dates_to_fetch": 7,
        "dates_fetched": 7 - len(failed),
        "dates_failed": len(failed),
        "failed_dates": failed,
        "records_inserted": 100,
        "logical_fetches": {"calendar": 1, "daily_bars": 7},
        "pages": 7,
        "http_requests": 9,
        "retries": 0,
        "empty_today": False,
    }
    processor.get_database_stats.return_value = {}
    monkeypatch.setattr(
        entry, "JQuantsDataProcessor", MagicMock(return_value=processor)
    )

    import subprocess

    calls = []
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: calls.append(a) or MagicMock(returncode=0)
    )

    if expect_exit is None:
        entry.main(chain=True)
    else:
        with pytest.raises(SystemExit) as exc:
            entry.main(chain=True)
        assert exc.value.code == expect_exit
    # 失敗があっても後段（Daily Analysis）は走っている
    assert len(calls) == 1
    processor.update_prices_by_date.assert_called_once()
    processor.update_prices_to_db_optimized.assert_not_called()
