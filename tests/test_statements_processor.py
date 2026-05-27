"""JQuantsStatementsProcessor (V2) の単体テスト。

V2 移行に伴い、認証は `JQuantsClient` 側に集約されたため、本テストでは
processor に MagicMock の `JQuantsClient` を DI してレスポンスを制御する。
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from market_pipeline.jquants.statements_processor import JQuantsStatementsProcessor


# V2 サンプル(短縮カラム名)
SAMPLE_V2_STATEMENTS = [
    {
        "Code": "72030",
        "DiscDate": "2024-05-10",
        "CurPerType": "FY",
        "DocType": "有価証券報告書",
        "Sales": 45000000000000,
        "OP": 3000000000000,
        "OdP": 3200000000000,
        "NP": 2500000000000,
        "EPS": 180.5,
        "TA": 80000000000000,
        "Eq": 35000000000000,
        "EqAR": 43.75,
        "BPS": 2500.0,
        "CFO": 5000000000000,
        "CFI": -2000000000000,
        "CFF": -1000000000000,
        "DivAnn": 60.0,
        "ShOutFY": 14000000000,
    },
    {
        "Code": "99840",
        "DiscDate": "2024-05-15",
        "CurPerType": "FY",
        "DocType": "有価証券報告書",
        "Sales": 6000000000000,
        "OP": 500000000000,
        "NP": 400000000000,
        "EPS": 450.0,
        "TA": 12000000000000,
        "Eq": 5000000000000,
        "BPS": 5600.0,
        "DivAnn": 44.0,
    },
]


@pytest.fixture
def fake_client():
    client = MagicMock()
    # 銘柄一覧は V2 形式
    master_rows = [
        {
            "Date": "2024-01-01",
            "Code": "72030",
            "CoName": "トヨタ自動車",
            "CoNameEn": "",
            "S17": "10",
            "S17Nm": "自動車",
            "S33": "3700",
            "S33Nm": "輸送用機器",
            "ScaleCat": "TOPIX Core30",
            "Mkt": "0111",
            "MktNm": "プライム",
            "Mrgn": "1",
            "MrgnNm": "信用",
        },
        {
            "Date": "2024-01-01",
            "Code": "99840",
            "CoName": "ソフトバンクグループ",
            "CoNameEn": "",
            "S17": "5",
            "S17Nm": "情報",
            "S33": "5250",
            "S33Nm": "情報・通信業",
            "ScaleCat": "TOPIX Large70",
            "Mkt": "0111",
            "MktNm": "プライム",
            "Mrgn": "1",
            "MrgnNm": "信用",
        },
    ]

    def paginate(path, params=None):
        if path == "/v2/equities/master":
            yield master_rows
        else:
            yield []

    client.paginate.side_effect = paginate
    return client


@pytest.fixture
def processor(fake_client):
    p = JQuantsStatementsProcessor(
        client=fake_client,
        max_concurrent_requests=2,
        batch_size=10,
        request_delay=0.0,
    )
    p.cache.clear_all()
    yield p
    p.cache.clear_all()


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.unlink(db_path)


class TestInit:
    def test_default_creates_client(self):
        with patch(
            "market_pipeline.jquants.statements_processor.JQuantsClient"
        ) as mock_cls:
            JQuantsStatementsProcessor()
            mock_cls.assert_called_once()

    def test_di_uses_passed_client(self, fake_client):
        p = JQuantsStatementsProcessor(client=fake_client)
        assert p.client is fake_client

    def test_config_values(self, processor):
        assert processor.max_concurrent_requests == 2
        assert processor.batch_size == 10


class TestDBInit:
    def test_initialize_database(self, processor, temp_db):
        processor._initialize_database(temp_db)

        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='financial_statements'"
            )
            assert cursor.fetchone() is not None

            cursor = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='calculated_fundamentals'"
            )
            assert cursor.fetchone() is not None

            cursor = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name='idx_statements_code'"
            )
            assert cursor.fetchone() is not None


class TestMapping:
    def test_map_after_normalize(self, processor):
        """V2 サンプルを normalize_statements 経由でマップ可能なこと。"""
        from market_pipeline.jquants._v2_translator import normalize_statements

        df = normalize_statements(SAMPLE_V2_STATEMENTS)
        v1_row = df.iloc[0].to_dict()
        record = processor._map_statement_to_record(v1_row)

        assert record["local_code"] == "72030"
        assert record["disclosed_date"] == "2024-05-10"
        assert record["type_of_current_period"] == "FY"
        assert record["net_sales"] == 45000000000000
        assert record["earnings_per_share"] == 180.5
        assert record["book_value_per_share"] == 2500.0
        assert record["cf_operating"] == 5000000000000
        assert record["cf_investing"] == -2000000000000
        assert record["number_of_shares"] == 14000000000


class TestSave:
    def test_save_statements_batch(self, processor, temp_db):
        from market_pipeline.jquants._v2_translator import normalize_statements

        processor._initialize_database(temp_db)
        df = normalize_statements(SAMPLE_V2_STATEMENTS)
        statements_data = [
            ("72030", [df.iloc[0].to_dict()]),
            ("99840", [df.iloc[1].to_dict()]),
        ]
        records_saved = processor.save_statements_batch(temp_db, statements_data)
        assert records_saved == 2

        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM financial_statements")
            assert cursor.fetchone()[0] == 2


class TestListedInfo:
    def test_returns_v1_columns(self, processor):
        processor.cache.clear_all()
        df = processor.get_listed_info_cached()
        assert not df.empty
        assert "Code" in df.columns
        assert "CompanyName" in df.columns


class TestAsyncFetch:
    def test_get_statements_async_normalizes(self, processor):
        """paginate_async の出力が V2 形式でも、戻り値レコードは V1 ロング名キーを持つ。"""
        import asyncio

        async def fake_paginate(session, path, params=None):
            yield SAMPLE_V2_STATEMENTS[:1]

        processor.client.paginate_async = fake_paginate

        async def run():
            return await processor.get_statements_async(session=None, code="72030")

        code, records = asyncio.run(run())
        assert code == "72030"
        assert len(records) == 1
        rec = records[0]
        # V1 ロング名
        assert rec["LocalCode"] == "72030"
        assert rec["NetSales"] == 45000000000000
        assert rec["TypeOfCurrentPeriod"] == "FY"

    def test_get_statements_async_handles_error(self, processor):
        import asyncio

        async def boom(session, path, params=None):
            raise RuntimeError("boom")
            yield

        processor.client.paginate_async = boom

        async def run():
            return await processor.get_statements_async(session=None, code="72030")

        code, records = asyncio.run(run())
        assert code == "72030"
        assert records == []


class TestOrchestrationGetAllStatements:
    """get_all_statements の fetch → save 経路を end-to-end でカバー。"""

    def test_persists_and_counts(self, processor, temp_db):
        processor.cache.clear_all()

        # process_codes_batch の戻り値を制御: 1 件目は 1 レコード、2 件目は空
        from market_pipeline.jquants._v2_translator import normalize_statements

        df = normalize_statements(SAMPLE_V2_STATEMENTS)
        records_72030 = [{str(k): v for k, v in df.iloc[0].to_dict().items()}]

        async def fake_batch(codes):
            results: list[tuple[str, list[dict]]] = []
            for code in codes:
                if code == "72030":
                    results.append(("72030", records_72030))
                else:
                    results.append((code, []))
            return results

        processor.process_codes_batch = fake_batch  # type: ignore[method-assign]

        # 例外なく完走、DB に成功銘柄のレコードが入っていること
        processor.get_all_statements(temp_db)

        with sqlite3.connect(temp_db) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM financial_statements"
            ).fetchone()[0]
            assert count == 1
            row = conn.execute(
                "SELECT local_code, net_sales FROM financial_statements"
            ).fetchone()
            assert row[0] == "72030"
            assert row[1] == 45000000000000

    def test_continues_on_batch_exception(self, processor, temp_db):
        processor.cache.clear_all()

        async def boom(codes):
            raise RuntimeError("batch failure")

        processor.process_codes_batch = boom  # type: ignore[method-assign]

        # 例外が発生してもプロセス全体は完走する(failed に計上され続行)
        processor.get_all_statements(temp_db)

        # 1 レコードも入らない
        with sqlite3.connect(temp_db) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM financial_statements"
            ).fetchone()[0]
            assert count == 0
