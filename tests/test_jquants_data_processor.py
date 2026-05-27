"""JQuantsDataProcessor (V2) の単体テスト。

V2 移行に伴い、認証は `JQuantsClient` 側に集約されたため、本テストでは
processor に MagicMock の `JQuantsClient` を DI してレスポンスを制御する。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from market_pipeline.jquants.data_processor import JQuantsDataProcessor


@pytest.fixture
def fake_client():
    """JQuantsClient のフェイク。paginate は V2 形式の `data` 配列を返す。

    上場銘柄一覧は 100 件以上必要(MIN_EXPECTED_COMPANIES バリデーション)。
    """
    client = MagicMock()
    # 100 件以上のダミー銘柄を生成
    master_rows = [
        {
            "Date": "2024-01-01",
            "Code": f"{1000 + i:04d}0",
            "CoName": f"Company {i}",
            "CoNameEn": f"Company {i} Inc",
            "S17": "16",
            "S17Nm": "金融",
            "S33": "7200",
            "S33Nm": "その他金融業",
            "ScaleCat": "TOPIX Small",
            "Mkt": "0111",
            "MktNm": "プライム",
            "Mrgn": "1",
            "MrgnNm": "信用",
            "ProdCat": "011",
        }
        for i in range(100)
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
    p = JQuantsDataProcessor(client=fake_client)
    # テスト分離のためキャッシュを毎回クリア
    p.cache.clear_all()
    yield p
    p.cache.clear_all()


class TestInit:
    def test_default_creates_client(self):
        """client 未指定時は内部で生成される。"""
        with patch("market_pipeline.jquants.data_processor.JQuantsClient") as mock_cls:
            JQuantsDataProcessor()
            mock_cls.assert_called_once()

    def test_di_uses_passed_client(self, fake_client):
        p = JQuantsDataProcessor(client=fake_client)
        assert p.client is fake_client


class TestListedInfo:
    def test_fetch_returns_v1_columns(self, processor):
        # cache をクリア
        processor.cache.clear_all()
        df = processor.get_listed_info_cached()
        assert not df.empty
        # V2 短縮名は無く V1 ロング名のみ
        assert "CompanyName" in df.columns
        assert "Sector33CodeName" in df.columns
        assert "MarketCodeName" in df.columns
        assert "CoName" not in df.columns
        assert "S33Nm" not in df.columns
        assert len(df) >= JQuantsDataProcessor.MIN_EXPECTED_COMPANIES

    def test_below_threshold_not_cached(self, fake_client):
        # 50 件しか返さないようにする
        fake_client.paginate.side_effect = lambda path, params=None: iter(
            [
                [
                    {
                        "Date": "2024-01-01",
                        "Code": f"{i:05d}",
                        "CoName": f"X{i}",
                        "CoNameEn": "",
                        "S17": "1",
                        "S17Nm": "x",
                        "S33": "1",
                        "S33Nm": "x",
                        "ScaleCat": "",
                        "Mkt": "0111",
                        "MktNm": "プライム",
                        "Mrgn": "",
                        "MrgnNm": "",
                    }
                    for i in range(50)
                ]
            ]
        )
        proc = JQuantsDataProcessor(client=fake_client)
        proc.cache.clear_all()
        processor = proc
        df = processor.get_listed_info_cached()
        # 取得は返るがキャッシュには載らない
        assert len(df) == 50
        assert processor.cache.get("jquants_listed_info") is None


class TestDailyQuotes:
    def test_async_quotes_rename_to_v1(self, processor):
        """get_daily_quotes_async は V1 ロング名カラムを返す。"""
        import asyncio

        # paginate_async が V2 形式の data を 1 ページ返すように差し替え
        async def fake_paginate_async(session, path, params=None):
            yield [
                {
                    "Date": "2024-01-01",
                    "Code": "13010",
                    "O": 100.0,
                    "H": 110.0,
                    "L": 95.0,
                    "C": 105.0,
                    "Vo": 10000.0,
                    "Va": 1_050_000.0,
                    "AdjFactor": 1.0,
                    "AdjO": 100.0,
                    "AdjH": 110.0,
                    "AdjL": 95.0,
                    "AdjC": 105.0,
                    "AdjVo": 10000.0,
                    "UL": "0",
                    "LL": "0",
                }
            ]

        processor.client.paginate_async = fake_paginate_async

        async def run():
            # session は実際には使用されない(fake_paginate_async が無視)
            return await processor.get_daily_quotes_async(
                session=None, code="13010", from_date="2024-01-01", to_date="2024-01-01"
            )

        code, df = asyncio.run(run())
        assert code == "13010"
        assert not df.empty
        assert df.iloc[0]["Open"] == 100.0
        assert df.iloc[0]["Close"] == 105.0
        assert df.iloc[0]["Volume"] == 10000.0
        assert df.iloc[0]["AdjustmentClose"] == 105.0
        # ストップ高フラグは落とされる
        assert "UL" not in df.columns

    def test_async_quotes_empty_on_exception(self, processor):
        import asyncio

        async def boom(session, path, params=None):
            raise RuntimeError("boom")
            yield  # never reached

        processor.client.paginate_async = boom

        async def run():
            return await processor.get_daily_quotes_async(
                session=None, code="13010", from_date="2024-01-01", to_date="2024-01-01"
            )

        code, df = asyncio.run(run())
        assert code == "13010"
        assert df.empty


class TestReturnValueShape:
    """戻り値のキーが V1 と同一であることを確認。"""

    def test_all_up_to_date_returns_expected_keys(self, processor, tmp_path):
        processor.cache.clear_all()
        db_path = str(tmp_path / "test.db")
        # 各銘柄の最終日付を今日にして全件「最新」扱いにする
        from datetime import datetime

        today = datetime.now().strftime("%Y-%m-%d")

        def fake_last_dates(db, codes):
            return {c: today for c in codes}

        processor.get_last_dates_batch = fake_last_dates  # type: ignore[method-assign]
        result = processor.update_prices_to_db_optimized(db_path)
        assert set(result.keys()) == {
            "total_listed",
            "codes_to_update",
            "codes_updated",
            "records_inserted",
            "codes_failed",
        }
        assert result["codes_to_update"] == 0


class TestOrchestrationUpdate:
    """update_prices_to_db_optimized の fetch → save 経路を end-to-end でカバー。

    `process_codes_batch` をモック化して、戻り値集計と DB 投入が正しく動くことを確認。
    """

    def test_update_with_mocked_fetch_persists_and_counts(self, processor, tmp_path):
        import sqlite3

        import pandas as pd

        processor.cache.clear_all()
        db_path = str(tmp_path / "update.db")

        # 全銘柄を「2024-01-01 以降」更新対象にする
        def fake_last_dates(db, codes):
            return {c: "2024-01-01" for c in codes}

        processor.get_last_dates_batch = fake_last_dates  # type: ignore[method-assign]

        # process_codes_batch の戻り値を制御:
        # - 各 batch の最初の 60% が成功(各 2 行)、残り 40% は空 DataFrame
        # - 全体で 60 件成功 / 40 件「データなし」になる構成
        async def fake_batch(codes, from_date, to_date):
            results: list[tuple[str, pd.DataFrame]] = []
            for offset, code in enumerate(codes):
                if offset < int(len(codes) * 0.6):
                    df = pd.DataFrame(
                        [
                            {
                                "Date": "2024-01-02",
                                "Code": code,
                                "Open": 100.0,
                                "High": 101.0,
                                "Low": 99.0,
                                "Close": 100.5,
                                "Volume": 1000,
                                "TurnoverValue": 100500.0,
                                "AdjustmentFactor": 1.0,
                                "AdjustmentOpen": 100.0,
                                "AdjustmentHigh": 101.0,
                                "AdjustmentLow": 99.0,
                                "AdjustmentClose": 100.5,
                                "AdjustmentVolume": 1000,
                            },
                            {
                                "Date": "2024-01-03",
                                "Code": code,
                                "Open": 101.0,
                                "High": 102.0,
                                "Low": 100.0,
                                "Close": 101.5,
                                "Volume": 1100,
                                "TurnoverValue": 111650.0,
                                "AdjustmentFactor": 1.0,
                                "AdjustmentOpen": 101.0,
                                "AdjustmentHigh": 102.0,
                                "AdjustmentLow": 100.0,
                                "AdjustmentClose": 101.5,
                                "AdjustmentVolume": 1100,
                            },
                        ]
                    )
                    results.append((code, df))
                else:
                    results.append((code, pd.DataFrame()))
            return results

        async def call_fake(codes, from_date, to_date):
            return await fake_batch(codes, from_date, to_date)

        processor.process_codes_batch = call_fake  # type: ignore[method-assign]

        result = processor.update_prices_to_db_optimized(db_path)

        # 戻り値: fixture が 100 件を返す → 全件更新対象 → 60 件成功 + 40 件空
        assert result["total_listed"] == 100
        assert result["codes_to_update"] == 100
        # codes_updated は「実際にレコードが入った銘柄数」のみカウント
        assert result["codes_updated"] == 60
        assert result["records_inserted"] == 60 * 2  # 各 2 行

        # DB に source='jquants' 付きで投入されていること
        with sqlite3.connect(db_path) as con:
            count = con.execute(
                "SELECT COUNT(*) FROM daily_quotes WHERE source='jquants'"
            ).fetchone()[0]
            assert count == 120

    def test_update_collects_failed_when_batch_raises(self, processor, tmp_path):
        processor.cache.clear_all()
        db_path = str(tmp_path / "fail.db")

        def fake_last_dates(db, codes):
            return {c: "2024-01-01" for c in codes}

        processor.get_last_dates_batch = fake_last_dates  # type: ignore[method-assign]

        async def boom(codes, from_date, to_date):
            raise RuntimeError("batch failure")

        processor.process_codes_batch = boom  # type: ignore[method-assign]

        result = processor.update_prices_to_db_optimized(db_path)
        # 例外でも戻り値構造は維持し、failed に全件計上
        assert set(result.keys()) == {
            "total_listed",
            "codes_to_update",
            "codes_updated",
            "records_inserted",
            "codes_failed",
        }
        assert result["codes_failed"] == 100
        assert result["records_inserted"] == 0
