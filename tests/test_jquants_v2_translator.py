"""_v2_translator.py のユニットテスト。

サンプル JSON(tmp/v2_samples/)に基づき、V2→V1 のカラム rename と射影を確認する。
"""

from __future__ import annotations

import logging


from market_pipeline.jquants._v2_translator import (
    DAILY_QUOTES_COLUMN_MAP,
    LISTED_INFO_COLUMN_MAP,
    STATEMENTS_COLUMN_MAP,
    normalize_daily_quotes,
    normalize_listed_info,
    normalize_statements,
)


# ---------------------------------------------------------------------------
# daily quotes
# ---------------------------------------------------------------------------
class TestNormalizeDailyQuotes:
    def _sample(self) -> list[dict]:
        return [
            {
                "Date": "2023-03-24",
                "Code": "86970",
                "O": 2047.0,
                "H": 2069.0,
                "L": 2035.0,
                "C": 2045.0,
                "UL": "0",
                "LL": "0",
                "Vo": 2202500.0,
                "Va": 4507051850.0,
                "AdjFactor": 1.0,
                "AdjO": 2047.0,
                "AdjH": 2069.0,
                "AdjL": 2035.0,
                "AdjC": 2045.0,
                "AdjVo": 2202500.0,
                # Premium 限定の前場/後場 — translator は落とす
                "MO": 2047.0,
                "MH": 2069.0,
                "AO": 2047.0,
                "AAdjC": 2045.0,
            }
        ]

    def test_renames_to_v1_columns(self):
        df = normalize_daily_quotes(self._sample())
        assert list(df.columns) == [
            "Date",
            "Code",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "TurnoverValue",
            "AdjustmentFactor",
            "AdjustmentOpen",
            "AdjustmentHigh",
            "AdjustmentLow",
            "AdjustmentClose",
            "AdjustmentVolume",
        ]
        assert df.iloc[0]["Open"] == 2047.0
        assert df.iloc[0]["Close"] == 2045.0
        assert df.iloc[0]["Volume"] == 2202500.0
        assert df.iloc[0]["AdjustmentClose"] == 2045.0

    def test_drops_premium_only_fields(self):
        df = normalize_daily_quotes(self._sample())
        for unwanted in ("UL", "LL", "MO", "MH", "AO", "AAdjC"):
            assert unwanted not in df.columns

    def test_empty_input_returns_empty_df(self):
        df = normalize_daily_quotes([])
        assert df.empty

    def test_missing_optional_field_handled(self):
        """V1 互換出力なので、サンプルから一部欠落していても KeyError にならず欠損列が落ちるだけ。"""
        partial = [{"Date": "2023-03-24", "Code": "86970", "O": 100.0, "C": 110.0}]
        df = normalize_daily_quotes(partial)
        assert list(df.columns) == ["Date", "Code", "Open", "Close"]
        assert df.iloc[0]["Open"] == 100.0

    def test_unknown_field_warning(self, caplog):
        rows = [{"Date": "2023-03-24", "Code": "86970", "O": 1.0, "Mystery": "x"}]
        with caplog.at_level(
            logging.WARNING, logger="market_pipeline.jquants._v2_translator"
        ):
            normalize_daily_quotes(rows)
        assert any("Mystery" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# listed info
# ---------------------------------------------------------------------------
class TestNormalizeListedInfo:
    def _sample(self) -> list[dict]:
        return [
            {
                "Date": "2022-11-11",
                "Code": "86970",
                "CoName": "日本取引所グループ",
                "CoNameEn": "Japan Exchange Group,Inc.",
                "S17": "16",
                "S17Nm": "金融（除く銀行）",
                "S33": "7200",
                "S33Nm": "その他金融業",
                "ScaleCat": "TOPIX Large70",
                "Mkt": "0111",
                "MktNm": "プライム",
                "Mrgn": "1",
                "MrgnNm": "信用",
                "ProdCat": "011",
            }
        ]

    def test_renames_to_v1_columns(self):
        df = normalize_listed_info(self._sample())
        assert "CompanyName" in df.columns
        assert "Sector33CodeName" in df.columns
        assert "MarketCodeName" in df.columns
        assert df.iloc[0]["CompanyName"] == "日本取引所グループ"
        assert df.iloc[0]["Sector33CodeName"] == "その他金融業"
        assert df.iloc[0]["MarketCodeName"] == "プライム"

    def test_drops_prodcat(self):
        df = normalize_listed_info(self._sample())
        assert "ProdCat" not in df.columns

    def test_empty_input(self):
        df = normalize_listed_info([])
        assert df.empty


# ---------------------------------------------------------------------------
# statements
# ---------------------------------------------------------------------------
class TestNormalizeStatements:
    def _sample(self) -> list[dict]:
        return [
            {
                "DiscDate": "2024-05-10",
                "DiscTime": "15:00:00",
                "Code": "72030",
                "DiscNo": "1234567",
                "DocType": "有価証券報告書",
                "CurPerType": "FY",
                "CurPerSt": "2023-04-01",
                "CurPerEn": "2024-03-31",
                "CurFYSt": "2023-04-01",
                "CurFYEn": "2024-03-31",
                "Sales": 45000000000000,
                "OP": 3000000000000,
                "OdP": 3200000000000,
                "NP": 2500000000000,
                "EPS": 180.5,
                "DEPS": 179.8,
                "TA": 80000000000000,
                "Eq": 35000000000000,
                "EqAR": 43.75,
                "BPS": 2500.0,
                "CFO": 5000000000000,
                "CFI": -2000000000000,
                "CFF": -1000000000000,
                "CashEq": 8000000000000,
                "DivAnn": 60.0,
                "FDivAnn": 70.0,
                "PayoutRatioAnn": 33.0,
                "ShOutFY": 14000000000,
                "TrShFY": 100000000,
                "FSales": 48000000000000,
                "FOP": 3500000000000,
                "FOdP": 3700000000000,
                "FNP": 2800000000000,
                "FEPS": 200.0,
                # 翌期予想は落とされる想定
                "NxFSales": 99,
                "NxFEPS": 99,
                # 非連結も落とされる
                "NCSales": 100,
            }
        ]

    def test_renames_to_v1_columns(self):
        df = normalize_statements(self._sample())
        row = df.iloc[0]
        assert row["LocalCode"] == "72030"
        assert row["DisclosedDate"] == "2024-05-10"
        assert row["TypeOfCurrentPeriod"] == "FY"
        assert row["NetSales"] == 45000000000000
        assert row["OperatingProfit"] == 3000000000000
        assert row["OrdinaryProfit"] == 3200000000000
        assert row["Profit"] == 2500000000000
        assert row["EarningsPerShare"] == 180.5
        assert row["TotalAssets"] == 80000000000000
        assert row["Equity"] == 35000000000000
        assert row["BookValuePerShare"] == 2500.0
        assert row["CashFlowsFromOperatingActivities"] == 5000000000000
        assert row["CashFlowsFromInvestingActivities"] == -2000000000000
        assert row["CashFlowsFromFinancingActivities"] == -1000000000000
        assert row["CashAndEquivalents"] == 8000000000000
        assert row["ResultDividendPerShareAnnual"] == 60.0
        assert row["ForecastDividendPerShareAnnual"] == 70.0
        assert (
            row[
                "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"
            ]
            == 14000000000
        )
        assert row["NumberOfTreasuryStockAtTheEndOfFiscalYear"] == 100000000
        assert row["ForecastNetSales"] == 48000000000000
        assert row["ForecastEarningsPerShare"] == 200.0

    def test_drops_unused_fields(self):
        df = normalize_statements(self._sample())
        # 翌期予想・非連結・四半期配当などは落とされる
        for unwanted in ("NxFSales", "NxFEPS", "NCSales", "DiscTime"):
            assert unwanted not in df.columns

    def test_empty_input(self):
        df = normalize_statements([])
        assert df.empty

    def test_partial_record_no_error(self):
        partial = [{"Code": "12345", "DiscDate": "2024-01-01", "CurPerType": "FY"}]
        df = normalize_statements(partial)
        assert df.iloc[0]["LocalCode"] == "12345"


# ---------------------------------------------------------------------------
# integration: V1 互換性チェック(_map_statement_to_record が受け取れる形)
# ---------------------------------------------------------------------------
class TestV1Compat:
    def test_statements_to_record_mapping_compatible(self):
        """normalize_statements の出力を従来の _map_statement_to_record に
        渡しても KeyError にならないことを確認(rename が正しく行われている)。"""
        from market_pipeline.jquants.statements_processor import (
            JQuantsStatementsProcessor,
        )

        sample = [
            {
                "Code": "12345",
                "DiscDate": "2024-01-01",
                "CurPerType": "FY",
                "Sales": 1000,
                "OP": 100,
                "NP": 80,
                "EPS": 10.0,
                "TA": 5000,
                "Eq": 2000,
                "BPS": 200.0,
                "CFO": 200,
                "CFI": -50,
                "CFF": -30,
                "DivAnn": 5.0,
                "ShOutFY": 1000000,
            }
        ]
        df = normalize_statements(sample)
        row_dict = df.iloc[0].to_dict()
        # _map_statement_to_record は dict を受け取り、内部で .get() するので
        # 例外を起こさずレコード化できることを確認
        proc = JQuantsStatementsProcessor.__new__(JQuantsStatementsProcessor)
        record = proc._map_statement_to_record(row_dict)
        assert record["local_code"] == "12345"
        assert record["net_sales"] == 1000
        assert record["operating_profit"] == 100
        assert record["profit"] == 80
        assert record["earnings_per_share"] == 10.0
        assert record["total_assets"] == 5000
        assert record["equity"] == 2000
        assert record["cf_operating"] == 200
        assert record["cf_investing"] == -50
        assert record["number_of_shares"] == 1000000


# ---------------------------------------------------------------------------
# column map sanity
# ---------------------------------------------------------------------------
def test_no_duplicate_v1_values_in_column_maps():
    """各マップの V1 側に重複が無いこと(rename collision を防ぐ)。"""
    for label, m in [
        ("daily_quotes", DAILY_QUOTES_COLUMN_MAP),
        ("listed_info", LISTED_INFO_COLUMN_MAP),
        ("statements", STATEMENTS_COLUMN_MAP),
    ]:
        values = list(m.values())
        assert len(values) == len(set(values)), (
            f"{label} に重複した V1 カラム名がある: {values}"
        )
