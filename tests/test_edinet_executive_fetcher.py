"""EdinetExecutiveFetcher のユニットテスト.

フィクスチャは Phase 0 PoC で取得した実データから役員関連タグのみ縮約したもの:
- toyota_0104010.htm: 監査等委員会設置会社（取締役系タグのみ）
- sony_0104010.htm: 指名委員会等設置会社（取締役系＋執行役系）
"""

from __future__ import annotations

from pathlib import Path

import pytest

from market_pipeline.executives.edinet_executive_fetcher import (
    EdinetExecutiveFetcher,
    normalize_text,
)
from market_pipeline.executives.exceptions import EdinetParseError

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "executives"
TOYOTA_IXBRL = FIXTURE_DIR / "toyota_0104010.htm"
SONY_IXBRL = FIXTURE_DIR / "sony_0104010.htm"


class TestNormalizeText:
    """氏名・役職の正規化ルール."""

    def test_nobreak_space_to_halfwidth(self) -> None:
        assert normalize_text("豊\u00a0田\u00a0章\u00a0男") == "豊 田 章 男"

    def test_fullwidth_space_to_halfwidth(self) -> None:
        assert normalize_text("山田\u3000太郎") == "山田 太郎"

    def test_collapse_consecutive_spaces(self) -> None:
        assert normalize_text("A   B\t\tC") == "A B C"

    def test_strip_leading_trailing_spaces(self) -> None:
        assert normalize_text("  \u3000佐藤  ") == "佐藤"

    def test_mixed_whitespace(self) -> None:
        assert normalize_text("豊\u00a0田\u3000章\u00a0男  ") == "豊 田 章 男"


class TestParseFromToyotaFixture:
    """監査等委員会設置会社（取締役系タグのみ）のパース動作."""

    @pytest.fixture
    def executives(self) -> list:
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        return fetcher.fetch_from_local_ixbrl(
            TOYOTA_IXBRL, code="7203", doc_id="S100VWVY"
        )

    def test_total_count(self, executives) -> None:
        assert len(executives) == 10

    def test_representative_count(self, executives) -> None:
        reps = [e for e in executives if e.is_representative]
        assert len(reps) == 4

    def test_representative_names(self, executives) -> None:
        rep_names = {e.name for e in executives if e.is_representative}
        assert "豊 田 章 男" in rep_names
        assert "佐 藤 恒 治" in rep_names

    def test_all_have_role(self, executives) -> None:
        assert all(e.role for e in executives)

    def test_doc_id_propagated(self, executives) -> None:
        assert all(e.edinet_source_doc_id == "S100VWVY" for e in executives)

    def test_code_propagated(self, executives) -> None:
        assert all(e.code == "7203" for e in executives)

    def test_career_summary_extracted(self, executives) -> None:
        """略歴テキストが抽出されていること."""
        populated = [e for e in executives if e.career_summary]
        assert len(populated) == 10  # 全役員分
        toyoda = next(e for e in executives if "豊 田" in e.name)
        assert toyoda.career_summary is not None
        assert "トヨタ自動車" in toyoda.career_summary
        assert "1984年" in toyoda.career_summary


class TestParseFromSonyFixture:
    """指名委員会等設置会社（取締役系＋執行役系）のパース動作."""

    @pytest.fixture
    def executives(self) -> list:
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        return fetcher.fetch_from_local_ixbrl(
            SONY_IXBRL, code="6758", doc_id="S100W19Q"
        )

    def test_total_count(self, executives) -> None:
        # 取締役 10 + 代表執行役 3 + 執行役 3 = 16
        assert len(executives) == 16

    def test_representative_executive_count(self, executives) -> None:
        reps = [e for e in executives if e.is_representative]
        assert len(reps) == 3

    def test_dual_entries_for_concurrent_roles(self, executives) -> None:
        """吉田氏は取締役と代表執行役の2レコードに分離されているべき."""
        yoshida_entries = [e for e in executives if "吉田" in e.name]
        assert len(yoshida_entries) == 2
        roles = {e.role for e in yoshida_entries}
        assert any("取締役" == r for r in roles)
        assert any("代表執行役" in r for r in roles)

    def test_context_ref_sharing_resolved(self, executives) -> None:
        """同一 contextRef を共有する取締役・代表執行役が正しく分離される."""
        yoshida_dir = next(
            e for e in executives if e.name.startswith("吉田") and e.role == "取締役"
        )
        yoshida_exec = next(
            e
            for e in executives
            if e.name.startswith("吉田") and "代表執行役" in e.role
        )
        assert yoshida_dir.is_representative is False
        assert yoshida_exec.is_representative is True

    def test_executive_officer_not_representative(self, executives) -> None:
        """執行役（代表なし）は is_representative=False."""
        officer = next(e for e in executives if e.name.startswith("小寺"))
        assert officer.is_representative is False
        assert "執行役" in officer.role

    def test_career_summary_for_both_schemas(self, executives) -> None:
        """取締役系・執行役系の双方で略歴が取れる."""
        # 代表執行役の御供氏（exec schema）
        mitsutomo = next(
            e for e in executives if "御供" in e.name and "執行役" in e.role
        )
        assert mitsutomo.career_summary is not None
        # 取締役の畑中氏（dir schema）
        hatanaka = next(e for e in executives if "畑中" in e.name)
        assert hatanaka.career_summary is not None


class TestParseFromHandcraftedIxbrl:
    """手書きフィクスチャでのエッジケース."""

    def test_empty_ixbrl_returns_empty(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.htm"
        p.write_text(
            "<html xmlns:ix='http://www.xbrl.org/2013/inlineXBRL'><body></body></html>",
            encoding="utf-8",
        )
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        assert fetcher.fetch_from_local_ixbrl(p, code="9999") == []

    def test_missing_role_keeps_empty_string(self, tmp_path: Path) -> None:
        p = tmp_path / "partial.htm"
        p.write_text(
            """
            <html xmlns:ix='http://www.xbrl.org/2013/inlineXBRL'><body>
            <ix:nonNumeric contextRef="ctx1" name="jpcrp_cor:NameInformationAboutDirectorsAndCorporateAuditors">田中一郎</ix:nonNumeric>
            </body></html>
            """,
            encoding="utf-8",
        )
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        result = fetcher.fetch_from_local_ixbrl(p, code="9999")
        assert len(result) == 1
        assert result[0].name == "田中一郎"
        assert result[0].role == ""
        assert result[0].is_representative is False

    def test_representative_detection_on_boundary(self, tmp_path: Path) -> None:
        """代表取締役 / 代表執行役 / 代表理事 等をまとめて検出."""
        p = tmp_path / "reps.htm"
        p.write_text(
            """
            <html xmlns:ix='http://www.xbrl.org/2013/inlineXBRL'><body>
            <ix:nonNumeric contextRef="c1" name="jpcrp_cor:NameInformationAboutDirectorsAndCorporateAuditors">A氏</ix:nonNumeric>
            <ix:nonNumeric contextRef="c1" name="jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors">代表取締役社長</ix:nonNumeric>
            <ix:nonNumeric contextRef="c2" name="jpcrp_cor:NameInformationAboutExecutiveDirectors">B氏</ix:nonNumeric>
            <ix:nonNumeric contextRef="c2" name="jpcrp_cor:OfficialTitleOrPositionInformationAboutExecutiveDirectors">代表執行役 社長 CEO</ix:nonNumeric>
            <ix:nonNumeric contextRef="c3" name="jpcrp_cor:NameInformationAboutDirectorsAndCorporateAuditors">C氏</ix:nonNumeric>
            <ix:nonNumeric contextRef="c3" name="jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors">取締役</ix:nonNumeric>
            </body></html>
            """,
            encoding="utf-8",
        )
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        result = fetcher.fetch_from_local_ixbrl(p, code="9999")
        by_name = {e.name: e for e in result}
        assert by_name["A氏"].is_representative is True
        assert by_name["B氏"].is_representative is True
        assert by_name["C氏"].is_representative is False


class TestDownloadZipError:
    """XBRL ZIPダウンロード周りのエラーハンドリング."""

    def test_raises_when_api_key_missing(self) -> None:
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        fetcher._api_key = ""  # 環境に依存せず空文字列を強制
        with pytest.raises(Exception) as exc_info:
            fetcher._download_xbrl_zip("S100XXXX")
        assert "EDINET_API_KEY" in str(exc_info.value)

    def test_bad_zip_bytes_raise_parse_error(self) -> None:
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        with pytest.raises(EdinetParseError):
            fetcher._extract_officer_ixbrl(b"not-a-zip")

    def test_missing_0104010_reports_available_ixbrl_files(self) -> None:
        """0104010 が無い場合は、参考情報として検出された iXBRL ファイル名をエラーに含める."""
        import io
        import zipfile

        from market_pipeline.executives.exceptions import EdinetParseError

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(
                "XBRL/PublicDoc/0101010_honbun_sample_ixbrl.htm", "<html></html>"
            )
            zf.writestr(
                "XBRL/PublicDoc/0105000_honbun_sample_ixbrl.htm", "<html></html>"
            )
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        with pytest.raises(EdinetParseError) as exc_info:
            fetcher._extract_officer_ixbrl(buf.getvalue())
        msg = str(exc_info.value)
        assert "0101010_honbun_sample_ixbrl.htm" in msg
        assert "0105000_honbun_sample_ixbrl.htm" in msg
