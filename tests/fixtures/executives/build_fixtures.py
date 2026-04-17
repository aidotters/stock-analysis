#!/usr/bin/env python3
"""EDINET 実データからテスト用 iXBRL フィクスチャを再生成する.

参考実装リポ (`/Users/tak/Markets/Stocks/company-research-agent/`) に
ダウンロード済みの有価証券報告書から役員関連タグだけを抽出した
縮約版 iXBRL を作成する。

使い方:
    python tests/fixtures/executives/build_fixtures.py
"""

from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup

SOURCE_ROOT = Path("/Users/tak/Markets/Stocks/company-research-agent/downloads")
OUT_DIR = Path(__file__).parent

TOYOTA_IXBRL = (
    SOURCE_ROOT / "72030_トヨタ自動車株式会社/120_有価証券報告書/202503/S100VWVY/"
    "XBRL/PublicDoc/0104010_honbun_jpcrp030000-asr-001_E02144-000_2025-03-31_01_2025-06-18_ixbrl.htm"
)
SONY_IXBRL = (
    SOURCE_ROOT / "67580_ソニーグループ株式会社/120_有価証券報告書/202503/S100W19Q/"
    "XBRL/PublicDoc/0104010_honbun_jpcrp030000-asr-001_E01777-000_2025-03-31_01_2025-06-20_ixbrl.htm"
)

# 役員情報に関連する XBRL タグ（取締役系＋執行役系、両方の career も含める）
TARGET_ELEMENTS = {
    # 取締役系
    "jpcrp_cor:NameInformationAboutDirectorsAndCorporateAuditors",
    "jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors",
    "jpcrp_cor:DateOfBirthInformationAboutDirectorsAndCorporateAuditors",
    "jpcrp_cor:TermOfOfficeInformationAboutDirectorsAndCorporateAuditors",
    "jpcrp_cor:CareerSummaryInformationAboutDirectorsAndCorporateAuditorsTextBlock",
    # 執行役系
    "jpcrp_cor:NameInformationAboutExecutiveDirectors",
    "jpcrp_cor:OfficialTitleOrPositionInformationAboutExecutiveDirectors",
    "jpcrp_cor:DateOfBirthInformationAboutExecutiveDirectors",
    "jpcrp_cor:TermOfOfficeInformationAboutExecutiveDirectors",
    "jpcrp_cor:CareerSummaryInformationAboutExecutiveDirectorsTextBlock",
}


def build_fixture(source: Path, out: Path) -> None:
    html = source.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    kept: list[str] = []
    for tag in soup.find_all("ix:nonnumeric"):
        name = tag.get("name", "")
        if name in TARGET_ELEMENTS:
            kept.append(str(tag))
    content = (
        '<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">\n'
        "<body>\n" + "\n".join(kept) + "\n</body>\n</html>\n"
    )
    out.write_text(content, encoding="utf-8")
    print(f"WROTE {out} ({len(kept)} tags, {len(content)} bytes)")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    build_fixture(TOYOTA_IXBRL, OUT_DIR / "toyota_0104010.htm")
    build_fixture(SONY_IXBRL, OUT_DIR / "sony_0104010.htm")


if __name__ == "__main__":
    main()
