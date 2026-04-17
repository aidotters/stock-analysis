"""EDINET 有価証券報告書から法定役員情報を取得するフェッチャー.

Phase 0 PoC (`scripts/poc_edinet_executives.py`) で検証したロジックを本実装化:

- 有価証券報告書の役員情報は `XBRL/PublicDoc/0104010_honbun_*_ixbrl.htm` に集約
- 取締役系タグ + 執行役系タグの両系統をパース
- `(contextRef, 役員種別)` の複合キーでSony等の指名委員会等設置会社に対応
- is_representativeは役職文字列に "代表" が含まれるかで判定
"""

from __future__ import annotations

import io
import logging
import re
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

from market_pipeline.config import get_settings
from market_pipeline.executives.exceptions import EdinetFetchError, EdinetParseError

logger = logging.getLogger(__name__)

# XBRLタグ定義（Phase 0 PoC確定事項）
_EL_NAME_DIR = "jpcrp_cor:NameInformationAboutDirectorsAndCorporateAuditors"
_EL_ROLE_DIR = (
    "jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors"
)
_EL_BIRTH_DIR = "jpcrp_cor:DateOfBirthInformationAboutDirectorsAndCorporateAuditors"
_EL_TERM_DIR = "jpcrp_cor:TermOfOfficeInformationAboutDirectorsAndCorporateAuditors"
_EL_CAREER_DIR = (
    "jpcrp_cor:CareerSummaryInformationAboutDirectorsAndCorporateAuditorsTextBlock"
)

_EL_NAME_EXEC = "jpcrp_cor:NameInformationAboutExecutiveDirectors"
_EL_ROLE_EXEC = "jpcrp_cor:OfficialTitleOrPositionInformationAboutExecutiveDirectors"
_EL_BIRTH_EXEC = "jpcrp_cor:DateOfBirthInformationAboutExecutiveDirectors"
_EL_TERM_EXEC = "jpcrp_cor:TermOfOfficeInformationAboutExecutiveDirectors"
_EL_CAREER_EXEC = "jpcrp_cor:CareerSummaryInformationAboutExecutiveDirectorsTextBlock"

ELEMENT_SCHEMAS: dict[str, dict[str, str]] = {
    "dir": {
        "name": _EL_NAME_DIR,
        "role": _EL_ROLE_DIR,
        "birth": _EL_BIRTH_DIR,
        "term": _EL_TERM_DIR,
        "career": _EL_CAREER_DIR,
    },
    "exec": {
        "name": _EL_NAME_EXEC,
        "role": _EL_ROLE_EXEC,
        "birth": _EL_BIRTH_EXEC,
        "term": _EL_TERM_EXEC,
        "career": _EL_CAREER_EXEC,
    },
}

# 有価証券報告書を識別するEDINETメタ
ASR_ORDINANCE_CODE = "010"
ASR_FORM_CODE = "030000"


@dataclass
class Executive:
    """有価証券報告書から抽出した法定役員エントリ.

    同一人物でも役職が異なる場合は別レコードとして扱う
    （例: Sony 吉田氏は「取締役」と「代表執行役 会長」の2レコード）。
    """

    code: str
    name: str
    role: str
    is_representative: bool
    birthdate: Optional[str] = None
    appointed_date: Optional[str] = None
    edinet_source_doc_id: Optional[str] = None
    career_summary: Optional[str] = None


def normalize_text(value: str) -> str:
    """XBRL抽出テキストの正規化（Phase 0 PoC確定ルール）.

    - ノーブレークスペース `\\u00a0` → 半角スペース
    - 全角スペース `\\u3000` → 半角スペース
    - 連続空白は1個の半角スペースに圧縮
    - 前後の空白をトリム
    """
    value = value.replace("\u00a0", " ").replace("\u3000", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


class EdinetExecutiveFetcher:
    """EDINET APIから有価証券報告書を取得し、役員情報を抽出する.

    Example:
        fetcher = EdinetExecutiveFetcher()
        executives, doc_id = fetcher.fetch_from_doc_id("S100W4HN", code="9984")
        for e in executives:
            print(e.name, e.role, e.is_representative)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_list: Optional[int] = None,
        timeout_download: Optional[int] = None,
        max_retries: Optional[int] = None,
    ) -> None:
        settings = get_settings()
        self._api_key = api_key or settings.edinet.api_key
        self._base_url = base_url or settings.edinet.base_url
        self._timeout_list = (
            timeout_list if timeout_list is not None else settings.edinet.timeout_list
        )
        self._timeout_download = (
            timeout_download
            if timeout_download is not None
            else settings.edinet.timeout_download
        )
        self._max_retries = (
            max_retries if max_retries is not None else settings.edinet.max_retries
        )

    def fetch_from_doc_id(self, doc_id: str, code: str) -> tuple[list[Executive], str]:
        """指定した `doc_id` から XBRL を取得し、役員リストを返す.

        Args:
            doc_id: EDINET書類管理番号
            code: 銘柄コード (4桁)

        Returns:
            (executives, doc_id) のタプル

        Raises:
            EdinetFetchError: XBRLダウンロード失敗時
            EdinetParseError: iXBRLファイルが見つからない/パース失敗時
        """
        content = self._download_xbrl_zip(doc_id)
        xbrl_text = self._extract_officer_ixbrl(content)
        executives = self._parse_executives(xbrl_text, code=code, doc_id=doc_id)
        return executives, doc_id

    def fetch_from_local_ixbrl(
        self, ixbrl_path: Path, code: str, doc_id: Optional[str] = None
    ) -> list[Executive]:
        """ローカルの iXBRL ファイルから役員リストを抽出する（テスト・再処理用）."""
        text = ixbrl_path.read_text(encoding="utf-8", errors="replace")
        return self._parse_executives(text, code=code, doc_id=doc_id)

    def _download_xbrl_zip(self, doc_id: str) -> bytes:
        """指数バックオフ付きリトライで XBRL ZIPをダウンロード."""
        if not self._api_key:
            raise EdinetFetchError("EDINET_API_KEY が設定されていません")

        url = f"{self._base_url}/documents/{doc_id}"
        params = {"type": "1", "Subscription-Key": self._api_key}
        last_error: Optional[Exception] = None

        for attempt in range(self._max_retries):
            try:
                resp = requests.get(url, params=params, timeout=self._timeout_download)
                if resp.status_code == 200:
                    return resp.content
                if 500 <= resp.status_code < 600:
                    last_error = EdinetFetchError(
                        f"HTTP {resp.status_code} for doc_id={doc_id}"
                    )
                else:
                    raise EdinetFetchError(
                        f"HTTP {resp.status_code} for doc_id={doc_id}"
                    )
            except requests.RequestException as exc:
                last_error = exc
                logger.warning(
                    "EDINET download retry %d/%d doc_id=%s error=%s",
                    attempt + 1,
                    self._max_retries,
                    doc_id,
                    exc,
                )
            wait = min(60, 4 * (2**attempt))
            time.sleep(wait)

        raise EdinetFetchError(f"XBRLダウンロード失敗 doc_id={doc_id}") from last_error

    def _extract_officer_ixbrl(self, zip_bytes: bytes) -> str:
        """XBRL ZIPから役員情報が記載された iXBRL ファイルを抽出する.

        有価証券報告書では `XBRL/PublicDoc/0104010_honbun_*_ixbrl.htm` に集約される。
        """
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                officer_files = sorted(
                    name
                    for name in names
                    if "XBRL/PublicDoc/0104010_" in name and name.endswith("_ixbrl.htm")
                )
                if not officer_files:
                    ixbrl_candidates = [n for n in names if n.endswith("_ixbrl.htm")]
                    raise EdinetParseError(
                        "0104010_*_ixbrl.htm が XBRL ZIP内に見つかりません "
                        f"(iXBRL候補: {ixbrl_candidates[:10]})"
                    )
                return zf.read(officer_files[-1]).decode("utf-8", errors="replace")
        except zipfile.BadZipFile as exc:
            raise EdinetParseError("不正なXBRL ZIP形式") from exc

    def _parse_executives(
        self, xbrl_html: str, code: str, doc_id: Optional[str]
    ) -> list[Executive]:
        """iXBRL テキストから役員リストを抽出する（純粋関数）."""
        soup = BeautifulSoup(xbrl_html, "html.parser")

        # 要素名 → (役員種別, フィールド名) の逆引きマップ
        element_lookup: dict[str, tuple[str, str]] = {}
        for schema_key, fields in ELEMENT_SCHEMAS.items():
            for field_key, element_name in fields.items():
                element_lookup[element_name] = (schema_key, field_key)

        # (contextRef, 役員種別) をキーに属性を集約
        # 同じcontextRefが両種別で共有される場合（Sony等）を分離する
        records: dict[tuple[str, str], dict[str, str]] = {}
        for tag in soup.find_all("ix:nonnumeric"):
            name_attr = tag.get("name")
            ctx = tag.get("contextref")
            if not name_attr or not ctx:
                continue
            mapping = element_lookup.get(name_attr)
            if mapping is None:
                continue
            schema_key, field_key = mapping
            text = normalize_text(tag.get_text())
            records.setdefault((ctx, schema_key), {})[field_key] = text

        executives: list[Executive] = []
        for (_ctx, _schema_key), attrs in records.items():
            name = attrs.get("name")
            if not name:
                continue
            role = attrs.get("role", "")
            executives.append(
                Executive(
                    code=code,
                    name=name,
                    role=role,
                    is_representative="代表" in role,
                    birthdate=attrs.get("birth"),
                    appointed_date=attrs.get("term"),
                    edinet_source_doc_id=doc_id,
                    career_summary=attrs.get("career"),
                )
            )
        return executives
