"""EDINET役員情報取得PoC.

参考実装 `/Users/tak/Markets/Stocks/company-research-agent/` の edinet_client.py と
XBRLパーサーの知見を元に、最小依存（requests + bs4 + 標準ライブラリ）で
役員情報を抽出できるか検証する。

Usage:
    python scripts/poc_edinet_executives.py                       # 7203 / 9984 / 6758
    python scripts/poc_edinet_executives.py 7203 6758
    python scripts/poc_edinet_executives.py --prefer-local        # ローカルXBRL優先
    python scripts/poc_edinet_executives.py --api-only            # EDINET APIでのみ取得
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import dotenv_values

REFERENCE_DOWNLOAD_ROOT = Path(
    "/Users/tak/Markets/Stocks/company-research-agent/downloads"
)
REFERENCE_ENV = Path("/Users/tak/Markets/Stocks/company-research-agent/.env")
LOCAL_ENV = Path(__file__).resolve().parent.parent / ".env"

EDINET_BASE = "https://api.edinet-fsa.go.jp/api/v2"

# 有価証券報告書を識別する EDINET メタ
ASR_ORDINANCE_CODE = "010"
ASR_FORM_CODE = "030000"

# iXBRL要素名（役員の状況セクション）
# グループA: 監査役会設置会社 / 監査等委員会設置会社の「役員」
EL_NAME_DIR = "jpcrp_cor:NameInformationAboutDirectorsAndCorporateAuditors"
EL_ROLE_DIR = (
    "jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors"
)
EL_BIRTH_DIR = "jpcrp_cor:DateOfBirthInformationAboutDirectorsAndCorporateAuditors"
EL_TERM_DIR = "jpcrp_cor:TermOfOfficeInformationAboutDirectorsAndCorporateAuditors"

# グループB: 指名委員会等設置会社の「執行役」
EL_NAME_EXEC = "jpcrp_cor:NameInformationAboutExecutiveDirectors"
EL_ROLE_EXEC = "jpcrp_cor:OfficialTitleOrPositionInformationAboutExecutiveDirectors"
EL_BIRTH_EXEC = "jpcrp_cor:DateOfBirthInformationAboutExecutiveDirectors"
EL_TERM_EXEC = "jpcrp_cor:TermOfOfficeInformationAboutExecutiveDirectors"

# 役員種別（取締役 vs 執行役）ごとに属性をグルーピング
# キー: 種別識別子 ("dir" / "exec")、値: 各フィールドのiXBRL要素名
ELEMENT_SCHEMAS: dict[str, dict[str, str]] = {
    "dir": {
        "name": EL_NAME_DIR,
        "role": EL_ROLE_DIR,
        "birth": EL_BIRTH_DIR,
        "term": EL_TERM_DIR,
    },
    "exec": {
        "name": EL_NAME_EXEC,
        "role": EL_ROLE_EXEC,
        "birth": EL_BIRTH_EXEC,
        "term": EL_TERM_EXEC,
    },
}

DEFAULT_TARGETS = ["7203", "9984", "6758"]


@dataclass
class Executive:
    code: str
    name: str
    role: str
    is_representative: bool
    birthdate: str | None = None
    term_of_office: str | None = None
    context_ref: str = ""


@dataclass
class FetchResult:
    code: str
    source: str  # "local" | "edinet_api" | "missing"
    doc_id: str | None = None
    xbrl_path: str | None = None
    executives: list[Executive] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    elapsed_sec: float = 0.0


def load_api_key() -> str | None:
    """stock-analysis/.env → 参考リポの.env の順で EDINET_API_KEY を読み込む."""
    for path in (LOCAL_ENV, REFERENCE_ENV):
        if not path.exists():
            continue
        values = dotenv_values(path)
        if values.get("EDINET_API_KEY"):
            return values["EDINET_API_KEY"]
    return os.environ.get("EDINET_API_KEY")


def find_local_xbrl(code: str) -> Path | None:
    """参考リポのダウンロード配下で {code}0_* を探し、0104010_*_ixbrl.htm を返す."""
    if not REFERENCE_DOWNLOAD_ROOT.exists():
        return None
    pattern = f"{code}0_*"
    for company_dir in REFERENCE_DOWNLOAD_ROOT.glob(pattern):
        matches = sorted(
            company_dir.glob(
                "120_有価証券報告書/**/XBRL/PublicDoc/0104010_*_ixbrl.htm"
            ),
            reverse=True,
        )
        if matches:
            return matches[0]
    return None


def clean_text(value: str) -> str:
    """XBRLから抽出したテキストの正規化（全角スペース・改行除去、空白連続→1個）."""
    value = value.replace("\u00a0", " ").replace("\u3000", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def extract_executives_from_ixbrl(xbrl_path: Path, code: str) -> list[Executive]:
    """iXBRLファイルから役員情報を抽出する."""
    html = xbrl_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    # 要素名 → (役員種別, フィールド名) の逆引きマップ
    element_lookup: dict[str, tuple[str, str]] = {}
    for schema_key, fields in ELEMENT_SCHEMAS.items():
        for field_key, element_name in fields.items():
            element_lookup[element_name] = (schema_key, field_key)

    # (contextRef, 役員種別) をキーに属性を集約
    # 同じcontextRefが両種別で使われる場合（Sony等の指名委員会等設置会社）を分離
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
        text = clean_text(tag.get_text())
        records.setdefault((ctx, schema_key), {})[field_key] = text

    executives: list[Executive] = []
    for (ctx, schema_key), attrs in records.items():
        name = attrs.get("name")
        if not name:
            continue
        role = attrs.get("role", "")
        is_repr = "代表" in role
        executives.append(
            Executive(
                code=code,
                name=name,
                role=role,
                is_representative=is_repr,
                birthdate=attrs.get("birth"),
                term_of_office=attrs.get("term"),
                context_ref=ctx,
            )
        )
    return executives


def find_asr_doc_via_api(
    sec_code4: str, api_key: str, months_back: int = 18
) -> tuple[str | None, list[str]]:
    """EDINET APIで対象銘柄の直近有価証券報告書の doc_id を探す.

    Returns:
        (doc_id, error_messages)
    """
    errors: list[str] = []
    sec_code5 = sec_code4 + "0"  # EDINETは5桁
    today = date.today()
    # 有報の提出期限は期末から3ヶ月以内。直近18ヶ月を探索
    for offset in range(months_back * 31):
        target_day = today - timedelta(days=offset)
        resp = requests.get(
            f"{EDINET_BASE}/documents.json",
            params={
                "date": target_day.strftime("%Y-%m-%d"),
                "type": "2",
                "Subscription-Key": api_key,
            },
            timeout=30,
        )
        if resp.status_code == 404:
            continue  # その日は提出なし
        if resp.status_code != 200:
            errors.append(f"{target_day}: HTTP {resp.status_code}")
            continue
        data = resp.json()
        for entry in data.get("results", []) or []:
            if (
                entry.get("secCode") == sec_code5
                and entry.get("ordinanceCode") == ASR_ORDINANCE_CODE
                and entry.get("formCode") == ASR_FORM_CODE
            ):
                return entry.get("docID"), errors
        # 礼儀としてレート制限を回避する軽いスリープ
        time.sleep(0.05)
    return None, errors


def download_and_extract_xbrl(
    doc_id: str, api_key: str, extract_dir: Path
) -> Path | None:
    """XBRL ZIPをダウンロードして展開し、0104010の iXBRL パスを返す."""
    resp = requests.get(
        f"{EDINET_BASE}/documents/{doc_id}",
        params={"type": "1", "Subscription-Key": api_key},
        timeout=120,
    )
    if resp.status_code != 200:
        return None
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(extract_dir)
    matches = sorted(
        extract_dir.glob("**/XBRL/PublicDoc/0104010_*_ixbrl.htm"),
        reverse=True,
    )
    return matches[0] if matches else None


def process_code(
    code: str,
    api_key: str | None,
    prefer_local: bool,
    api_only: bool,
    cache_dir: Path,
) -> FetchResult:
    start = time.perf_counter()
    result = FetchResult(code=code, source="missing")

    # 1) ローカル XBRL を優先/フォールバックで試す
    if not api_only:
        local = find_local_xbrl(code)
        if local:
            try:
                execs = extract_executives_from_ixbrl(local, code)
                result.source = "local"
                result.xbrl_path = str(local)
                result.executives = execs
                result.elapsed_sec = time.perf_counter() - start
                return result
            except Exception as exc:  # pragma: no cover - PoC
                result.errors.append(f"local parse error: {exc}")

    if api_only or (prefer_local is False):
        pass

    # 2) API 経由（ローカル不在 or --api-only）
    if api_key is None:
        result.errors.append("EDINET_API_KEY not found; skip API fetch")
        result.elapsed_sec = time.perf_counter() - start
        return result

    doc_id, api_errors = find_asr_doc_via_api(code, api_key)
    result.errors.extend(api_errors[:5])  # 冗長回避
    if doc_id is None:
        result.errors.append("no ASR document found via EDINET API")
        result.elapsed_sec = time.perf_counter() - start
        return result

    target_dir = cache_dir / code / doc_id
    xbrl_path = download_and_extract_xbrl(doc_id, api_key, target_dir)
    if xbrl_path is None:
        result.errors.append(f"failed to extract XBRL for doc_id={doc_id}")
        result.elapsed_sec = time.perf_counter() - start
        return result
    try:
        execs = extract_executives_from_ixbrl(xbrl_path, code)
        result.source = "edinet_api"
        result.doc_id = doc_id
        result.xbrl_path = str(xbrl_path)
        result.executives = execs
    except Exception as exc:  # pragma: no cover - PoC
        result.errors.append(f"api xbrl parse error: {exc}")

    result.elapsed_sec = time.perf_counter() - start
    return result


def summarize(results: list[FetchResult]) -> dict:
    summary = {
        "total_codes": len(results),
        "success_codes": sum(1 for r in results if r.executives),
        "per_code": [],
    }
    for r in results:
        repr_count = sum(1 for e in r.executives if e.is_representative)
        summary["per_code"].append(
            {
                "code": r.code,
                "source": r.source,
                "doc_id": r.doc_id,
                "exec_count": len(r.executives),
                "representative_count": repr_count,
                "elapsed_sec": round(r.elapsed_sec, 2),
                "errors": r.errors,
            }
        )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="EDINET役員情報取得 PoC")
    parser.add_argument("codes", nargs="*", default=DEFAULT_TARGETS)
    parser.add_argument(
        "--prefer-local",
        action="store_true",
        help="ローカル参考実装のXBRLを優先（APIはフォールバック）",
    )
    parser.add_argument(
        "--api-only",
        action="store_true",
        help="必ずEDINET APIから取得する",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/poc_edinet_executives.json"),
        help="結果JSONの出力先",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data/cache/edinet_xbrl"),
        help="API取得時の一時展開先",
    )
    args = parser.parse_args()

    api_key = load_api_key()
    if api_key is None and args.api_only:
        print("ERROR: EDINET_API_KEY not configured", file=sys.stderr)
        return 2
    if api_key is None:
        print(
            "WARN: EDINET_API_KEY未設定 — ローカルXBRLのみで実行します", file=sys.stderr
        )

    results: list[FetchResult] = []
    for code in args.codes:
        code = code.strip()
        print(f"\n[{code}] 取得開始...", file=sys.stderr)
        r = process_code(
            code,
            api_key=api_key,
            prefer_local=args.prefer_local or (not args.api_only),
            api_only=args.api_only,
            cache_dir=args.cache_dir,
        )
        results.append(r)
        print(
            f"[{code}] source={r.source} count={len(r.executives)} "
            f"repr={sum(1 for e in r.executives if e.is_representative)} "
            f"elapsed={r.elapsed_sec:.2f}s errors={len(r.errors)}",
            file=sys.stderr,
        )
        if r.errors:
            for err in r.errors:
                print(f"    - {err}", file=sys.stderr)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": date.today().isoformat(),
        "summary": summarize(results),
        "details": [
            {
                "code": r.code,
                "source": r.source,
                "doc_id": r.doc_id,
                "xbrl_path": r.xbrl_path,
                "executives": [asdict(e) for e in r.executives],
                "errors": r.errors,
                "elapsed_sec": round(r.elapsed_sec, 2),
            }
            for r in results
        ],
    }
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        f"\n=== 完了 === 成功 {payload['summary']['success_codes']}/{payload['summary']['total_codes']}"
        f"  出力: {args.output}",
        file=sys.stderr,
    )
    return 0 if payload["summary"]["success_codes"] == len(args.codes) else 1


if __name__ == "__main__":
    sys.exit(main())
