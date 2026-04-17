"""役員の外部発信コンテンツ収集モジュール.

設計:
- WebSearchの呼び出しは `web_search_fn` として注入可能にする（DI パターン）
- Claude Code スキル層が内蔵 WebSearch ツールを使って検索結果を取得し、このコレクタに渡す
- Python スクリプトから直接呼ぶ場合は、事前にスキル側で結果を収集して受け渡す想定
- 30日キャッシュは ExecutiveRepository が管理する
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional
from urllib.parse import urlparse

from market_pipeline.config import get_settings
from market_pipeline.executives.published_date_extractor import (
    extract_published_date,
)
from market_pipeline.executives.repository import ExecutiveRepository

logger = logging.getLogger(__name__)


# WebSearch結果の想定スキーマ:
# [{"url": "...", "title": "...", "snippet": "...", "published_date": "YYYY-MM-DD"}]
WebSearchFn = Callable[[str], list[dict]]

# URL → YYYY-MM-DD (or None) を返す日付抽出関数
DateExtractorFn = Callable[[str], Optional[str]]


@dataclass
class Communication:
    """収集した発信コンテンツ."""

    code: str
    executive_name: str
    source_url: str
    source_type: Optional[str] = None
    published_date: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None


# ドメイン→source_type のマッピング（一次分類）
_DOMAIN_TYPE_MAP: dict[str, str] = {
    "note.com": "blog",
    "medium.com": "blog",
    "ameblo.jp": "blog",
    "hatenablog.com": "blog",
    "youtube.com": "speech",
    "youtu.be": "speech",
    "prtimes.jp": "article",
    "diamond.jp": "article",
    "toyokeizai.net": "article",
    "nikkei.com": "article",
    "forbesjapan.com": "article",
    "newspicks.com": "article",
    "reuters.com": "article",
    "bloomberg.co.jp": "article",
    "logmi.jp": "speech",
}


def classify_source_type(url: str, title: str = "") -> str:
    """URLドメインとタイトルキーワードから発信カテゴリを一次分類する."""
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        host = ""
    for domain, stype in _DOMAIN_TYPE_MAP.items():
        if host.endswith(domain):
            return stype

    lowered = title
    if "インタビュー" in lowered:
        return "interview"
    if "講演" in lowered or "スピーチ" in lowered:
        return "speech"
    if "書籍" in lowered or "著書" in lowered:
        return "book"
    if "寄稿" in lowered:
        return "article"
    return "article"


SEARCH_KEYWORDS = (
    "インタビュー",
    "講演",
    "対談",
    "コラム",
    "ブログ",
    "記事",
    "寄稿",
    "note",
    "メッセージ",
    "登壇",
)


def build_search_query(
    name: str, company: str, *, since_date: Optional[str] = None
) -> str:
    """PoC確定事項: 汎用クエリ1本で発信を拾う.

    since_date (YYYY-MM-DD) が指定された場合、Google の ``after:`` 演算子を末尾に
    付与して期間を絞る。プロダクト系プレスリリースのノイズを避けるため、
    ``プレス`` / ``ニュース`` / ``発表`` は含めない。
    """
    kw = " OR ".join(SEARCH_KEYWORDS)
    base = f'"{name}" "{company}" ({kw})'
    if since_date:
        return f"{base} after:{since_date}"
    return base


class CommunicationCollector:
    """WebSearch + 30日キャッシュでの発信収集.

    Example:
        def my_search(q: str) -> list[dict]:
            # Claude Codeスキル層では WebSearch(q) を呼ぶ
            return [{"url": "...", "title": "...", "snippet": "...", "published_date": "2026-02-15"}]

        collector = CommunicationCollector(web_search_fn=my_search)
        comms = collector.collect("佐藤恒治", "トヨタ自動車", code="7203")
    """

    def __init__(
        self,
        *,
        web_search_fn: Optional[WebSearchFn] = None,
        repository: Optional[ExecutiveRepository] = None,
        cache_ttl_days: Optional[int] = None,
        date_extractor_fn: Optional[DateExtractorFn] = extract_published_date,
        lookback_days: Optional[int] = None,
    ) -> None:
        settings = get_settings()
        self._web_search_fn = web_search_fn
        self._repo = repository or ExecutiveRepository()
        self._cache_ttl_days = (
            cache_ttl_days
            if cache_ttl_days is not None
            else settings.executives.cache_ttl_days
        )
        self._date_extractor_fn = date_extractor_fn
        # 遅延インポートで循環参照を回避
        from market_pipeline.executives import LOOKBACK_DAYS_TOTAL

        self._lookback_days = (
            lookback_days if lookback_days is not None else LOOKBACK_DAYS_TOTAL
        )

    def _since_date(self) -> str:
        """収集・読み出しの対象下限日付 (YYYY-MM-DD)."""
        return (datetime.now().date() - timedelta(days=self._lookback_days)).strftime(
            "%Y-%m-%d"
        )

    def collect(
        self,
        name: str,
        company: str,
        *,
        code: str,
        force_refresh: bool = False,
    ) -> list[Communication]:
        """指定役員の発信コンテンツを収集する.

        Args:
            name: 役員氏名
            company: 企業名（検索クエリ構築に使用）
            code: 銘柄コード
            force_refresh: True なら30日キャッシュを無視して再取得

        Returns:
            収集した Communication のリスト。キャッシュ有効時は既存DBレコードを返す
        """
        since_date = self._since_date()

        if not force_refresh and self._repo.is_cache_valid(
            name, ttl_days=self._cache_ttl_days
        ):
            logger.info(
                "発信キャッシュ有効 (TTL=%d日) name=%s — 再収集をスキップ",
                self._cache_ttl_days,
                name,
            )
            return [
                self._row_to_comm(row)
                for row in self._repo.get_communications(name, since_date=since_date)
            ]

        if self._web_search_fn is None:
            logger.warning(
                "web_search_fn が未設定のため再収集スキップ name=%s "
                "— Claude Code スキル層から呼び出してください",
                name,
            )
            return [
                self._row_to_comm(row)
                for row in self._repo.get_communications(name, since_date=since_date)
            ]

        query = build_search_query(name, company, since_date=since_date)
        try:
            raw_results = self._web_search_fn(query)
        except Exception as exc:  # noqa: BLE001 - 単一役員の失敗でバッチ止めない
            logger.error(
                "WebSearch失敗 name=%s error=%s — 既存キャッシュを返却",
                name,
                exc,
            )
            return [
                self._row_to_comm(row)
                for row in self._repo.get_communications(name, since_date=since_date)
            ]

        communications = self._normalize_results(
            raw_results, code=code, executive_name=name
        )
        if not communications:
            logger.info("発信0件 name=%s", name)
            return []

        records = [
            {
                "code": c.code,
                "executive_name": c.executive_name,
                "source_url": c.source_url,
                "source_type": c.source_type,
                "published_date": c.published_date,
                "title": c.title,
                "summary": c.summary,
            }
            for c in communications
        ]
        inserted = self._repo.upsert_communications(records)
        logger.info(
            "発信収集完了 name=%s total=%d inserted=%d",
            name,
            len(communications),
            inserted,
        )
        return communications

    def _normalize_results(
        self, raw: list[dict], *, code: str, executive_name: str
    ) -> list[Communication]:
        """WebSearch生データを Communication に正規化する.

        published_date が WebSearch 結果に無い場合は、date_extractor_fn
        （既定は requests で URL取得→ meta/JSON-LD 抽出）にフォールバック。
        """
        communications: list[Communication] = []
        seen_urls: set[str] = set()
        for entry in raw:
            url = entry.get("url") or entry.get("link")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            title = entry.get("title") or ""
            summary = entry.get("snippet") or entry.get("description")
            published = _normalize_date(
                entry.get("published_date") or entry.get("date")
            )
            if published is None and self._date_extractor_fn is not None:
                try:
                    published = self._date_extractor_fn(url)
                except Exception as exc:  # noqa: BLE001 - 個別URLの失敗で止めない
                    logger.debug("発信日抽出失敗 url=%s error=%s", url, exc)
                    published = None
            communications.append(
                Communication(
                    code=code,
                    executive_name=executive_name,
                    source_url=url,
                    source_type=classify_source_type(url, title),
                    published_date=published,
                    title=title,
                    summary=summary,
                )
            )
        return communications

    @staticmethod
    def _row_to_comm(row: dict) -> Communication:
        return Communication(
            code=row.get("code", ""),
            executive_name=row.get("executive_name", ""),
            source_url=row.get("source_url", ""),
            source_type=row.get("source_type"),
            published_date=row.get("published_date"),
            title=row.get("title"),
            summary=row.get("summary"),
        )


_DATE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})"),
    re.compile(r"^(\d{4})/(\d{1,2})/(\d{1,2})"),
    re.compile(r"^(\d{4})年(\d{1,2})月(\d{1,2})日"),
    re.compile(r"^(\d{4})(\d{2})(\d{2})$"),
)


def _normalize_date(value: Optional[str]) -> Optional[str]:
    """発信日を YYYY-MM-DD 形式に正規化する（失敗時はそのまま返す）.

    先頭のプレフィックスに一致する形式を正規表現で抽出するため、
    ISO 8601 のタイムゾーン部や末尾のノイズがあっても安全に処理できる。
    """
    if not value:
        return None
    stripped = value.strip()
    for pattern in _DATE_PATTERNS:
        match = pattern.match(stripped)
        if not match:
            continue
        try:
            y, m, d = (int(g) for g in match.groups())
            return f"{y:04d}-{m:02d}-{d:02d}"
        except ValueError:
            continue
    return value
