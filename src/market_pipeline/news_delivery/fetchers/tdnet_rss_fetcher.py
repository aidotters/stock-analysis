"""TDnet RSS fetcher (Phase 2)。

`https://webapi.yanoshin.jp/webapi/tdnet/list/{code}.atom` (yanoshin TDnet ラッパー)
を巡回し、銘柄ごとの適時開示を `NewsItem` に正規化する。

Phase 1 の `CdpDisclosureFetcher` (四季報スクレイピング) との重複は
`Deduplicator` の URL ハッシュで吸収される (TDnet のリンクが直接 release.tdnet.info
の PDF を指すのに対し、四季報側は `/viewer-pdf?fileName=...` の中継URLなので
URL は別になるが、片方を採用するならこちらの方が一次情報に近い)。

依存: feedparser >= 6
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import feedparser

from market_pipeline.news.config_parser import FilterKeywords, NewsSource, load_config
from market_pipeline.news_delivery.fetchers.base import BaseFetcher
from market_pipeline.news_delivery.models import Importance, NewsItem
from market_pipeline.news_delivery.rate_limiter import RateLimiter, RateLimitError

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/news_sources.yaml"
DEFAULT_CATEGORY_KEY = "ir_release"
DEFAULT_RPM = 60


FeedParser = Callable[[str], object]


class TdnetRssFetcher(BaseFetcher):
    """yanoshin TDnet ラッパー (Atom) から適時開示を取得する。"""

    source_name = "tdnet_rss"
    category = "ir_release"

    def __init__(
        self,
        *,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        category_key: str = DEFAULT_CATEGORY_KEY,
        rate_limiter: Optional[RateLimiter] = None,
        requests_per_minute: int = DEFAULT_RPM,
        lookback_days: Optional[int] = None,
        feed_parser: Optional[FeedParser] = None,
        now_fn: Callable[[], datetime] = datetime.now,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config_path = Path(config_path)
        self._category_key = category_key
        self._source = self._load_source()
        self._rate_limiter = rate_limiter or RateLimiter(requests_per_minute)
        self._lookback_days = lookback_days
        self._feed_parser: FeedParser = feed_parser or feedparser.parse
        self._now_fn = now_fn
        self._sleep = sleep_fn

    def _load_source(self) -> NewsSource:
        config = load_config(self._config_path)
        sources = config.sources.get(self._category_key, [])
        if not sources:
            raise ValueError(
                f"No '{self._category_key}' sources defined in {self._config_path}"
            )
        return sources[0]

    @property
    def filter_keywords(self) -> Optional[FilterKeywords]:
        return self._source.filter_keywords

    def _build_url(self, code: str) -> str:
        url_tmpl = self._source.url_template or (
            "https://webapi.yanoshin.jp/webapi/tdnet/list/{code}.atom"
        )
        return url_tmpl.format(code=code)

    def _parse_feed(self, code: str, feed) -> list[NewsItem]:  # type: ignore[no-untyped-def]
        items: list[NewsItem] = []
        cutoff: Optional[datetime] = None
        if self._lookback_days is not None and self._lookback_days >= 0:
            cutoff = self._now_fn() - timedelta(days=self._lookback_days)

        max_items = self._source.max_items_per_code or 30
        seen: set[str] = set()
        entries = getattr(feed, "entries", []) or []

        for entry in entries:
            title = getattr(entry, "title", "") or ""
            url = getattr(entry, "link", "") or ""
            if not title or not url or url in seen:
                continue

            # yanoshin の title は "{社名}:{タイトル}" 形式。"{社名}:" を剥がす
            cleaned_title = self._strip_company_prefix(title)
            if not self._title_passes_filter(cleaned_title):
                continue

            published_at = self._parse_published(entry)
            if (
                cutoff is not None
                and published_at is not None
                and published_at < cutoff
            ):
                continue

            seen.add(url)
            items.append(
                NewsItem(
                    code=code,
                    title=cleaned_title,
                    url=url,
                    source=self.source_name,
                    category=self.category,
                    published_at=published_at,
                    summary=None,
                    importance=self._estimate_importance(cleaned_title),
                )
            )
            if len(items) >= max_items:
                break
        return items

    @staticmethod
    def _strip_company_prefix(title: str) -> str:
        # "Ｓａｎｓａｎ:業績予想の修正に関するお知らせ" → "業績予想の修正..."
        if ":" in title:
            return title.split(":", 1)[1].strip()
        return title

    def _parse_published(self, entry) -> Optional[datetime]:  # type: ignore[no-untyped-def]
        struct = getattr(entry, "published_parsed", None) or getattr(
            entry, "updated_parsed", None
        )
        if struct is None:
            return None
        try:
            return (
                datetime(*struct[:6], tzinfo=timezone.utc)
                .astimezone()
                .replace(tzinfo=None)
            )
        except (TypeError, ValueError):
            return None

    def _title_passes_filter(self, title: str) -> bool:
        fk = self.filter_keywords
        if fk is None:
            return True
        for kw in fk.exclude:
            if kw and kw in title:
                return False
        return True

    def _estimate_importance(self, title: str) -> Importance:
        fk = self.filter_keywords
        if fk is None:
            return "mid"
        for kw in fk.include:
            if kw and kw in title:
                return "high"
        return "mid"

    def fetch_for_codes(self, codes: list[str]) -> list[NewsItem]:
        items, _ = self.fetch_for_codes_with_errors(codes)
        return items

    def fetch_for_codes_with_errors(
        self, codes: list[str]
    ) -> tuple[list[NewsItem], dict[str, Exception]]:
        if not codes:
            return [], {}
        results: list[NewsItem] = []
        errors: dict[str, Exception] = {}
        skipped = 0

        for code in codes:
            if not self._rate_limiter.acquire():
                skipped += 1
                continue
            url = self._build_url(code)
            try:
                feed = self._feed_parser(url)
                results.extend(self._parse_feed(code, feed))
            except Exception as e:  # noqa: BLE001
                logger.warning("TdnetRssFetcher failed for %s: %s", code, e)
                errors[code] = e

        if skipped > 0:
            raise RateLimitError(
                f"{self.source_name}: rate limit hit; skipped {skipped} codes",
                skipped_count=skipped,
            )
        return results, errors
