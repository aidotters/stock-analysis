"""Google News RSS fetcher (Phase 2)。

`https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja` を巡回し、
銘柄ごとの一般ニュースを `NewsItem` に正規化する。

レート制限:
    - `RateLimiter(rpm=N)` を共有。N 回 / 分を超えそうな場合は `RateLimitError`
      を raise し、呼び出し側 (DeliveryService) で priority=high の銘柄のみ
      再試行させる。

依存:
    - feedparser >= 6
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import quote_plus

import feedparser

from market_pipeline.news.config_parser import FilterKeywords, NewsSource, load_config
from market_pipeline.news_delivery.fetchers.base import BaseFetcher
from market_pipeline.news_delivery.models import NewsItem
from market_pipeline.news_delivery.rate_limiter import RateLimiter, RateLimitError

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/news_sources.yaml"
DEFAULT_CATEGORY_KEY = "general_news"
DEFAULT_RPM = 30


# テスト・依存性注入用: feedparser.parse をオーバーライド可能に
FeedParser = Callable[[str], object]


class GoogleNewsRssFetcher(BaseFetcher):
    """Google News RSS から銘柄ごとの一般ニュースを取得する。"""

    source_name = "google_news_rss"
    category = "general_news"

    def __init__(
        self,
        *,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        category_key: str = DEFAULT_CATEGORY_KEY,
        rate_limiter: Optional[RateLimiter] = None,
        requests_per_minute: int = DEFAULT_RPM,
        lookback_days: Optional[int] = None,
        feed_parser: Optional[FeedParser] = None,
        meta_resolver: Optional[Callable[[str], dict]] = None,
        now_fn: Callable[[], datetime] = datetime.now,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config_path = Path(config_path)
        self._category_key = category_key
        self._source = self._load_source()
        self._rate_limiter = rate_limiter or RateLimiter(requests_per_minute)
        self._lookback_days = lookback_days
        self._feed_parser: FeedParser = feed_parser or feedparser.parse
        self._meta_resolver = meta_resolver or (lambda code: {})
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

    # ---------------------------------------------------------------- query

    def _build_query(self, code: str) -> str:
        meta = self._meta_resolver(code) or {}
        long_name = meta.get("long_name") or ""
        template = self._source.query_template or "{long_name} {code}"
        try:
            query = template.format(code=code, long_name=long_name).strip()
        except (KeyError, IndexError):
            query = f"{long_name} {code}".strip()
        if not query:
            query = code
        return query

    def _build_url(self, code: str) -> str:
        query = self._build_query(code)
        url_tmpl = self._source.url_template or (
            "https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"
        )
        return url_tmpl.format(query=quote_plus(query))

    # ---------------------------------------------------------------- parse

    def _parse_feed(self, code: str, feed) -> list[NewsItem]:  # type: ignore[no-untyped-def]
        items: list[NewsItem] = []
        cutoff: Optional[datetime] = None
        if self._lookback_days is not None and self._lookback_days >= 0:
            cutoff = self._now_fn() - timedelta(days=self._lookback_days)

        max_items = self._source.max_items_per_code or 10
        seen: set[str] = set()
        entries = getattr(feed, "entries", []) or []

        for entry in entries:
            title = getattr(entry, "title", "") or ""
            url = getattr(entry, "link", "") or ""
            if not title or not url or url in seen:
                continue
            if not self._title_passes_filter(title):
                continue

            published_at = self._parse_published(entry)
            if (
                cutoff is not None
                and published_at is not None
                and published_at < cutoff
            ):
                continue

            summary = self._extract_source_name(entry) or None

            seen.add(url)
            items.append(
                NewsItem(
                    code=code,
                    title=title,
                    url=url,
                    source=self.source_name,
                    category=self.category,
                    published_at=published_at,
                    summary=summary,
                    importance="mid",
                )
            )
            if len(items) >= max_items:
                break
        return items

    def _parse_published(self, entry) -> Optional[datetime]:  # type: ignore[no-untyped-def]
        # feedparser は published_parsed を struct_time で返す
        struct = getattr(entry, "published_parsed", None) or getattr(
            entry, "updated_parsed", None
        )
        if struct is None:
            return None
        try:
            # struct_time は UTC 想定で来るので tz-aware の datetime に変換しローカル比較に揃える
            return (
                datetime(*struct[:6], tzinfo=timezone.utc)
                .astimezone()
                .replace(tzinfo=None)
            )
        except (TypeError, ValueError):
            return None

    def _extract_source_name(self, entry) -> str:  # type: ignore[no-untyped-def]
        src = getattr(entry, "source", None)
        if isinstance(src, dict):
            return src.get("title", "") or ""
        title = getattr(src, "title", "") if src is not None else ""
        return title or ""

    def _title_passes_filter(self, title: str) -> bool:
        fk = self.filter_keywords
        if fk is None:
            return True
        for kw in fk.exclude:
            if kw and kw in title:
                return False
        return True

    # ---------------------------------------------------------------- fetch

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
                logger.warning("GoogleNewsRssFetcher failed for %s: %s", code, e)
                errors[code] = e

        if skipped > 0:
            raise RateLimitError(
                f"{self.source_name}: rate limit hit; skipped {skipped} codes",
                skipped_count=skipped,
            )
        return results, errors
