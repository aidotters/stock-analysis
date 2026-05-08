"""四季報銘柄ページの「この銘柄の関連記事」を Playwright で取得 (Phase 3)。

URL: `https://shikiho.toyokeizai.net/stocks/{code}` (Nuxt.js SPA、ログイン不要)
Page DOM (2026-05時点):
    .news                       <- ニュースセクション (ページ内に2つある)
        .news__title           "この銘柄の関連記事" or "東洋経済オンライン掲載記事"
        .news__body
            .news__body__item
                <a href="/news/X/YYYYY">タイトル</a>
                .news__body__item__right
                    "YYYY/MM/DD HH:MM"
                    "カテゴリ"

採用するセクションは「この銘柄の関連記事」のみ。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from market_pipeline.news.config_parser import FilterKeywords, NewsSource, load_config
from market_pipeline.news_delivery.exceptions import DisclosureFetchError
from market_pipeline.news_delivery.fetchers.base import BaseFetcher
from market_pipeline.news_delivery.models import Importance, NewsItem

logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT_MS = 30000
DEFAULT_RETRY_DELAYS = (1.0, 2.0, 4.0)
DEFAULT_CONFIG_PATH = "config/news_sources.yaml"
DEFAULT_CATEGORY_KEY = "stock_news"
DEFAULT_PROFILE_DIR = Path.home() / ".stock-news" / "chrome-profile"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
TARGET_SECTION_TITLE = "この銘柄の関連記事"


class ShikihoStockNewsFetcher(BaseFetcher):
    """四季報銘柄ページの関連記事を Playwright で取得する。"""

    source_name = "shikiho_stock_news"
    category = "stock_news"

    def __init__(
        self,
        *,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        category_key: str = DEFAULT_CATEGORY_KEY,
        profile_dir: str | Path = DEFAULT_PROFILE_DIR,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
        headless: bool = True,
        user_agent: str = DEFAULT_USER_AGENT,
        lookback_days: Optional[int] = None,
        page_factory=None,  # type: ignore[no-untyped-def]
        now_fn=datetime.now,  # type: ignore[no-untyped-def]
        sleep_fn=time.sleep,  # type: ignore[no-untyped-def]
    ) -> None:
        self._config_path = Path(config_path)
        self._category_key = category_key
        self._profile_dir = Path(profile_dir).expanduser()
        self._profile_dir.mkdir(parents=True, exist_ok=True)
        self._timeout_ms = timeout_ms
        self._retry_delays = retry_delays
        self._headless = headless
        self._user_agent = user_agent
        self._lookback_days = lookback_days
        self._page_factory = page_factory
        self._now_fn = now_fn
        self._sleep = sleep_fn
        self._source = self._load_source()

    def _load_source(self) -> NewsSource:
        config = load_config(self._config_path)
        sources = config.sources.get(self._category_key, [])
        if not sources:
            raise DisclosureFetchError(
                code="-",
                source=self.source_name,
                message=f"No '{self._category_key}' sources defined in {self._config_path}",
            )
        return sources[0]

    @property
    def base_url(self) -> str:
        return self._source.url or "https://shikiho.toyokeizai.net/"

    @property
    def filter_keywords(self) -> Optional[FilterKeywords]:
        return self._source.filter_keywords

    def _build_url(self, code: str) -> str:
        tmpl = (
            self._source.url_template or "https://shikiho.toyokeizai.net/stocks/{code}"
        )
        return tmpl.format(code=code)

    # ---------------------------------------------------------------- parse

    def _parse(self, code: str, html: str) -> list[NewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[NewsItem] = []
        seen: set[str] = set()
        cutoff: Optional[datetime] = None
        if self._lookback_days is not None and self._lookback_days >= 0:
            cutoff = self._now_fn() - timedelta(days=self._lookback_days)
        max_items = self._source.max_items_per_code or 8

        target_section = self._find_target_section(soup)
        if target_section is None:
            return items

        for node in target_section.select(".news__body__item"):
            link = node.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            url = urljoin("https://shikiho.toyokeizai.net/", href)
            if url in seen:
                continue

            # 実HTMLでは <a> 内テキストが空のため、.news__body__item__right の
            # 先頭チャンクをタイトルとして採用する。
            right = node.select_one(".news__body__item__right")
            title = self._extract_title(node, link, right)
            if not title:
                continue

            published_at = self._extract_published_at_from_right(right)
            if (
                cutoff is not None
                and published_at is not None
                and published_at < cutoff
            ):
                continue

            if not self._title_passes_filter(title):
                continue

            summary = self._extract_category_label_from_right(right) or None

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
                    importance=self._estimate_importance(title),
                )
            )
            if len(items) >= max_items:
                break
        return items

    def _find_target_section(self, soup: BeautifulSoup):  # type: ignore[no-untyped-def]
        for sec in soup.select(".news"):
            title_el = sec.select_one(".news__title")
            if title_el and TARGET_SECTION_TITLE in title_el.get_text(strip=True):
                return sec
        return None

    def _extract_title(self, node, link, right) -> str:  # type: ignore[no-untyped-def]
        # 優先: <a> のテキスト
        if link is not None:
            t = link.get_text(strip=True)
            if t:
                return t
        # 代替: .news__body__item__right の最初の非日付チャンクをタイトルとする
        if right is None:
            return ""
        for chunk in right.stripped_strings:
            if self._parse_date(chunk) is None:
                return chunk
        return ""

    def _extract_published_at_from_right(self, right) -> Optional[datetime]:  # type: ignore[no-untyped-def]
        if right is None:
            return None
        for chunk in right.stripped_strings:
            dt = self._parse_date(chunk)
            if dt is not None:
                return dt
        return None

    def _extract_category_label_from_right(self, right) -> str:  # type: ignore[no-untyped-def]
        if right is None:
            return ""
        chunks = list(right.stripped_strings)
        if len(chunks) >= 3:
            last = chunks[-1]
            if self._parse_date(last) is None:
                return last
        return ""

    @staticmethod
    def _parse_date(raw: str) -> Optional[datetime]:
        raw = raw.strip()
        if not raw:
            return None
        for fmt in ("%Y/%m/%d %H:%M", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
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

    # ---------------------------------------------------------------- fetch

    def _open_context(self):  # type: ignore[no-untyped-def]
        from playwright.sync_api import sync_playwright

        pw = sync_playwright().start()
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(self._profile_dir),
            headless=self._headless,
            user_agent=self._user_agent,
        )
        return pw, context

    def _fetch_html_via_page(self, page, code: str) -> str:  # type: ignore[no-untyped-def]
        url = self._build_url(code)
        last_exc: Optional[Exception] = None
        attempts = len(self._retry_delays) + 1
        for attempt in range(attempts):
            try:
                page.goto(url, wait_until="networkidle", timeout=self._timeout_ms)
                # 「この銘柄の関連記事」が出るまで少し待つ。出ない場合は0件として処理。
                try:
                    page.wait_for_selector(".news__body__item", timeout=5000)
                except Exception:
                    pass
                return page.content()
            except Exception as e:  # noqa: BLE001
                last_exc = e
                if attempt < len(self._retry_delays):
                    delay = self._retry_delays[attempt]
                    logger.warning(
                        "Stock news fetch retry %d/%d for code=%s after %ss: %s",
                        attempt + 1,
                        attempts,
                        code,
                        delay,
                        e,
                    )
                    self._sleep(delay)
                    continue
                break
        raise DisclosureFetchError(
            code=code,
            source=self.source_name,
            message=f"Failed to fetch stock news for {code}: {last_exc}",
        )

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

        if self._page_factory is not None:
            page = self._page_factory()
            try:
                for code in codes:
                    self._fetch_one_into(page, code, results, errors)
            finally:
                close = getattr(page, "close", None)
                if callable(close):
                    close()
            return results, errors

        # 実 Chromium 起動。launch 自体に失敗した場合は全銘柄分のエラーを積む。
        try:
            pw, context = self._open_context()
        except Exception as e:  # noqa: BLE001
            logger.warning("ShikihoStockNewsFetcher: CDP接続失敗 (%s)", e)
            wrapped = DisclosureFetchError(
                code="-",
                source=self.source_name,
                message=f"CDP接続失敗: {e}",
            )
            for c in codes:
                errors[c] = wrapped
            return results, errors

        try:
            page = context.new_page()
            for code in codes:
                self._fetch_one_into(page, code, results, errors)
        finally:
            try:
                context.close()
            finally:
                pw.stop()
        return results, errors

    def _fetch_one_into(
        self,
        page,
        code: str,
        results: list[NewsItem],
        errors: dict[str, Exception],  # type: ignore[no-untyped-def]
    ) -> None:
        try:
            html = self._fetch_html_via_page(page, code)
            results.extend(self._parse(code, html))
        except Exception as e:  # noqa: BLE001
            logger.warning("ShikihoStockNewsFetcher failed for %s: %s", code, e)
            errors[code] = e
