"""四季報適時開示ページを Playwright (CDP) で取得する Fetcher。

四季報の適時開示ページは Nuxt.js SPA のため、`requests + BeautifulSoup4` では
ゼロ件しか取れない。本 Fetcher は専用 Chromium プロファイル
(`~/.stock-news/chrome-profile`) を Playwright sync API で起動し、
レンダリング後の DOM を BeautifulSoup でパースして `NewsItem` に正規化する。

DOM 構造（2026-05時点）:
    .newsList__item
        .newsList__title       タイトル
        .newsList__stocks      銘柄コード + 銘柄名
        .newsList__date        MM/DD HH:MM (年は推論)
        a[href]                /viewer-pdf?fileName=... (相対URL)
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
DEFAULT_PROFILE_DIR = Path.home() / ".stock-news" / "chrome-profile"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)


class CdpDisclosureFetcher(BaseFetcher):
    """Playwright で四季報適時開示ページを巡回する Fetcher。"""

    source_name = "shikiho_disclosure"
    category = "disclosure"

    def __init__(
        self,
        *,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        profile_dir: str | Path = DEFAULT_PROFILE_DIR,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
        headless: bool = True,
        user_agent: str = DEFAULT_USER_AGENT,
        lookback_days: Optional[int] = None,
        now_fn=datetime.now,  # type: ignore[no-untyped-def]
        page_factory=None,  # type: ignore[no-untyped-def]
        sleep_fn=time.sleep,  # type: ignore[no-untyped-def]
    ) -> None:
        self._timeout_ms = timeout_ms
        self._retry_delays = retry_delays
        self._headless = headless
        self._user_agent = user_agent
        self._profile_dir = Path(profile_dir).expanduser()
        self._profile_dir.mkdir(parents=True, exist_ok=True)
        self._page_factory = page_factory
        self._sleep = sleep_fn
        self._now_fn = now_fn
        self._lookback_days = lookback_days  # None = フィルタ無効
        self._config_path = Path(config_path)
        self._source = self._load_source()

    def _load_source(self) -> NewsSource:
        if not self._config_path.exists():
            raise DisclosureFetchError(
                code="-",
                source=self.source_name,
                message=f"News config not found: {self._config_path}",
            )
        config = load_config(self._config_path)
        sources = config.sources.get("disclosure", [])
        if not sources:
            raise DisclosureFetchError(
                code="-",
                source=self.source_name,
                message="No 'disclosure' sources defined in news config",
            )
        return sources[0]

    @property
    def base_url(self) -> str:
        return self._source.url

    @property
    def filter_keywords(self) -> Optional[FilterKeywords]:
        return self._source.filter_keywords

    def _build_url(self, code: str) -> str:
        sep = "&" if "?" in self.base_url else "?"
        return f"{self.base_url}{sep}qtext={code}"

    # ------------------------------------------------------------------ parse

    def _parse(self, code: str, html: str) -> list[NewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[NewsItem] = []
        seen: set[str] = set()
        cutoff: Optional[datetime] = None
        if self._lookback_days is not None and self._lookback_days >= 0:
            cutoff = self._now_fn() - timedelta(days=self._lookback_days)

        for node in soup.select(".newsList__item"):
            title_el = node.select_one(".newsList__title")
            link_el = node.find("a", href=True)
            if not title_el or not link_el:
                continue
            title = title_el.get_text(strip=True)
            if not title:
                continue
            href = link_el["href"]
            url = urljoin(self.base_url, href)
            if url in seen:
                continue

            stocks_el = node.select_one(".newsList__stocks")
            if stocks_el and code not in stocks_el.get_text():
                # qtextで絞っているはずだが念のため銘柄コード一致を確認
                continue

            date_el = node.select_one(".newsList__date")
            published_at = self._parse_date(
                date_el.get_text(strip=True) if date_el else ""
            )

            if (
                cutoff is not None
                and published_at is not None
                and published_at < cutoff
            ):
                continue

            if not self._title_passes_filter(title):
                continue

            seen.add(url)
            items.append(
                NewsItem(
                    code=code,
                    title=title,
                    url=url,
                    source=self.source_name,
                    category=self.category,
                    published_at=published_at,
                    summary=None,
                    importance=self._estimate_importance(title),
                )
            )
        return items

    def _parse_date(self, raw: str) -> Optional[datetime]:
        """四季報の日付表記を datetime に変換。

        対応フォーマット:
            - `MM/DD HH:MM`        … 当年の datetime に変換 (未来日なら前年扱い)
            - `YYYY/MM/DD HH:MM`   … そのまま datetime に変換
        """
        if not raw:
            return None
        for fmt in ("%Y/%m/%d %H:%M", "%m/%d %H:%M"):
            try:
                dt = datetime.strptime(raw, fmt)
                if fmt == "%m/%d %H:%M":
                    now = self._now_fn()
                    dt = dt.replace(year=now.year)
                    if dt > now:
                        dt = dt.replace(year=now.year - 1)
                return dt
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

    # ------------------------------------------------------------------ fetch

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
                # 主コンテナの出現を待つ。出ない場合は0件として扱う。
                try:
                    page.wait_for_selector(".newsList__item", timeout=5000)
                except Exception:
                    pass
                return page.content()
            except Exception as e:  # noqa: BLE001
                last_exc = e
                if attempt < len(self._retry_delays):
                    delay = self._retry_delays[attempt]
                    logger.warning(
                        "CDP disclosure fetch retry %d/%d for code=%s after %ss: %s",
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
            message=f"Failed to fetch disclosure for {code}: {last_exc}",
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
            # テスト用: ファクトリから page を取得（リソース管理は呼び出し側）
            page = self._page_factory()
            try:
                for code in codes:
                    self._fetch_one_into(page, code, results, errors)
            finally:
                close = getattr(page, "close", None)
                if callable(close):
                    close()
            return results, errors

        pw, context = self._open_context()
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
            logger.warning("CdpDisclosureFetcher failed for %s: %s", code, e)
            errors[code] = e
