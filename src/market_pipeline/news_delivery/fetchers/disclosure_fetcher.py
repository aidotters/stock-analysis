"""四季報適時開示ページの取得。

URL: https://shikiho.toyokeizai.net/news?id=disclosure&qtext={code}
ライブラリ: requests + BeautifulSoup4
リトライ: 1秒/2秒/4秒の指数バックオフで最大3回
並列度: ThreadPoolExecutor(max_workers=5) (config で上書き可)
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from market_pipeline.news.config_parser import (
    FilterKeywords,
    NewsSource,
    load_config,
)
from market_pipeline.news_delivery.exceptions import DisclosureFetchError
from market_pipeline.news_delivery.fetchers.base import BaseFetcher
from market_pipeline.news_delivery.models import NewsItem

logger = logging.getLogger(__name__)


DEFAULT_USER_AGENT = (
    "Stock-Analysis/1.0 (+https://github.com/takuyaakashi/stock-analysis)"
)
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_WORKERS = 5
DEFAULT_RETRY_DELAYS = (1.0, 2.0, 4.0)
DEFAULT_CONFIG_PATH = "config/news_sources.yaml"


class DisclosureFetcher(BaseFetcher):
    """四季報適時開示ページを巡回して NewsItem を返す。"""

    source_name = "shikiho_disclosure"
    category = "disclosure"

    def __init__(
        self,
        *,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        max_workers: int = DEFAULT_MAX_WORKERS,
        timeout: float = DEFAULT_TIMEOUT,
        user_agent: str = DEFAULT_USER_AGENT,
        retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
        session: Optional[requests.Session] = None,
        sleep_fn=time.sleep,  # type: ignore[no-untyped-def]
    ) -> None:
        self._max_workers = max_workers
        self._timeout = timeout
        self._user_agent = user_agent
        self._retry_delays = retry_delays
        self._sleep = sleep_fn
        self._session = session or requests.Session()
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

    def _fetch_html(self, code: str) -> str:
        url = self._build_url(code)
        last_exc: Optional[Exception] = None
        last_status: Optional[int] = None

        attempts = len(self._retry_delays) + 1
        for attempt in range(attempts):
            try:
                resp = self._session.get(
                    url,
                    timeout=self._timeout,
                    headers={"User-Agent": self._user_agent},
                )
                if 500 <= resp.status_code < 600:
                    last_status = resp.status_code
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp.text
            except (requests.RequestException, requests.HTTPError) as e:
                last_exc = e
                resp_obj = getattr(e, "response", None)
                if resp_obj is not None:
                    last_status = resp_obj.status_code
                if attempt < len(self._retry_delays):
                    delay = self._retry_delays[attempt]
                    logger.warning(
                        "Disclosure fetch retry %d/%d for code=%s after %ss: %s",
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
            status=last_status,
            message=f"Failed to fetch disclosure for {code}: {last_exc}",
        )

    def _parse(self, code: str, html: str) -> list[NewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[NewsItem] = []

        # 四季報の適時開示ページは構造が変わりやすいため、複数のセレクタで
        # 寛容にパースする。優先度順:
        #   1. <ul class="news-list"> 内の <li><a href> + 日付
        #   2. <article> または <li> 内の <a> リンク全体
        candidates = soup.select(
            "ul.news-list li, ul.disclosure-list li, "
            "div.news-list li, article, li.news-item, li"
        )

        for node in candidates:
            link = node.find("a", href=True)
            if not link:
                continue
            title = link.get_text(strip=True)
            if not title:
                continue
            href = link["href"]
            url = urljoin(self.base_url, href)

            published_at = _extract_date(node)

            if not self._title_passes_filter(title):
                continue

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

        # 同一URLの重複を排除（同じ a が複数 li に紐づくケース）
        seen: set[str] = set()
        unique: list[NewsItem] = []
        for it in items:
            if it.url in seen:
                continue
            seen.add(it.url)
            unique.append(it)
        return unique

    def _title_passes_filter(self, title: str) -> bool:
        fk = self.filter_keywords
        if fk is None:
            return True
        for kw in fk.exclude:
            if kw and kw in title:
                return False
        return True

    def _estimate_importance(self, title: str) -> str:
        fk = self.filter_keywords
        if fk is None:
            return "mid"
        for kw in fk.include:
            if kw and kw in title:
                return "high"
        return "mid"

    def fetch_one(self, code: str) -> list[NewsItem]:
        html = self._fetch_html(code)
        return self._parse(code, html)

    def fetch_for_codes(self, codes: list[str]) -> list[NewsItem]:
        """単一銘柄の例外で全体を中断しない安全な並列取得。"""
        items, _ = self.fetch_for_codes_with_errors(codes)
        return items

    def fetch_for_codes_with_errors(
        self, codes: list[str]
    ) -> tuple[list[NewsItem], dict[str, Exception]]:
        """並列取得し、(items, {code: exception}) を返す。"""
        if not codes:
            return [], {}

        results: list[NewsItem] = []
        errors: dict[str, Exception] = {}

        def _run(code: str) -> tuple[str, list[NewsItem], Optional[Exception]]:
            try:
                return code, self.fetch_one(code), None
            except Exception as e:  # noqa: BLE001
                logger.warning("DisclosureFetcher failed for %s: %s", code, e)
                return code, [], e

        if self._max_workers <= 1 or len(codes) == 1:
            for code in codes:
                _c, items, err = _run(code)
                results.extend(items)
                if err is not None:
                    errors[_c] = err
            return results, errors

        with ThreadPoolExecutor(max_workers=self._max_workers) as ex:
            futures = [ex.submit(_run, code) for code in codes]
            for fut in as_completed(futures):
                code, items, err = fut.result()
                results.extend(items)
                if err is not None:
                    errors[code] = err
        return results, errors


_DATE_FORMATS = ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d")


def _extract_date(node) -> Optional[datetime]:  # type: ignore[no-untyped-def]
    """ノード内のテキストから日付を緩く抽出する。"""
    import re as _re

    text = node.get_text(" ", strip=True)
    match = _re.search(r"(\d{4})[/\-.](\d{1,2})[/\-.](\d{1,2})", text)
    if match:
        try:
            return datetime(
                int(match.group(1)), int(match.group(2)), int(match.group(3))
            )
        except ValueError:
            return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text[:10], fmt)
        except ValueError:
            continue
    return None
