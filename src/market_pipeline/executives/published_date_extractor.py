"""URLからHTML を取得して発信日（YYYY-MM-DD）を抽出するモジュール.

抽出優先順位（上位ほど信頼性高）:
1. JSON-LD (`<script type="application/ld+json">` の datePublished / dateCreated)
2. OGP/Article (`<meta property="article:published_time">`)
3. Dublin Core (`<meta name="DC.date.issued">` / `<meta name="date">`)
4. `<time datetime="...">` タグ（本文先頭の最初の要素）
5. URLパスの日付 (`/YYYY/MM/DD/` パターン)

失敗時は `None` を返す（例外は内部で吸収し、バッチを止めない）。
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/119.0.0.0 Safari/537.36"
)
_URL_DATE_PATTERN = re.compile(r"/(\d{4})[/-](\d{1,2})[/-](\d{1,2})(?:/|$|\.|-)")

# ホスト単位の連続アクセス間隔（秒）。礼儀的クロールのためのレート制御。
_DEFAULT_MIN_INTERVAL_SEC = 1.0
_host_last_fetch: dict[str, float] = {}
_host_lock = threading.Lock()


def _throttle_for_host(url: str, min_interval: float) -> None:
    """同一ホストへの連続アクセスを `min_interval` 秒以上空ける."""
    if min_interval <= 0:
        return
    host = urlparse(url).netloc
    if not host:
        return
    with _host_lock:
        last = _host_last_fetch.get(host, 0.0)
        now = time.monotonic()
        wait = min_interval - (now - last)
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _host_last_fetch[host] = now


def _normalize_iso_date(value: str) -> Optional[str]:
    """ISO 8601 / 日本語形式 / YYYYMMDD を YYYY-MM-DD に正規化する."""
    if not value:
        return None
    value = value.strip()
    # ISO 8601: 2024-05-12T08:00:00+09:00 等
    iso_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
    if iso_match:
        return f"{iso_match.group(1)}-{iso_match.group(2)}-{iso_match.group(3)}"
    # 2024/5/12
    slash_match = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", value)
    if slash_match:
        try:
            y, m, d = map(int, slash_match.groups())
            return f"{y:04d}-{m:02d}-{d:02d}"
        except ValueError:
            return None
    # 2024年5月12日
    jp_match = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", value)
    if jp_match:
        y, m, d = map(int, jp_match.groups())
        return f"{y:04d}-{m:02d}-{d:02d}"
    # YYYYMMDD
    if re.match(r"^\d{8}$", value):
        try:
            return datetime.strptime(value, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def _extract_from_jsonld(soup: BeautifulSoup) -> Optional[str]:
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = (
            [data] if isinstance(data, dict) else data if isinstance(data, list) else []
        )
        for item in candidates:
            if not isinstance(item, dict):
                continue
            for key in ("datePublished", "dateCreated", "uploadDate"):
                value = item.get(key)
                if isinstance(value, str):
                    parsed = _normalize_iso_date(value)
                    if parsed:
                        return parsed
            graph = item.get("@graph")
            if isinstance(graph, list):
                for node in graph:
                    if not isinstance(node, dict):
                        continue
                    for key in ("datePublished", "dateCreated", "uploadDate"):
                        value = node.get(key)
                        if isinstance(value, str):
                            parsed = _normalize_iso_date(value)
                            if parsed:
                                return parsed
    return None


def _extract_from_meta(soup: BeautifulSoup) -> Optional[str]:
    meta_candidates = [
        {"property": "article:published_time"},
        {"property": "og:article:published_time"},
        {"name": "pubdate"},
        {"name": "publishdate"},
        {"name": "DC.date.issued"},
        {"name": "DC.date"},
        {"name": "date"},
        {"itemprop": "datePublished"},
    ]
    for attrs in meta_candidates:
        tag = soup.find("meta", attrs=attrs)
        if tag:
            content = tag.get("content") or tag.get("value")
            if isinstance(content, str):
                parsed = _normalize_iso_date(content)
                if parsed:
                    return parsed
    return None


def _extract_from_time_tag(soup: BeautifulSoup) -> Optional[str]:
    for time_tag in soup.find_all("time"):
        dt = time_tag.get("datetime") or time_tag.get_text()
        if isinstance(dt, str):
            parsed = _normalize_iso_date(dt)
            if parsed:
                return parsed
    return None


def _extract_from_url_path(url: str) -> Optional[str]:
    match = _URL_DATE_PATTERN.search(url)
    if not match:
        return None
    try:
        y = int(match.group(1))
        m = int(match.group(2))
        d = int(match.group(3))
        if 1990 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{m:02d}-{d:02d}"
    except ValueError:
        pass
    return None


def extract_published_date(
    url: str,
    *,
    timeout: float = 5.0,
    session: Optional[requests.Session] = None,
    min_interval: float = _DEFAULT_MIN_INTERVAL_SEC,
) -> Optional[str]:
    """URL から発信日を YYYY-MM-DD 形式で抽出する.

    優先順位: JSON-LD → OGP/meta → <time> タグ → URLパスの日付。

    礼儀的クロール: 同一ホストへの連続リクエストは `min_interval` 秒間隔を
    空ける（モジュール内でプロセス共有の last-fetch 時刻を管理）。

    Args:
        url: 対象ページの URL
        timeout: HTTP 取得のタイムアウト秒
        session: 既存 `requests.Session` を使いたい場合（Keep-Alive 活用）
        min_interval: 同一ホストへのアクセス最小間隔（秒）。0 以下で無効化

    Returns:
        "YYYY-MM-DD" 形式の日付文字列。失敗時は None
    """
    if not url:
        return None

    # 1) URL 先頭のパスから抽出（HTTP 取得前の最短経路）
    url_hint = _extract_from_url_path(url)

    _throttle_for_host(url, min_interval)

    try:
        client = session or requests.Session()
        resp = client.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "ja,en;q=0.9",
            },
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return url_hint
        # 巨大ページは先頭 512KB のみ解析
        html = resp.text[:512_000]
    except requests.RequestException as exc:
        logger.debug("URL取得失敗 url=%s error=%s", url, exc)
        return url_hint

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:  # noqa: BLE001
        logger.debug("HTMLパース失敗 url=%s error=%s", url, exc)
        return url_hint

    for extractor in (_extract_from_jsonld, _extract_from_meta, _extract_from_time_tag):
        result = extractor(soup)
        if result:
            return result

    return url_hint
