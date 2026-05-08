"""配信パイプラインのオーケストレーター。"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Callable, Iterable, Optional

import requests

from market_pipeline.config import get_settings
from market_pipeline.news_delivery.deduplicator import Deduplicator
from market_pipeline.news_delivery.fetchers.base import BaseFetcher
from market_pipeline.news_delivery.fetchers.cdp_disclosure_fetcher import (
    CdpDisclosureFetcher,
)
from market_pipeline.news_delivery.fetchers.disclosure_fetcher import (
    DisclosureFetcher,
)
from market_pipeline.news_delivery.fetchers.google_news_rss_fetcher import (
    GoogleNewsRssFetcher,
)
from market_pipeline.news_delivery.fetchers.shikiho_stock_news_fetcher import (
    ShikihoStockNewsFetcher,
)
from market_pipeline.news_delivery.fetchers.tdnet_rss_fetcher import (
    TdnetRssFetcher,
)
from market_pipeline.news_delivery.formatter import (
    DeliveryStats,
    SlackFormatter,
    StockMetaInfo,
)
from market_pipeline.news_delivery.models import NewsItem
from market_pipeline.news_delivery.rate_limiter import RateLimitError
from market_pipeline.news_delivery.watchlist import StockMeta, WatchList

logger = logging.getLogger(__name__)


class DeliveryService:
    """ウォッチリスト→Fetcher→重複排除→フォーマット→Slack送信を統括する。"""

    def __init__(
        self,
        watchlist: WatchList,
        fetchers: list[BaseFetcher],
        deduplicator: Deduplicator,
        formatter: SlackFormatter,
        slack_post: Optional[Callable[[str, list[dict]], None]] = None,
        webhook_url: Optional[str] = None,
        warning_handler: Optional[Callable[[str], None]] = None,
        metric_handler: Optional[Callable[[str, str], None]] = None,
        quiet_when_empty: Optional[bool] = None,
    ) -> None:
        self._watchlist = watchlist
        self._fetchers = fetchers
        self._deduplicator = deduplicator
        self._formatter = formatter
        self._slack_post = slack_post or _default_slack_post
        self._webhook_url = webhook_url or _resolve_webhook_url()
        self._warn = warning_handler or (lambda _msg: None)
        self._metric = metric_handler or (lambda _k, _v: None)
        if quiet_when_empty is None:
            quiet_when_empty = (
                os.environ.get("STOCK_NEWS_QUIET_WHEN_EMPTY", "false").lower() == "true"
            )
        self._quiet_when_empty = quiet_when_empty

    def run(self, slot: str, *, dry_run: bool = False) -> dict[str, int]:
        """指定スロットで配信を実行し、メトリクスを dict で返す。"""
        self._deduplicator.initialize()
        entries = self._watchlist.list()
        codes = [e.code for e in entries]

        if not codes:
            logger.info("Watchlist is empty; nothing to deliver")
            self._metric("配信銘柄", "0")
            self._metric("新規ニュース", "0")
            self._metric("スキップ(重複)", "0")
            return {"code_count": 0, "new_items": 0, "skipped_items": 0, "messages": 0}

        all_items: list[NewsItem] = []
        per_fetcher_failures = 0
        high_priority_codes = [
            e.code for e in entries if getattr(e, "priority", None) == "high"
        ]
        for fetcher in self._fetchers:
            try:
                items, errors = _run_fetcher(fetcher, codes)
            except RateLimitError as e:
                self._warn(
                    f"{fetcher.source_name}: レート制限到達 (skipped={e.skipped_count}); "
                    "priority=high の銘柄のみで再試行"
                )
                self._metric(
                    f"レート制限到達({fetcher.source_name})", str(e.skipped_count)
                )
                if not high_priority_codes:
                    items, errors = [], {}
                else:
                    try:
                        items, errors = _run_fetcher(fetcher, high_priority_codes)
                    except RateLimitError as e2:
                        # 再試行でもレート制限 → 諦める
                        self._warn(
                            f"{fetcher.source_name}: 再試行もレート制限到達 "
                            f"(skipped={e2.skipped_count})"
                        )
                        items, errors = [], {}
            all_items.extend(items)
            for code, exc in errors.items():
                self._warn(f"{code}: {fetcher.source_name} 取得失敗 ({exc})")
            if errors and not items:
                per_fetcher_failures += 1

        if (
            self._fetchers
            and per_fetcher_failures == len(self._fetchers)
            and all(len(_) == 0 for _ in [all_items])
        ):
            raise RuntimeError("all fetchers failed")

        unseen = self._deduplicator.filter_unseen(all_items)
        skipped = len(all_items) - len(unseen)

        items_by_code: dict[str, list[NewsItem]] = {}
        for it in unseen:
            items_by_code.setdefault(it.code, []).append(it)

        stats = DeliveryStats(
            code_count=len(codes),
            new_items=len(unseen),
            skipped_items=skipped,
        )

        def _meta(code: str) -> StockMetaInfo:
            m: StockMeta = self._watchlist.resolve_meta(code)
            return StockMetaInfo(long_name=m.long_name, sector=m.sector)

        messages = self._formatter.build(
            slot=slot,
            watchlist=entries,
            items_by_code=items_by_code,
            stats=stats,
            meta_resolver=_meta,
            now=datetime.now(),
            quiet_when_empty=self._quiet_when_empty,
        )

        sent = 0
        if dry_run:
            logger.info("dry-run: would send %d Slack message(s)", len(messages))
        elif messages:
            for blocks in messages:
                self._send(blocks)
                sent += 1
        else:
            logger.info(
                "Slack送信スキップ (新規0件 + STOCK_NEWS_QUIET_WHEN_EMPTY=true)"
            )

        if not dry_run and unseen:
            self._deduplicator.mark_delivered(unseen, slot=slot)

        self._metric("配信銘柄", str(len(codes)))
        self._metric("新規ニュース", str(len(unseen)))
        self._metric("スキップ(重複)", str(skipped))
        self._metric("送信メッセージ", str(sent))

        return {
            "code_count": len(codes),
            "new_items": len(unseen),
            "skipped_items": skipped,
            "messages": sent,
        }

    def _send(self, blocks: list[dict]) -> None:
        if not self._webhook_url:
            logger.info("Slack通知スキップ (webhook未設定)")
            return
        try:
            self._slack_post(self._webhook_url, blocks)
        except requests.RequestException as e:
            logger.info("Slack通知失敗: %s", e)


def _run_fetcher(
    fetcher: BaseFetcher, codes: list[str]
) -> tuple[list[NewsItem], dict[str, Exception]]:
    """fetcher が `fetch_for_codes_with_errors` を持てばそれを使い、
    無ければ通常の `fetch_for_codes` を呼んで errors={} を返す。"""
    method = getattr(fetcher, "fetch_for_codes_with_errors", None)
    if callable(method):
        return method(codes)  # type: ignore[no-any-return]
    return fetcher.fetch_for_codes(codes), {}


def _resolve_webhook_url() -> Optional[str]:
    """STOCK_NEWS_SLACK_WEBHOOK_URL → SLACK_WEBHOOK_URL の順でフォールバック。"""
    url = os.environ.get("STOCK_NEWS_SLACK_WEBHOOK_URL", "").strip()
    if url:
        return url
    settings = get_settings()
    return settings.slack.webhook_url or None


def _default_slack_post(url: str, blocks: list[dict]) -> None:
    """Block Kit メッセージを Slack へ POST する。"""
    payload = {"blocks": blocks}
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()


DEFAULT_SOURCES = ("disclosure", "general_news", "ir_release", "stock_news")


def build_default_service(
    *,
    watchlist_name: str = "default",
    config_path: str = "config/news_sources.yaml",
    lookback_days: Optional[int] = None,
    sources: Iterable[str] = DEFAULT_SOURCES,
) -> DeliveryService:
    """既定構成で `DeliveryService` を生成。

    Phase 1 で追加された `CdpDisclosureFetcher` (四季報 SPA → Playwright) に加え、
    Phase 2 で `GoogleNewsRssFetcher` (一般ニュース) と `TdnetRssFetcher`
    (適時開示の補強) を統合する。`sources` 引数で取捨選択可能。

    `lookback_days` を明示しない場合は `settings.news_delivery.lookback_days`
    (既定 7 日) を使用する。
    """
    wl = WatchList.load(watchlist_name)
    if lookback_days is None:
        lookback_days = get_settings().news_delivery.lookback_days
    _ = DisclosureFetcher  # 保持 (公開APIとして残す)

    fetchers: list[BaseFetcher] = []
    sources_set = set(sources)
    if "disclosure" in sources_set:
        fetchers.append(
            CdpDisclosureFetcher(config_path=config_path, lookback_days=lookback_days)
        )
    if "general_news" in sources_set:
        fetchers.append(
            GoogleNewsRssFetcher(
                config_path=config_path,
                lookback_days=lookback_days,
                meta_resolver=lambda code: _resolve_meta_dict(wl, code),
            )
        )
    if "ir_release" in sources_set:
        fetchers.append(
            TdnetRssFetcher(config_path=config_path, lookback_days=lookback_days)
        )
    if "stock_news" in sources_set:
        fetchers.append(
            ShikihoStockNewsFetcher(
                config_path=config_path, lookback_days=lookback_days
            )
        )

    dedup = Deduplicator()
    formatter = SlackFormatter()
    return DeliveryService(
        watchlist=wl,
        fetchers=fetchers,
        deduplicator=dedup,
        formatter=formatter,
    )


def _resolve_meta_dict(wl: WatchList, code: str) -> dict:
    m: StockMeta = wl.resolve_meta(code)
    return {"long_name": m.long_name, "sector": m.sector}


def collect_news(fetchers: Iterable[BaseFetcher], codes: list[str]) -> list[NewsItem]:
    """ヘルパ: 複数Fetcherから順次収集。"""
    out: list[NewsItem] = []
    for f in fetchers:
        out.extend(f.fetch_for_codes(codes))
    return out
