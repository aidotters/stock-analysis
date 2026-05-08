"""ウォッチリストベースのニュース配信パッケージ。

主要コンポーネント:
- WatchList / WatchListEntry: data/watchlists/*.json のCRUD
- NewsItem: 全Fetcher共通のデータクラス
- DisclosureFetcher: 四季報適時開示の取得 (Phase 1)
- Deduplicator: data/news_delivery.db による重複排除
- SlackFormatter: Block Kit メッセージ生成
- DeliveryService: 配信パイプラインのオーケストレーター
"""

from market_pipeline.news_delivery.exceptions import (
    DeduplicatorError,
    DisclosureFetchError,
    FetcherError,
    NewsDeliveryError,
    WatchListError,
    WatchListSchemaError,
)
from market_pipeline.news_delivery.models import NewsItem, WatchListEntry

__all__ = [
    "NewsItem",
    "WatchListEntry",
    "NewsDeliveryError",
    "WatchListError",
    "WatchListSchemaError",
    "FetcherError",
    "DisclosureFetchError",
    "DeduplicatorError",
]
