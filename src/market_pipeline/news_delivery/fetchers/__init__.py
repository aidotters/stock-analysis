"""ニュース Fetcher 群。

- `DisclosureFetcher` (Phase 1 旧版, HTTP+BS4): 静的HTML想定。テスト/フィクスチャ用に保持
- `CdpDisclosureFetcher` (Phase 1 → CDP化前倒し): 四季報適時開示 (Playwright)
- `GoogleNewsRssFetcher` (Phase 2): Google News RSS で銘柄関連の一般ニュース取得
- `TdnetRssFetcher` (Phase 2): yanoshin TDnet ラッパー (Atom) で適時開示の一次情報を補強
- `ShikihoStockNewsFetcher` (Phase 3): 四季報銘柄ページの「この銘柄の関連記事」セクション
"""

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

__all__ = [
    "BaseFetcher",
    "CdpDisclosureFetcher",
    "DisclosureFetcher",
    "GoogleNewsRssFetcher",
    "ShikihoStockNewsFetcher",
    "TdnetRssFetcher",
]
