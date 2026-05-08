"""Fetcher抽象基底クラス。"""

from __future__ import annotations

from abc import ABC, abstractmethod

from market_pipeline.news_delivery.models import NewsItem


class BaseFetcher(ABC):
    """銘柄コードリストから NewsItem を取得する抽象基底。"""

    source_name: str = "base"
    category: str = "news"

    @abstractmethod
    def fetch_for_codes(self, codes: list[str]) -> list[NewsItem]:
        """銘柄コードのリストを受け取り、NewsItem の平坦リストを返す。"""
        raise NotImplementedError
