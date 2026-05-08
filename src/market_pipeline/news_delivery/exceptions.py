"""news_delivery パッケージのカスタム例外。"""

from __future__ import annotations

from typing import Optional


class NewsDeliveryError(Exception):
    """news_delivery パッケージの基底例外。"""


class WatchListError(NewsDeliveryError):
    """ウォッチリスト操作に関する例外。"""


class WatchListSchemaError(WatchListError):
    """ウォッチリストエントリのスキーマ違反。"""


class FetcherError(NewsDeliveryError):
    """Fetcher系の基底例外。code/source/status を保持する。"""

    def __init__(
        self,
        code: str,
        source: str,
        *,
        status: Optional[int] = None,
        message: Optional[str] = None,
    ) -> None:
        self.code = code
        self.source = source
        self.status = status
        msg = (
            message or f"Fetcher failed for code={code} source={source} status={status}"
        )
        super().__init__(msg)


class DisclosureFetchError(FetcherError):
    """四季報適時開示取得時の例外。"""


class DeduplicatorError(NewsDeliveryError):
    """重複排除DB操作に関する例外。"""
