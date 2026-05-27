"""J-Quants API V2 関連の例外定義。"""

from __future__ import annotations


class JQuantsError(Exception):
    """J-Quants 関連の基底例外。"""


class JQuantsAuthError(JQuantsError):
    """401/403 認証エラー。リトライせず即時失敗。"""


class JQuantsRateLimitError(JQuantsError):
    """429 レート制限エラー。指数バックオフリトライ対象。"""


class JQuantsServerError(JQuantsError):
    """5xx サーバーエラー。指数バックオフリトライ対象。"""


class JQuantsResponseError(JQuantsError):
    """レスポンスの `data` キー欠落・JSON デコード失敗など形式異常。"""
