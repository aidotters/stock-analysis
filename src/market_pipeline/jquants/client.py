"""J-Quants API V2 共通 HTTP クライアント。

x-api-key 認証、トークンバケット式レート制限、pagination_key の自動追跡、
指数バックオフリトライ(429/5xx)、同期/非同期両系統対応。

設計方針:
- アダプタ層パターン。本モジュールは HTTP 層のみを担当し、
  V2→V1 のレスポンス整形は `_v2_translator` で行う。
- `api_key` 未指定時は `__init__` 時点では遅延 raise せず、`get()` / `get_async()` 等
  実呼び出し時に `JQuantsAuthError` を投げる。テストでの DI を容易にする。
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any, Optional

import aiohttp
import requests

from market_pipeline.jquants.exceptions import (
    JQuantsAuthError,
    JQuantsError,
    JQuantsRateLimitError,
    JQuantsResponseError,
    JQuantsServerError,
)

DEFAULT_API_BASE = "https://api.jquants.com"


class _TokenBucket:
    """同期/非同期どちらからも安全に利用できるトークンバケット。

    `_take()` が状態更新と必要待機時間の算出を `threading.Lock` 内で行い、
    待機自体は `acquire()` / `acquire_async()` がそれぞれ同期/非同期で行う。
    """

    def __init__(
        self,
        capacity: float,
        refill_per_second: float,
        initial_tokens: float | None = None,
    ):
        if capacity <= 0 or refill_per_second <= 0:
            raise ValueError(
                "capacity と refill_per_second は正の値である必要があります"
            )
        if initial_tokens is not None and initial_tokens < 0:
            raise ValueError("initial_tokens は非負である必要があります")
        self.capacity = float(capacity)
        self.refill_per_second = float(refill_per_second)
        self.tokens = float(initial_tokens if initial_tokens is not None else capacity)
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _take(self) -> float:
        """1 トークンの取得を試みる。0 を返したら取得成功、>0 なら必要待機時間(秒)。"""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(
                self.capacity, self.tokens + elapsed * self.refill_per_second
            )
            self.last_refill = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0
            needed = 1.0 - self.tokens
            return needed / self.refill_per_second

    def acquire(self) -> None:
        while True:
            wait = self._take()
            if wait <= 0:
                return
            time.sleep(wait)

    async def acquire_async(self) -> None:
        while True:
            wait = self._take()
            if wait <= 0:
                return
            await asyncio.sleep(wait)


class JQuantsClient:
    """J-Quants API V2 HTTP クライアント。

    同期(`requests`)と非同期(`aiohttp`)の両系統を提供し、レート制限・
    ページネーション・リトライ・例外マッピングを集約する。
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = DEFAULT_API_BASE,
        rate_limit_per_minute: int = 55,
        max_retries: int = 3,
        retry_initial_seconds: float = 1.0,
        retry_max_seconds: float = 8.0,
        timeout_seconds: int = 30,
    ):
        """`api_key` 未指定時は環境変数 `JQUANTS_API_KEY` を参照する。
        どちらも未設定の場合は `__init__` では raise せず、
        実 API 呼び出し時に `JQuantsAuthError` を発生させる。
        """
        self._api_key = (
            api_key if api_key is not None else os.getenv("JQUANTS_API_KEY", "")
        )
        self.base_url = base_url.rstrip("/")
        self.rate_limit_per_minute = rate_limit_per_minute
        self.max_retries = max_retries
        self.retry_initial_seconds = retry_initial_seconds
        self.retry_max_seconds = retry_max_seconds
        self.timeout_seconds = timeout_seconds
        # capacity=1 + initial_tokens=0 でバーストと「起動時 free token」を抑止する。
        # J-Quants V2 は過去 60 秒の sliding window で req 数を判定するため、
        # - capacity=rate(=60)だと 60 バースト + 60 補充で 120/min に達して 429
        # - capacity=1 + initial_tokens=1 でも 1(初期) + 60(補充) = 60 秒以内 61 件で 429
        # initial_tokens=0 にすることで起動から refill レートを厳密に守れる
        # (副作用: 初回リクエストは 1/refill_rate 秒待ち。デフォルト rate=55 なら ~1.1 秒)。
        self._bucket = _TokenBucket(
            capacity=1,
            refill_per_second=rate_limit_per_minute / 60.0,
            initial_tokens=0,
        )
        self.logger = logging.getLogger(__name__)

    @property
    def headers(self) -> dict[str, str]:
        if not self._api_key:
            raise JQuantsAuthError(
                "JQUANTS_API_KEY が設定されていません。.env もしくは引数で指定してください。"
            )
        return {"x-api-key": self._api_key}

    def _build_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    @staticmethod
    def _is_retryable_status(status: int) -> bool:
        return status == 429 or 500 <= status < 600

    def _backoff_seconds(self, attempt: int) -> float:
        """attempt は 0 始まり。1s, 2s, 4s, ... 上限 retry_max_seconds。"""
        return min(self.retry_initial_seconds * (2**attempt), self.retry_max_seconds)

    @staticmethod
    def _parse_json(text: str) -> dict[str, Any]:
        import json

        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise JQuantsResponseError(f"JSON デコード失敗: {exc}") from exc
        if not isinstance(data, dict):
            raise JQuantsResponseError(
                f"レスポンスが dict ではありません: type={type(data).__name__}"
            )
        return data

    def _raise_for_status(self, status: int, body_text: str) -> None:
        if status in (401, 403):
            raise JQuantsAuthError(f"認証エラー(status={status}): {body_text[:200]}")
        if status == 429:
            raise JQuantsRateLimitError(f"レート制限(status=429): {body_text[:200]}")
        if 500 <= status < 600:
            raise JQuantsServerError(
                f"サーバーエラー(status={status}): {body_text[:200]}"
            )
        if status >= 400:
            raise JQuantsError(f"HTTP エラー(status={status}): {body_text[:200]}")

    # ------------------------------------------------------------------ sync
    def get(self, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """同期 GET。1 レスポンス(1 ページ)を dict で返す。

        429/5xx は指数バックオフで最大 `max_retries` 回までリトライする。
        401/403 は即時 `JQuantsAuthError`。
        """
        url = self._build_url(path)
        params = params or {}
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            self._bucket.acquire()
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    wait = self._backoff_seconds(attempt)
                    self.logger.warning(
                        "ネットワークエラー(%s): %s/%s 回目、%.1f 秒待機して再試行",
                        exc.__class__.__name__,
                        attempt + 1,
                        self.max_retries + 1,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                raise JQuantsServerError(
                    f"ネットワークエラーで {self.max_retries + 1} 回失敗: {exc}"
                ) from exc

            if response.status_code == 200:
                return self._parse_json(response.text)

            if self._is_retryable_status(response.status_code):
                if attempt < self.max_retries:
                    wait = self._backoff_seconds(attempt)
                    self.logger.warning(
                        "HTTP %s: %s/%s 回目、%.1f 秒待機して再試行",
                        response.status_code,
                        attempt + 1,
                        self.max_retries + 1,
                        wait,
                    )
                    time.sleep(wait)
                    continue

            # リトライ不可 or リトライ上限
            self._raise_for_status(response.status_code, response.text)
            # _raise_for_status が必ず raise するので到達しないが mypy 対策
            raise JQuantsError(f"想定外: status={response.status_code}")

        # ループ脱出ケース(基本到達しない)
        if last_exc is not None:
            raise JQuantsServerError(str(last_exc)) from last_exc
        raise JQuantsError("リトライ上限に達しました")

    def paginate(
        self, path: str, params: Optional[dict[str, Any]] = None
    ) -> Iterator[list[dict[str, Any]]]:
        """`pagination_key` を自動追跡し、各ページの `data` 配列を順次 yield。"""
        next_params = dict(params or {})
        while True:
            payload = self.get(path, params=dict(next_params))
            if "data" not in payload:
                raise JQuantsResponseError(
                    f"レスポンスに 'data' キーがありません: keys={list(payload.keys())}"
                )
            data = payload["data"]
            if not isinstance(data, list):
                raise JQuantsResponseError(
                    f"'data' がリスト型ではありません: type={type(data).__name__}"
                )
            yield data
            next_key = payload.get("pagination_key")
            if not next_key:
                return
            next_params["pagination_key"] = next_key

    def health_check(self) -> bool:
        """`/v2/equities/master` を 1 件取得して認証/疎通を確認する。

        Returns:
            True if successful. Raises on failure.
        """
        payload = self.get("/v2/equities/master")
        if "data" not in payload:
            raise JQuantsResponseError(
                "health_check: レスポンスに 'data' キーがありません"
            )
        return True

    # ----------------------------------------------------------------- async
    async def get_async(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        url = self._build_url(path)
        params = params or {}
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            await self._bucket.acquire_async()
            try:
                async with session.get(
                    url, params=params, headers=self.headers
                ) as response:
                    status = response.status
                    text = await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    wait = self._backoff_seconds(attempt)
                    self.logger.warning(
                        "ネットワークエラー(%s): %s/%s 回目、%.1f 秒待機して再試行",
                        exc.__class__.__name__,
                        attempt + 1,
                        self.max_retries + 1,
                        wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                raise JQuantsServerError(
                    f"ネットワークエラーで {self.max_retries + 1} 回失敗: {exc}"
                ) from exc

            if status == 200:
                return self._parse_json(text)

            if self._is_retryable_status(status):
                if attempt < self.max_retries:
                    wait = self._backoff_seconds(attempt)
                    self.logger.warning(
                        "HTTP %s: %s/%s 回目、%.1f 秒待機して再試行",
                        status,
                        attempt + 1,
                        self.max_retries + 1,
                        wait,
                    )
                    await asyncio.sleep(wait)
                    continue

            self._raise_for_status(status, text)
            raise JQuantsError(f"想定外: status={status}")

        if last_exc is not None:
            raise JQuantsServerError(str(last_exc)) from last_exc
        raise JQuantsError("リトライ上限に達しました")

    async def paginate_async(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        next_params = dict(params or {})
        while True:
            payload = await self.get_async(session, path, params=dict(next_params))
            if "data" not in payload:
                raise JQuantsResponseError(
                    f"レスポンスに 'data' キーがありません: keys={list(payload.keys())}"
                )
            data = payload["data"]
            if not isinstance(data, list):
                raise JQuantsResponseError(
                    f"'data' がリスト型ではありません: type={type(data).__name__}"
                )
            yield data
            next_key = payload.get("pagination_key")
            if not next_key:
                return
            next_params["pagination_key"] = next_key
