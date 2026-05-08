"""トークンバケット式レート制限。

Phase 2 で追加された WebSearch / RSS Fetcher の流量制御に使用する。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable, Deque


class RateLimitError(Exception):
    """レート制限到達を示す例外。

    Fetcher 内で `RateLimiter.acquire()` が False を返した時に raise する。
    `DeliveryService` 側でキャッチして priority=high の銘柄のみ再試行する。
    """

    def __init__(self, message: str, *, skipped_count: int = 0) -> None:
        super().__init__(message)
        self.skipped_count = skipped_count


class RateLimiter:
    """1分あたり N 回までの呼び出しを許可するトークンバケット。

    - スレッドセーフ
    - `acquire()`: トークンが取れなければ即 False を返す（非ブロッキング）
    - `wait()`: 取れるまでブロックして返す
    """

    def __init__(
        self,
        requests_per_minute: int,
        *,
        time_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be > 0")
        self._limit = requests_per_minute
        self._window_seconds = 60.0
        self._time = time_fn
        self._sleep = sleep_fn
        self._timestamps: Deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        """非ブロッキングでトークンを取得。失敗時は False。"""
        with self._lock:
            now = self._time()
            self._evict_expired(now)
            if len(self._timestamps) >= self._limit:
                return False
            self._timestamps.append(now)
            return True

    def wait(self) -> None:
        """取れるまでブロックして待機。"""
        while True:
            with self._lock:
                now = self._time()
                self._evict_expired(now)
                if len(self._timestamps) < self._limit:
                    self._timestamps.append(now)
                    return
                wait_seconds = self._timestamps[0] + self._window_seconds - now
            if wait_seconds > 0:
                self._sleep(wait_seconds)

    def _evict_expired(self, now: float) -> None:
        threshold = now - self._window_seconds
        while self._timestamps and self._timestamps[0] <= threshold:
            self._timestamps.popleft()

    @property
    def current_count(self) -> int:
        """現在のウィンドウ内で消費されているトークン数。"""
        with self._lock:
            self._evict_expired(self._time())
            return len(self._timestamps)
