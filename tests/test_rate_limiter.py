"""RateLimiter / RateLimitError のテスト。"""

from __future__ import annotations

import pytest

from market_pipeline.news_delivery.rate_limiter import RateLimiter, RateLimitError


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self._t = start
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self._t

    def sleep(self, secs: float) -> None:
        self.sleeps.append(secs)
        self._t += secs

    def advance(self, secs: float) -> None:
        self._t += secs


def _make(rpm: int, clock: FakeClock | None = None) -> RateLimiter:
    clock = clock or FakeClock()
    return RateLimiter(rpm, time_fn=clock.time, sleep_fn=clock.sleep)


def test_invalid_rpm():
    with pytest.raises(ValueError):
        RateLimiter(0)


def test_acquire_within_limit():
    rl = _make(3)
    assert rl.acquire() is True
    assert rl.acquire() is True
    assert rl.acquire() is True
    assert rl.acquire() is False


def test_eviction_after_window():
    clock = FakeClock()
    rl = _make(2, clock)
    assert rl.acquire() is True
    assert rl.acquire() is True
    assert rl.acquire() is False
    clock.advance(60.5)
    # 古いトークンがウィンドウ外に出るので再取得可能
    assert rl.acquire() is True


def test_wait_blocks_until_token_available():
    clock = FakeClock()
    rl = _make(1, clock)
    rl.wait()
    rl.wait()  # 2回目はsleepする
    assert clock.sleeps, "wait()でsleepされたはず"
    assert clock.sleeps[0] > 0


def test_current_count_reports_active_tokens():
    clock = FakeClock()
    rl = _make(5, clock)
    rl.acquire()
    rl.acquire()
    assert rl.current_count == 2
    clock.advance(60.5)
    assert rl.current_count == 0


def test_rate_limit_error_carries_skipped_count():
    err = RateLimitError("hit", skipped_count=12)
    assert err.skipped_count == 12
    assert "hit" in str(err)
