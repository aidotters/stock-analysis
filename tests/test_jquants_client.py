"""JQuantsClient のユニットテスト。"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock, patch

import aiohttp
import pytest
import requests

from market_pipeline.jquants.client import JQuantsClient, _TokenBucket
from market_pipeline.jquants.exceptions import (
    JQuantsAuthError,
    JQuantsError,
    JQuantsResponseError,
    JQuantsServerError,
)

TEST_API_KEY = "test_api_key_123"


# ---------------------------------------------------------------------------
# _TokenBucket
# ---------------------------------------------------------------------------
class TestTokenBucket:
    def test_initial_tokens_default_full_capacity(self):
        """initial_tokens 未指定時は capacity と同じ(後方互換)。"""
        bucket = _TokenBucket(capacity=5, refill_per_second=1.0)
        wait = bucket._take()
        assert wait == 0.0

    def test_initial_tokens_zero_blocks_first_take(self):
        """initial_tokens=0 で起動直後の即時取得を抑止。"""
        bucket = _TokenBucket(capacity=1, refill_per_second=10.0, initial_tokens=0)
        wait = bucket._take()
        # 0 トークンから 1 トークン必要 → refill 10/sec で約 0.1 秒待ち
        assert 0.05 < wait <= 0.15

    def test_take_returns_wait_when_empty(self):
        bucket = _TokenBucket(capacity=1, refill_per_second=1.0)
        bucket._take()
        wait = bucket._take()
        # refill 1/sec なので 1 トークン分 = 1 秒近い待機が必要
        assert wait > 0.5
        assert wait <= 1.0

    def test_acquire_blocks_until_refill(self):
        bucket = _TokenBucket(capacity=1, refill_per_second=10.0)  # 100ms 補充
        bucket.acquire()  # 即取得
        start = time.monotonic()
        bucket.acquire()  # 1 トークン待つ
        elapsed = time.monotonic() - start
        assert 0.05 < elapsed < 0.3

    def test_invalid_capacity_raises(self):
        with pytest.raises(ValueError):
            _TokenBucket(capacity=0, refill_per_second=1.0)


# ---------------------------------------------------------------------------
# JQuantsClient init / api_key
# ---------------------------------------------------------------------------
class TestInit:
    def test_api_key_from_arg(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        assert client.headers == {"x-api-key": TEST_API_KEY}

    def test_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("JQUANTS_API_KEY", "env_key")
        client = JQuantsClient()
        assert client.headers == {"x-api-key": "env_key"}

    def test_missing_api_key_raises_on_use(self, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        client = JQuantsClient(api_key="")
        with pytest.raises(JQuantsAuthError):
            _ = client.headers


# ---------------------------------------------------------------------------
# get() / sync paginate
# ---------------------------------------------------------------------------
class TestGetSync:
    def _make_response(self, status: int, body: str = "{}"):
        resp = MagicMock()
        resp.status_code = status
        resp.text = body
        return resp

    def test_get_x_api_key_header(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        with patch("market_pipeline.jquants.client.requests.get") as mock_get:
            mock_get.return_value = self._make_response(200, '{"data": []}')
            client.get("/v2/equities/master")
            _, kwargs = mock_get.call_args
            assert kwargs["headers"] == {"x-api-key": TEST_API_KEY}

    def test_get_401_raises_auth_error_without_retry(self):
        client = JQuantsClient(api_key=TEST_API_KEY, max_retries=3)
        with patch("market_pipeline.jquants.client.requests.get") as mock_get:
            mock_get.return_value = self._make_response(401, '{"message":"unauth"}')
            with pytest.raises(JQuantsAuthError):
                client.get("/v2/equities/master")
            assert mock_get.call_count == 1

    def test_get_429_retries_with_backoff(self):
        client = JQuantsClient(
            api_key=TEST_API_KEY,
            max_retries=2,
            retry_initial_seconds=0.001,
            retry_max_seconds=0.01,
        )
        responses = [
            self._make_response(429, '{"message":"rate"}'),
            self._make_response(429, '{"message":"rate"}'),
            self._make_response(200, '{"data": [{"x":1}]}'),
        ]
        with patch(
            "market_pipeline.jquants.client.requests.get", side_effect=responses
        ) as mock_get:
            result = client.get("/v2/equities/master")
            assert result == {"data": [{"x": 1}]}
            assert mock_get.call_count == 3

    def test_get_5xx_retries(self):
        client = JQuantsClient(
            api_key=TEST_API_KEY,
            max_retries=1,
            retry_initial_seconds=0.001,
            retry_max_seconds=0.01,
        )
        responses = [
            self._make_response(503, "{}"),
            self._make_response(200, '{"data":[]}'),
        ]
        with patch(
            "market_pipeline.jquants.client.requests.get", side_effect=responses
        ):
            result = client.get("/v2/equities/master")
            assert result == {"data": []}

    def test_get_exhausts_retries(self):
        client = JQuantsClient(
            api_key=TEST_API_KEY,
            max_retries=1,
            retry_initial_seconds=0.001,
            retry_max_seconds=0.01,
        )
        with patch(
            "market_pipeline.jquants.client.requests.get",
            return_value=self._make_response(503, "{}"),
        ) as mock_get:
            with pytest.raises(JQuantsServerError):
                client.get("/v2/equities/master")
            assert mock_get.call_count == 2  # initial + 1 retry

    def test_get_invalid_json_raises_response_error(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        with patch(
            "market_pipeline.jquants.client.requests.get",
            return_value=self._make_response(200, "not json"),
        ):
            with pytest.raises(JQuantsResponseError):
                client.get("/v2/equities/master")

    def test_network_error_retries_then_raises(self):
        client = JQuantsClient(
            api_key=TEST_API_KEY,
            max_retries=1,
            retry_initial_seconds=0.001,
            retry_max_seconds=0.01,
        )
        with patch(
            "market_pipeline.jquants.client.requests.get",
            side_effect=requests.ConnectionError("boom"),
        ) as mock_get:
            with pytest.raises(JQuantsServerError):
                client.get("/v2/equities/master")
            assert mock_get.call_count == 2

    def test_400_raises_generic_error(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        with patch(
            "market_pipeline.jquants.client.requests.get",
            return_value=self._make_response(400, '{"message":"bad"}'),
        ):
            with pytest.raises(JQuantsError):
                client.get("/v2/equities/master")


class TestPaginate:
    def _resp(self, status, body):
        m = MagicMock()
        m.status_code = status
        m.text = body
        return m

    def test_paginate_follows_pagination_key(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        pages = [
            '{"data": [{"i":1}], "pagination_key": "k1"}',
            '{"data": [{"i":2}], "pagination_key": "k2"}',
            '{"data": [{"i":3}]}',
        ]
        responses = [self._resp(200, p) for p in pages]
        with patch(
            "market_pipeline.jquants.client.requests.get", side_effect=responses
        ) as mock_get:
            collected = list(client.paginate("/v2/equities/bars/daily"))
            assert collected == [[{"i": 1}], [{"i": 2}], [{"i": 3}]]
            assert mock_get.call_count == 3
            # 2 回目以降は pagination_key を params に追加して送る
            second_params = mock_get.call_args_list[1][1]["params"]
            assert second_params.get("pagination_key") == "k1"
            third_params = mock_get.call_args_list[2][1]["params"]
            assert third_params.get("pagination_key") == "k2"

    def test_paginate_missing_data_raises(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        with patch(
            "market_pipeline.jquants.client.requests.get",
            return_value=self._resp(200, '{"info": []}'),
        ):
            with pytest.raises(JQuantsResponseError):
                list(client.paginate("/v2/equities/master"))


# ---------------------------------------------------------------------------
# rate limiting
# ---------------------------------------------------------------------------
class TestRateLimit:
    def test_no_burst_then_steady_throttling(self):
        """capacity=1 によりバースト無し、refill レートに従って持続スロットリング。"""
        # 600 req/min = 10 req/sec → 100ms/req
        client = JQuantsClient(api_key=TEST_API_KEY, rate_limit_per_minute=600)
        m = MagicMock()
        m.status_code = 200
        m.text = '{"data": []}'
        with patch("market_pipeline.jquants.client.requests.get", return_value=m):
            start = time.monotonic()
            # 5 リクエスト: 初回即時 + 4 回 × 100ms ≈ 0.4 秒
            for _ in range(5):
                client.get("/v2/equities/master")
            elapsed = time.monotonic() - start
        # 4 回分の待機 = 0.4 秒以上、上限は十分緩く
        assert 0.3 < elapsed < 1.0


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------
class TestHealthCheck:
    def test_success(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        m = MagicMock()
        m.status_code = 200
        m.text = '{"data": [{"Code":"86970"}]}'
        with patch("market_pipeline.jquants.client.requests.get", return_value=m):
            assert client.health_check() is True

    def test_missing_data_raises(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        m = MagicMock()
        m.status_code = 200
        m.text = '{"info": []}'
        with patch("market_pipeline.jquants.client.requests.get", return_value=m):
            with pytest.raises(JQuantsResponseError):
                client.health_check()

    def test_auth_failure_raises(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        m = MagicMock()
        m.status_code = 401
        m.text = '{"message":"unauth"}'
        with patch("market_pipeline.jquants.client.requests.get", return_value=m):
            with pytest.raises(JQuantsAuthError):
                client.health_check()


# ---------------------------------------------------------------------------
# async path
# ---------------------------------------------------------------------------
class _FakeAsyncResponse:
    def __init__(self, status: int, body: str):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._body


class _FakeAsyncSession:
    """`session.get(...)` の戻り値を順次返す簡易セッション。"""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, headers=None):
        self.calls.append((url, dict(params or {})))
        return self._responses.pop(0)


class TestAsync:
    def test_get_async_success(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        session = _FakeAsyncSession([_FakeAsyncResponse(200, '{"data": [{"i":1}]}')])

        async def run():
            return await client.get_async(session, "/v2/equities/master")  # type: ignore[arg-type]

        result = asyncio.run(run())
        assert result == {"data": [{"i": 1}]}

    def test_get_async_429_retries(self):
        client = JQuantsClient(
            api_key=TEST_API_KEY,
            max_retries=1,
            retry_initial_seconds=0.001,
            retry_max_seconds=0.01,
        )
        session = _FakeAsyncSession(
            [
                _FakeAsyncResponse(429, '{"message":"r"}'),
                _FakeAsyncResponse(200, '{"data":[]}'),
            ]
        )

        async def run():
            return await client.get_async(session, "/v2/equities/master")  # type: ignore[arg-type]

        result = asyncio.run(run())
        assert result == {"data": []}

    def test_get_async_401_immediate(self):
        client = JQuantsClient(api_key=TEST_API_KEY, max_retries=3)
        session = _FakeAsyncSession([_FakeAsyncResponse(401, '{"message":"x"}')])

        async def run():
            return await client.get_async(session, "/v2/equities/master")  # type: ignore[arg-type]

        with pytest.raises(JQuantsAuthError):
            asyncio.run(run())
        assert len(session.calls) == 1

    def test_get_async_network_error(self):
        client = JQuantsClient(
            api_key=TEST_API_KEY,
            max_retries=1,
            retry_initial_seconds=0.001,
            retry_max_seconds=0.01,
        )

        class _BadSession:
            def get(self, *a, **k):
                raise aiohttp.ClientError("boom")

        async def run():
            return await client.get_async(_BadSession(), "/v2/equities/master")  # type: ignore[arg-type]

        with pytest.raises(JQuantsServerError):
            asyncio.run(run())

    def test_paginate_async_follows_pages(self):
        client = JQuantsClient(api_key=TEST_API_KEY)
        session = _FakeAsyncSession(
            [
                _FakeAsyncResponse(200, '{"data":[{"i":1}], "pagination_key":"k1"}'),
                _FakeAsyncResponse(200, '{"data":[{"i":2}], "pagination_key":"k2"}'),
                _FakeAsyncResponse(200, '{"data":[{"i":3}]}'),
            ]
        )

        async def run():
            collected = []
            async for page in client.paginate_async(session, "/v2/equities/bars/daily"):  # type: ignore[arg-type]
                collected.append(page)
            return collected

        pages = asyncio.run(run())
        assert pages == [[{"i": 1}], [{"i": 2}], [{"i": 3}]]
        # 2 回目以降は params に pagination_key
        assert session.calls[1][1].get("pagination_key") == "k1"
        assert session.calls[2][1].get("pagination_key") == "k2"
