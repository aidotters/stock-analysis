"""published_date_extractor のユニットテスト."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from market_pipeline.executives import published_date_extractor
from market_pipeline.executives.published_date_extractor import (
    _extract_from_jsonld,
    _extract_from_meta,
    _extract_from_time_tag,
    _extract_from_url_path,
    _normalize_iso_date,
    extract_published_date,
)
from bs4 import BeautifulSoup


@pytest.fixture(autouse=True)
def _reset_throttle_state() -> None:
    """テスト間でホスト別スロットル状態をリセット."""
    published_date_extractor._host_last_fetch.clear()


class TestNormalizeIsoDate:
    def test_iso8601_with_timezone(self) -> None:
        assert _normalize_iso_date("2024-05-12T08:00:00+09:00") == "2024-05-12"

    def test_iso8601_date_only(self) -> None:
        assert _normalize_iso_date("2024-05-12") == "2024-05-12"

    def test_slash_format(self) -> None:
        assert _normalize_iso_date("2024/5/12") == "2024-05-12"

    def test_japanese_format(self) -> None:
        assert _normalize_iso_date("2024年5月12日") == "2024-05-12"

    def test_compact_yyyymmdd(self) -> None:
        assert _normalize_iso_date("20240512") == "2024-05-12"

    def test_empty_returns_none(self) -> None:
        assert _normalize_iso_date("") is None

    def test_invalid_returns_none(self) -> None:
        assert _normalize_iso_date("not-a-date") is None


class TestExtractFromJsonLd:
    def test_top_level_date_published(self) -> None:
        html = """
        <html><head>
        <script type="application/ld+json">
        {"@context": "https://schema.org", "@type": "NewsArticle",
         "datePublished": "2024-03-15T09:00:00+09:00"}
        </script>
        </head></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_from_jsonld(soup) == "2024-03-15"

    def test_graph_nested_date(self) -> None:
        html = """
        <html><head>
        <script type="application/ld+json">
        {"@graph": [{"@type": "Article", "datePublished": "2023-11-01"}]}
        </script>
        </head></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_from_jsonld(soup) == "2023-11-01"

    def test_invalid_json_ignored(self) -> None:
        html = """
        <html><head>
        <script type="application/ld+json">{broken</script>
        </head></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_from_jsonld(soup) is None


class TestExtractFromMeta:
    def test_article_published_time(self) -> None:
        html = (
            '<html><head><meta property="article:published_time" '
            'content="2024-06-01T10:00:00Z"></head></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_from_meta(soup) == "2024-06-01"

    def test_dublin_core_date_issued(self) -> None:
        html = (
            '<html><head><meta name="DC.date.issued" content="2023/7/4"></head></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_from_meta(soup) == "2023-07-04"


class TestExtractFromTimeTag:
    def test_datetime_attribute(self) -> None:
        html = '<html><body><time datetime="2024-02-20T12:00:00"></time></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_from_time_tag(soup) == "2024-02-20"

    def test_text_content(self) -> None:
        html = "<html><body><time>2024年2月20日</time></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_from_time_tag(soup) == "2024-02-20"


class TestExtractFromUrlPath:
    def test_iso_date_in_path(self) -> None:
        assert (
            _extract_from_url_path("https://example.com/news/2024/05/12/article.html")
            == "2024-05-12"
        )

    def test_dashed_date_in_path(self) -> None:
        assert (
            _extract_from_url_path("https://example.com/2023-11-30-news")
            == "2023-11-30"
        )

    def test_invalid_date_rejected(self) -> None:
        assert _extract_from_url_path("https://example.com/2024/13/45/") is None

    def test_no_date_returns_none(self) -> None:
        assert _extract_from_url_path("https://example.com/article/foo-bar") is None


class TestExtractPublishedDate:
    def test_json_ld_preferred_over_meta(self) -> None:
        html = """
        <html><head>
        <meta property="article:published_time" content="2023-01-01">
        <script type="application/ld+json">
        {"datePublished": "2024-12-25"}
        </script>
        </head></html>
        """
        with patch("requests.Session") as session_cls:
            session_instance = MagicMock()
            session_instance.get.return_value = MagicMock(status_code=200, text=html)
            session_cls.return_value = session_instance
            result = extract_published_date("https://example.com/a", min_interval=0)
        assert result == "2024-12-25"

    def test_http_error_returns_url_hint(self) -> None:
        with patch("requests.Session") as session_cls:
            instance = MagicMock()
            instance.get.return_value = MagicMock(status_code=500, text="")
            session_cls.return_value = instance
            result = extract_published_date(
                "https://example.com/2024/01/15/article", min_interval=0
            )
        # URL内の日付パスから推定される
        assert result == "2024-01-15"

    def test_request_exception_returns_url_hint(self) -> None:
        import requests as rq

        with patch("requests.Session") as session_cls:
            instance = MagicMock()
            instance.get.side_effect = rq.Timeout()
            session_cls.return_value = instance
            result = extract_published_date(
                "https://example.com/2023/08/20/x", min_interval=0
            )
        assert result == "2023-08-20"

    def test_empty_url_returns_none(self) -> None:
        assert extract_published_date("") is None

    def test_fallback_to_time_tag(self) -> None:
        html = '<html><body><time datetime="2025-03-10"></time></body></html>'
        with patch("requests.Session") as session_cls:
            instance = MagicMock()
            instance.get.return_value = MagicMock(status_code=200, text=html)
            session_cls.return_value = instance
            result = extract_published_date(
                "https://example.com/article", min_interval=0
            )
        assert result == "2025-03-10"


class TestHostThrottle:
    """同一ホストへの連続アクセスを `min_interval` 秒以上空ける."""

    def test_throttle_enforces_host_interval(self) -> None:
        html = "<html></html>"
        with (
            patch("requests.Session") as session_cls,
            patch.object(published_date_extractor.time, "sleep") as sleep_mock,
            patch.object(
                published_date_extractor.time,
                "monotonic",
                side_effect=[100.0, 100.2, 101.0],
            ),
        ):
            instance = MagicMock()
            instance.get.return_value = MagicMock(status_code=200, text=html)
            session_cls.return_value = instance

            extract_published_date("https://same-host.example/a", min_interval=1.0)
            extract_published_date("https://same-host.example/b", min_interval=1.0)

        # 2回目は 100.2 - 100.0 = 0.2 秒しか経過していないので 0.8 秒 sleep
        sleep_mock.assert_called_once()
        assert sleep_mock.call_args[0][0] == pytest.approx(0.8, abs=1e-9)

    def test_throttle_skipped_for_different_hosts(self) -> None:
        html = "<html></html>"
        with (
            patch("requests.Session") as session_cls,
            patch.object(published_date_extractor.time, "sleep") as sleep_mock,
        ):
            instance = MagicMock()
            instance.get.return_value = MagicMock(status_code=200, text=html)
            session_cls.return_value = instance

            extract_published_date("https://host-a.example/a", min_interval=1.0)
            extract_published_date("https://host-b.example/b", min_interval=1.0)

        sleep_mock.assert_not_called()

    def test_min_interval_zero_disables_throttle(self) -> None:
        html = "<html></html>"
        with (
            patch("requests.Session") as session_cls,
            patch.object(published_date_extractor.time, "sleep") as sleep_mock,
        ):
            instance = MagicMock()
            instance.get.return_value = MagicMock(status_code=200, text=html)
            session_cls.return_value = instance

            extract_published_date("https://same-host.example/a", min_interval=0)
            extract_published_date("https://same-host.example/b", min_interval=0)

        sleep_mock.assert_not_called()
