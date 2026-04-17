"""CommunicationCollector のユニットテスト."""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from market_pipeline.executives.communication_collector import (
    CommunicationCollector,
    _normalize_date,
    build_search_query,
    classify_source_type,
)
from market_pipeline.executives.repository import ExecutiveRepository


@pytest.fixture
def repo() -> ExecutiveRepository:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    r = ExecutiveRepository(db_path=tmp.name)
    r.initialize_tables()
    yield r
    Path(tmp.name).unlink(missing_ok=True)


def _no_op_extractor(url: str) -> None:
    """テスト時にネットワーク呼び出しを回避する日付抽出モック."""
    return None


class TestBuildSearchQuery:
    def test_single_generic_query(self) -> None:
        q = build_search_query("豊田章男", "トヨタ自動車")
        assert '"豊田章男"' in q
        assert '"トヨタ自動車"' in q
        assert "インタビュー" in q
        assert "after:" not in q  # since_date 未指定時は期間演算子が付かない

    def test_keyword_coverage(self) -> None:
        """対談・コラム・ブログ・メッセージ・登壇も含む広めのキーワード集合."""
        q = build_search_query("豊田章男", "トヨタ自動車")
        for kw in ("対談", "コラム", "ブログ", "メッセージ", "登壇"):
            assert kw in q, f"キーワード {kw} が欠落"

    def test_since_date_appends_after_operator(self) -> None:
        q = build_search_query("豊田章男", "トヨタ自動車", since_date="2023-04-17")
        assert q.endswith(" after:2023-04-17")
        assert '"豊田章男"' in q


class TestClassifySourceType:
    def test_nikkei_is_article(self) -> None:
        assert classify_source_type("https://www.nikkei.com/article/abc") == "article"

    def test_note_is_blog(self) -> None:
        assert classify_source_type("https://note.com/ceo/n/x123") == "blog"

    def test_youtube_is_speech(self) -> None:
        assert classify_source_type("https://youtube.com/watch?v=abc") == "speech"

    def test_interview_title_kw(self) -> None:
        assert (
            classify_source_type("https://example.com/a", "社長インタビュー後編")
            == "interview"
        )

    def test_speech_title_kw(self) -> None:
        assert (
            classify_source_type("https://example.com/a", "株主総会での講演")
            == "speech"
        )

    def test_default_article(self) -> None:
        assert classify_source_type("https://example.com/a") == "article"


class TestCollectWithMockedSearch:
    def test_inserts_results_into_db(self, repo: ExecutiveRepository) -> None:
        search_fn = MagicMock(
            return_value=[
                {
                    "url": "https://nikkei.com/a",
                    "title": "社長インタビュー",
                    "snippet": "概要",
                    "published_date": "2026-02-15",
                },
                {
                    "url": "https://note.com/ceo/1",
                    "title": "noteの記事",
                    "snippet": "概要2",
                },
            ]
        )
        collector = CommunicationCollector(
            web_search_fn=search_fn,
            repository=repo,
            cache_ttl_days=30,
            date_extractor_fn=_no_op_extractor,
        )
        result = collector.collect("佐藤恒治", "トヨタ自動車", code="7203")
        assert len(result) == 2
        assert search_fn.call_count == 1

        rows = repo.get_communications("佐藤恒治")
        assert len(rows) == 2
        assert {r["source_type"] for r in rows} == {"article", "blog"}

    def test_cache_hit_skips_web_search(self, repo: ExecutiveRepository) -> None:
        # 事前にDBへ発信レコードを挿入してキャッシュヒット状態にする
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "佐藤恒治",
                    "source_url": "https://nikkei.com/a",
                    "source_type": "article",
                    "title": "既存レコード",
                }
            ]
        )
        search_fn = MagicMock(return_value=[])
        collector = CommunicationCollector(
            web_search_fn=search_fn,
            repository=repo,
            cache_ttl_days=30,
            date_extractor_fn=_no_op_extractor,
        )
        result = collector.collect("佐藤恒治", "トヨタ自動車", code="7203")
        assert search_fn.call_count == 0  # キャッシュ優先
        assert len(result) == 1
        assert result[0].source_url == "https://nikkei.com/a"

    def test_force_refresh_bypasses_cache(self, repo: ExecutiveRepository) -> None:
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "佐藤恒治",
                    "source_url": "https://nikkei.com/old",
                    "source_type": "article",
                    "title": "旧",
                }
            ]
        )
        search_fn = MagicMock(
            return_value=[
                {
                    "url": "https://nikkei.com/new",
                    "title": "新規",
                }
            ]
        )
        collector = CommunicationCollector(
            web_search_fn=search_fn,
            repository=repo,
            cache_ttl_days=30,
            date_extractor_fn=_no_op_extractor,
        )
        collector.collect("佐藤恒治", "トヨタ自動車", code="7203", force_refresh=True)
        assert search_fn.call_count == 1
        rows = repo.get_communications("佐藤恒治")
        urls = {r["source_url"] for r in rows}
        assert urls == {"https://nikkei.com/old", "https://nikkei.com/new"}

    def test_empty_search_result_handled(self, repo: ExecutiveRepository) -> None:
        collector = CommunicationCollector(
            web_search_fn=lambda q: [],
            repository=repo,
            date_extractor_fn=_no_op_extractor,
        )
        result = collector.collect("無名氏", "無名社", code="0000")
        assert result == []

    def test_search_exception_returns_cached_only(
        self, repo: ExecutiveRepository
    ) -> None:
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "佐藤恒治",
                    "source_url": "https://nikkei.com/a",
                    "source_type": "article",
                    "title": "既存",
                }
            ]
        )
        # collected_at を古くしてキャッシュ無効化
        import sqlite3

        with sqlite3.connect(repo._db_path) as conn:
            conn.execute(
                "UPDATE executive_communications SET collected_at = '2000-01-01 00:00:00'"
            )

        def boom(q: str) -> list[dict]:
            raise RuntimeError("timeout")

        collector = CommunicationCollector(
            web_search_fn=boom, repository=repo, date_extractor_fn=_no_op_extractor
        )
        result = collector.collect("佐藤恒治", "トヨタ自動車", code="7203")
        # 例外時は既存キャッシュを返す（バッチを止めない）
        assert len(result) == 1

    def test_no_search_fn_returns_cached(self, repo: ExecutiveRepository) -> None:
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "佐藤恒治",
                    "source_url": "https://nikkei.com/a",
                    "source_type": "article",
                    "title": "t",
                }
            ]
        )
        import sqlite3

        with sqlite3.connect(repo._db_path) as conn:
            conn.execute(
                "UPDATE executive_communications SET collected_at = '2000-01-01 00:00:00'"
            )

        collector = CommunicationCollector(
            web_search_fn=None, repository=repo, date_extractor_fn=_no_op_extractor
        )
        # キャッシュ無効＋検索関数なし→既存DBレコードをそのまま返す
        result = collector.collect("佐藤恒治", "トヨタ自動車", code="7203")
        assert len(result) == 1

    def test_lookback_days_applied_to_query(self, repo: ExecutiveRepository) -> None:
        """collect 実行時、WebSearch に渡るクエリに after: 演算子が含まれる."""
        search_fn = MagicMock(return_value=[])
        collector = CommunicationCollector(
            web_search_fn=search_fn,
            repository=repo,
            date_extractor_fn=_no_op_extractor,
            lookback_days=1095,
        )
        collector.collect("山田", "例社", code="0000", force_refresh=True)
        assert search_fn.call_count == 1
        query = search_fn.call_args.args[0]
        assert " after:" in query
        expected_since = (datetime.now().date() - timedelta(days=1095)).strftime(
            "%Y-%m-%d"
        )
        assert f"after:{expected_since}" in query

    def test_since_date_filters_cache_read(self, repo: ExecutiveRepository) -> None:
        """キャッシュヒット経路でも since_date で古いレコードが除外される."""
        old_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
        recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "佐藤",
                    "source_url": "https://x/old",
                    "source_type": "article",
                    "published_date": old_date,
                    "title": "古い",
                },
                {
                    "code": "7203",
                    "executive_name": "佐藤",
                    "source_url": "https://x/new",
                    "source_type": "article",
                    "published_date": recent_date,
                    "title": "新しい",
                },
            ]
        )
        # lookback_days=365 なら 400日前は除外、30日前は残る
        collector = CommunicationCollector(
            web_search_fn=MagicMock(return_value=[]),
            repository=repo,
            date_extractor_fn=_no_op_extractor,
            lookback_days=365,
        )
        result = collector.collect("佐藤", "トヨタ自動車", code="7203")
        urls = {c.source_url for c in result}
        assert urls == {"https://x/new"}

    def test_deduplicates_urls_in_single_search(
        self, repo: ExecutiveRepository
    ) -> None:
        search_fn = MagicMock(
            return_value=[
                {"url": "https://example.com/1", "title": "A"},
                {"url": "https://example.com/1", "title": "A重複"},
                {"url": "https://example.com/2", "title": "B"},
            ]
        )
        collector = CommunicationCollector(
            web_search_fn=search_fn, repository=repo, date_extractor_fn=_no_op_extractor
        )
        result = collector.collect("x", "y", code="0000")
        assert len(result) == 2


class TestCacheTtlBoundary:
    def test_fresh_record_is_valid(self, repo: ExecutiveRepository) -> None:
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "A",
                    "source_url": "https://x/1",
                    "source_type": "article",
                    "title": "t",
                }
            ]
        )
        assert repo.is_cache_valid("A", ttl_days=30) is True

    def test_old_record_is_invalid(self, repo: ExecutiveRepository) -> None:
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "A",
                    "source_url": "https://x/1",
                    "source_type": "article",
                    "title": "t",
                }
            ]
        )
        import sqlite3

        old_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(repo._db_path) as conn:
            conn.execute(
                "UPDATE executive_communications SET collected_at = ?", (old_date,)
            )
        assert repo.is_cache_valid("A", ttl_days=30) is False


class TestNormalizeDate:
    """_normalize_date は先頭マッチする日付パターンから YYYY-MM-DD を抽出する."""

    def test_none_returns_none(self) -> None:
        assert _normalize_date(None) is None
        assert _normalize_date("") is None

    def test_iso_date(self) -> None:
        assert _normalize_date("2024-05-12") == "2024-05-12"

    def test_iso_datetime_with_timezone(self) -> None:
        assert _normalize_date("2024-05-12T08:00:00+09:00") == "2024-05-12"

    def test_slash_date_zero_pads(self) -> None:
        assert _normalize_date("2024/5/7") == "2024-05-07"

    def test_japanese_date(self) -> None:
        assert _normalize_date("2024年5月12日") == "2024-05-12"

    def test_yyyymmdd(self) -> None:
        assert _normalize_date("20240512") == "2024-05-12"

    def test_unparseable_returns_original(self) -> None:
        assert _normalize_date("not-a-date") == "not-a-date"

    def test_surrounding_whitespace_stripped(self) -> None:
        assert _normalize_date("  2024-05-12  ") == "2024-05-12"
