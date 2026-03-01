"""YAML設定パーサーの単体テスト。"""

from __future__ import annotations

import pytest
import yaml

from market_pipeline.news.config_parser import (
    NewsSource,
    NewsConfig,
    load_config,
    get_sources_by_category,
)


@pytest.fixture
def full_config_yaml(tmp_path):
    """全項目が指定された設定ファイル。"""
    config = {
        "sources": {
            "news": [
                {
                    "name": "日経電子版",
                    "url": "https://www.nikkei.com/markets/",
                    "auth": "cdp",
                    "selector": "article",
                    "description": "日経の市場ニュース一覧",
                }
            ],
            "analysis": [
                {
                    "name": "トウシル",
                    "url": "https://media.rakuten-sec.net/",
                    "auth": "none",
                    "selector": "article",
                    "description": "楽天証券の投資情報メディア",
                }
            ],
        }
    }
    path = tmp_path / "news_sources.yaml"
    path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")
    return path


@pytest.fixture
def minimal_config_yaml(tmp_path):
    """最小項目のみの設定ファイル。"""
    config = {
        "sources": {
            "news": [
                {
                    "name": "TestSite",
                    "url": "https://example.com/",
                    "auth": "none",
                }
            ]
        }
    }
    path = tmp_path / "news_sources.yaml"
    path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")
    return path


@pytest.fixture
def multi_category_yaml(tmp_path):
    """複数カテゴリの設定ファイル。"""
    config = {
        "sources": {
            "news": [
                {"name": "Site1", "url": "https://example.com/1", "auth": "none"},
                {"name": "Site2", "url": "https://example.com/2", "auth": "cdp"},
            ],
            "analysis": [
                {"name": "Site3", "url": "https://example.com/3", "auth": "none"},
            ],
            "financial": [
                {
                    "name": "Site4",
                    "url_template": "https://example.com/{code}",
                    "auth": "cdp",
                },
            ],
        }
    }
    path = tmp_path / "news_sources.yaml"
    path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")
    return path


class TestLoadConfigNormal:
    """正常系テスト。"""

    def test_full_config(self, full_config_yaml):
        """全項目指定のパーステスト。"""
        config = load_config(full_config_yaml)

        assert isinstance(config, NewsConfig)
        assert "news" in config.categories
        assert "analysis" in config.categories

        news = config.sources["news"]
        assert len(news) == 1
        assert news[0].name == "日経電子版"
        assert news[0].url == "https://www.nikkei.com/markets/"
        assert news[0].auth == "cdp"
        assert news[0].selector == "article"
        assert news[0].description == "日経の市場ニュース一覧"

    def test_minimal_config(self, minimal_config_yaml):
        """最小項目（name, url, auth）のみのパーステスト。"""
        config = load_config(minimal_config_yaml)

        news = config.sources["news"]
        assert len(news) == 1
        assert news[0].name == "TestSite"
        assert news[0].url == "https://example.com/"
        assert news[0].auth == "none"
        assert news[0].selector == ""
        assert news[0].description == ""

    def test_multi_category(self, multi_category_yaml):
        """複数カテゴリの読み込みテスト。"""
        config = load_config(multi_category_yaml)

        assert len(config.categories) == 3
        assert len(config.sources["news"]) == 2
        assert len(config.sources["analysis"]) == 1
        assert len(config.sources["financial"]) == 1
        assert len(config.all_sources) == 4

        # url_template を持つソース
        financial = config.sources["financial"][0]
        assert financial.url_template == "https://example.com/{code}"
        assert financial.url == ""


class TestLoadConfigError:
    """異常系テスト。"""

    def test_missing_name(self, tmp_path):
        """必須項目（name）欠落時のエラーテスト。"""
        config = {
            "sources": {"news": [{"url": "https://example.com/", "auth": "none"}]}
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config), encoding="utf-8")

        with pytest.raises(ValueError, match="missing required field 'name'"):
            load_config(path)

    def test_invalid_auth(self, tmp_path):
        """不正なauth値のエラーテスト。"""
        config = {
            "sources": {
                "news": [
                    {"name": "Bad", "url": "https://example.com/", "auth": "basic"}
                ]
            }
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config), encoding="utf-8")

        with pytest.raises(ValueError, match="Invalid auth value 'basic'"):
            load_config(path)

    def test_file_not_found(self, tmp_path):
        """存在しないファイルのエラーテスト。"""
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yaml")

    def test_missing_sources_key(self, tmp_path):
        """トップレベルキーが不正な場合。"""
        path = tmp_path / "bad.yaml"
        path.write_text("key: value", encoding="utf-8")

        with pytest.raises(ValueError, match="top-level 'sources' key"):
            load_config(path)

    def test_no_url_and_no_template(self, tmp_path):
        """urlもurl_templateもない場合のエラーテスト。"""
        config = {"sources": {"news": [{"name": "NoUrl", "auth": "none"}]}}
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config), encoding="utf-8")

        with pytest.raises(
            ValueError, match="must have either 'url' or 'url_template'"
        ):
            load_config(path)

    def test_sources_not_dict(self, tmp_path):
        """sourcesがdictでない場合のエラーテスト。"""
        path = tmp_path / "bad.yaml"
        path.write_text("sources: not_a_dict", encoding="utf-8")

        with pytest.raises(ValueError, match="must be a mapping of categories"):
            load_config(path)

    def test_category_not_list(self, tmp_path):
        """カテゴリの値がリストでない場合のエラーテスト。"""
        config = {"sources": {"news": "not_a_list"}}
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config), encoding="utf-8")

        with pytest.raises(ValueError, match="must be a list of sources"):
            load_config(path)

    def test_source_not_dict(self, tmp_path):
        """ソース項目がdictでない場合のエラーテスト。"""
        config = {"sources": {"news": ["just_a_string"]}}
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config), encoding="utf-8")

        with pytest.raises(ValueError, match="must be a mapping"):
            load_config(path)

    def test_missing_auth(self, tmp_path):
        """authフィールド欠落時のエラーテスト。"""
        config = {
            "sources": {"news": [{"name": "NoAuth", "url": "https://example.com"}]}
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config), encoding="utf-8")

        with pytest.raises(ValueError, match="missing required field 'auth'"):
            load_config(path)


class TestGetSourcesByCategory:
    """get_sources_by_categoryのテスト。"""

    def test_existing_category(self, multi_category_yaml):
        """存在するカテゴリの取得。"""
        config = load_config(multi_category_yaml)
        news = get_sources_by_category(config, "news")
        assert len(news) == 2

    def test_nonexistent_category(self, multi_category_yaml):
        """存在しないカテゴリは空リストを返す。"""
        config = load_config(multi_category_yaml)
        result = get_sources_by_category(config, "nonexistent")
        assert result == []


class TestNewsSource:
    """NewsSourceデータクラスのテスト。"""

    def test_frozen(self):
        """immutableであることの確認。"""
        source = NewsSource(name="Test", url="https://example.com", auth="none")
        with pytest.raises(AttributeError):
            source.name = "Changed"  # type: ignore[misc]
