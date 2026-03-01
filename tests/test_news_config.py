"""YAML設定パーサーの単体テスト。"""

from __future__ import annotations

import pytest
import yaml

from market_pipeline.news.config_parser import (
    FilterKeywords,
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

    def test_filter_keywords_default_none(self):
        """filter_keywordsのデフォルト値はNone。"""
        source = NewsSource(name="Test", url="https://example.com", auth="none")
        assert source.filter_keywords is None

    def test_filter_keywords_set(self):
        """filter_keywordsを設定できる。"""
        fk = FilterKeywords(include=["上方修正", "決算短信"], exclude=["定款"])
        source = NewsSource(
            name="Test", url="https://example.com", auth="none", filter_keywords=fk
        )
        assert source.filter_keywords is not None
        assert source.filter_keywords.include == ["上方修正", "決算短信"]
        assert source.filter_keywords.exclude == ["定款"]


class TestFilterKeywords:
    """FilterKeywordsデータクラスのテスト。"""

    def test_valid(self):
        """正常なFilterKeywordsの生成。"""
        fk = FilterKeywords(include=["上方修正"], exclude=["定款"])
        assert fk.include == ["上方修正"]
        assert fk.exclude == ["定款"]

    def test_exclude_default_empty(self):
        """excludeのデフォルトは空リスト。"""
        fk = FilterKeywords(include=["決算短信"])
        assert fk.exclude == []

    def test_empty_include_raises(self):
        """includeが空リストの場合はValueError。"""
        with pytest.raises(ValueError, match="include must not be empty"):
            FilterKeywords(include=[])

    def test_exclude_not_list_raises(self):
        """excludeがリストでない場合はValueError。"""
        with pytest.raises(ValueError, match="exclude must be a list"):
            FilterKeywords(include=["test"], exclude="not_a_list")  # type: ignore[arg-type]

    def test_frozen(self):
        """immutableであることの確認。"""
        fk = FilterKeywords(include=["上方修正"])
        with pytest.raises(AttributeError):
            fk.include = ["changed"]  # type: ignore[misc]


class TestFilterKeywordsYamlParse:
    """filter_keywordsを含むYAML設定のパーステスト。"""

    @pytest.fixture
    def disclosure_config_yaml(self, tmp_path):
        """disclosureカテゴリを含む設定ファイル。"""
        config = {
            "sources": {
                "disclosure": [
                    {
                        "name": "適時開示",
                        "url": "https://example.com/disclosure",
                        "auth": "none",
                        "filter_keywords": {
                            "include": ["上方修正", "決算短信", "自社株買い"],
                            "exclude": ["株主総会招集通知", "定款"],
                        },
                    }
                ]
            }
        }
        path = tmp_path / "news_sources.yaml"
        path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")
        return path

    @pytest.fixture
    def no_filter_keywords_yaml(self, tmp_path):
        """filter_keywordsが省略された設定ファイル。"""
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

    def test_parse_disclosure_with_filter_keywords(self, disclosure_config_yaml):
        """filter_keywordsを含むdisclosureカテゴリのパース。"""
        config = load_config(disclosure_config_yaml)
        assert "disclosure" in config.categories

        sources = config.sources["disclosure"]
        assert len(sources) == 1

        source = sources[0]
        assert source.name == "適時開示"
        assert source.filter_keywords is not None
        assert "上方修正" in source.filter_keywords.include
        assert "決算短信" in source.filter_keywords.include
        assert "自社株買い" in source.filter_keywords.include
        assert len(source.filter_keywords.include) == 3
        assert "株主総会招集通知" in source.filter_keywords.exclude
        assert "定款" in source.filter_keywords.exclude
        assert len(source.filter_keywords.exclude) == 2

    def test_parse_without_filter_keywords(self, no_filter_keywords_yaml):
        """filter_keywords省略時はNone。"""
        config = load_config(no_filter_keywords_yaml)
        source = config.sources["news"][0]
        assert source.filter_keywords is None

    def test_real_config_file(self):
        """実際のconfig/news_sources.yamlがパースできることを確認。"""
        from pathlib import Path

        config_path = Path("config/news_sources.yaml")
        if not config_path.exists():
            pytest.skip("config/news_sources.yaml not found")

        config = load_config(config_path)
        assert "disclosure" in config.categories

        disclosure_sources = get_sources_by_category(config, "disclosure")
        assert len(disclosure_sources) >= 1

        source = disclosure_sources[0]
        assert source.filter_keywords is not None
        assert len(source.filter_keywords.include) > 0

    def test_filter_keywords_not_dict_raises(self, tmp_path):
        """filter_keywordsがdictでない場合はValueError。"""
        config = {
            "sources": {
                "news": [
                    {
                        "name": "Bad",
                        "url": "https://example.com/",
                        "auth": "none",
                        "filter_keywords": "not_a_dict",
                    }
                ]
            }
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")

        with pytest.raises(ValueError, match="filter_keywords.*must be a mapping"):
            load_config(path)

    def test_filter_keywords_missing_include_in_yaml(self, tmp_path):
        """filter_keywords.includeがYAMLで省略された場合はValueError。"""
        config = {
            "sources": {
                "news": [
                    {
                        "name": "Bad",
                        "url": "https://example.com/",
                        "auth": "none",
                        "filter_keywords": {"exclude": ["skip"]},
                    }
                ]
            }
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")

        with pytest.raises(ValueError, match="include must not be empty"):
            load_config(path)

    def test_filter_keywords_empty_include_raises(self, tmp_path):
        """filter_keywords.includeが空リストの場合はValueError。"""
        config = {
            "sources": {
                "news": [
                    {
                        "name": "Bad",
                        "url": "https://example.com/",
                        "auth": "none",
                        "filter_keywords": {"include": [], "exclude": ["test"]},
                    }
                ]
            }
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")

        with pytest.raises(ValueError, match="include must not be empty"):
            load_config(path)
