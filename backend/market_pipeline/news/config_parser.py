"""YAML設定ファイルのパーサー。巡回先サイト設定を読み込み・バリデーションする。"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


VALID_AUTH_VALUES = {"none", "cdp"}


@dataclass(frozen=True)
class NewsSource:
    """巡回先サイトの設定。"""

    name: str
    auth: str
    url: str = ""
    selector: str = ""
    description: str = ""
    url_template: str = ""

    def __post_init__(self) -> None:
        if self.auth not in VALID_AUTH_VALUES:
            raise ValueError(
                f"Invalid auth value '{self.auth}' for source '{self.name}'. "
                f"Must be one of: {', '.join(sorted(VALID_AUTH_VALUES))}"
            )
        if not self.url and not self.url_template:
            raise ValueError(
                f"Source '{self.name}' must have either 'url' or 'url_template'."
            )


@dataclass
class NewsConfig:
    """巡回先設定全体。カテゴリ別のNewsSourceリストを保持する。"""

    sources: dict[str, list[NewsSource]] = field(default_factory=dict)

    @property
    def categories(self) -> list[str]:
        return list(self.sources.keys())

    @property
    def all_sources(self) -> list[NewsSource]:
        result: list[NewsSource] = []
        for sources in self.sources.values():
            result.extend(sources)
        return result


def load_config(path: str | Path) -> NewsConfig:
    """YAML設定ファイルを読み込み、NewsConfigを返す。

    Args:
        path: YAML設定ファイルのパス

    Returns:
        NewsConfig: パース・バリデーション済みの設定

    Raises:
        FileNotFoundError: ファイルが存在しない場合
        ValueError: 設定内容が不正な場合
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict) or "sources" not in raw:
        raise ValueError("Config must have a top-level 'sources' key.")

    sources_raw = raw["sources"]
    if not isinstance(sources_raw, dict):
        raise ValueError("'sources' must be a mapping of categories.")

    config = NewsConfig()
    for category, items in sources_raw.items():
        if not isinstance(items, list):
            raise ValueError(f"Category '{category}' must be a list of sources.")

        parsed: list[NewsSource] = []
        for item in items:
            if not isinstance(item, dict):
                raise ValueError(f"Each source in '{category}' must be a mapping.")
            _validate_required_fields(item, category)
            parsed.append(
                NewsSource(
                    name=item["name"],
                    auth=item["auth"],
                    url=item.get("url", ""),
                    selector=item.get("selector", ""),
                    description=item.get("description", ""),
                    url_template=item.get("url_template", ""),
                )
            )
        config.sources[category] = parsed

    return config


def get_sources_by_category(config: NewsConfig, category: str) -> list[NewsSource]:
    """指定カテゴリの巡回先リストを返す。

    Args:
        config: NewsConfig
        category: カテゴリ名（news, analysis, financial等）

    Returns:
        該当カテゴリのNewsSourceリスト。カテゴリが存在しない場合は空リスト。
    """
    return config.sources.get(category, [])


def _validate_required_fields(item: dict[str, Any], category: str) -> None:
    """ソース定義の必須フィールドをチェックする。"""
    if "name" not in item:
        raise ValueError(
            f"Source in category '{category}' is missing required field 'name'."
        )
    if "auth" not in item:
        raise ValueError(
            f"Source '{item.get('name', '?')}' in category '{category}' "
            "is missing required field 'auth'."
        )
