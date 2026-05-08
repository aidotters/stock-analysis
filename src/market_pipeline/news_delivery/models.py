"""共通データモデル: NewsItem / WatchListEntry。"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from market_pipeline.news_delivery.exceptions import WatchListSchemaError

Tag = Literal["holding", "considering", "monitoring"]
Priority = Literal["high", "mid", "low"]
Importance = Literal["high", "mid", "low"]
Category = Literal["disclosure", "news", "ir"]

VALID_TAGS: tuple[str, ...] = ("holding", "considering", "monitoring")
VALID_PRIORITIES: tuple[str, ...] = ("high", "mid", "low")

_CODE_PATTERN = re.compile(r"^\d{4}$")


@dataclass(frozen=True)
class NewsItem:
    """全Fetcherが返す共通データクラス。"""

    code: str
    title: str
    url: str
    source: str
    category: str
    published_at: Optional[datetime] = None
    summary: Optional[str] = None
    importance: Importance = "mid"

    @property
    def url_hash(self) -> str:
        """URLのSHA256ハッシュ(hexdigest)。重複排除キー。"""
        return hashlib.sha256(self.url.encode("utf-8")).hexdigest()


class WatchListEntry(BaseModel):
    """ウォッチリストの1エントリ。値域は Literal で限定。"""

    model_config = ConfigDict(extra="forbid")

    code: str
    tag: Tag
    priority: Priority
    added_at: date
    note: Optional[str] = None
    target_price: Optional[float] = None
    stop_loss: Optional[float] = None

    @field_validator("code")
    @classmethod
    def _validate_code(cls, v: str) -> str:
        if not isinstance(v, str) or not _CODE_PATTERN.match(v):
            raise ValueError(f"Invalid stock code: {v!r} (must be 4 digits)")
        return v

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def create(cls, **kwargs: Any) -> "WatchListEntry":
        """ValidationError を WatchListSchemaError に変換する生成ヘルパー。"""
        try:
            return cls(**kwargs)
        except ValidationError as e:
            msg = _format_validation_error(e, kwargs)
            raise WatchListSchemaError(msg) from e


def _format_validation_error(e: ValidationError, payload: dict[str, Any]) -> str:
    """ValidationError から人間可読なエラー文字列を作る。"""
    parts: list[str] = []
    for err in e.errors():
        loc = ".".join(str(x) for x in err.get("loc", ()))
        msg = err.get("msg", "invalid")
        if loc == "code":
            parts.append(f"Invalid stock code: {payload.get('code')!r}")
        elif loc == "tag":
            parts.append(f"Invalid tag: {payload.get('tag')!r}")
        elif loc == "priority":
            parts.append(f"Invalid priority: {payload.get('priority')!r}")
        else:
            parts.append(f"{loc}: {msg}")
    return "; ".join(parts) if parts else str(e)
