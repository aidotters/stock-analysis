"""Slack Block Kit メッセージ生成器。

「全銘柄まとめて1メッセージ・銘柄ごとセクション」フォーマット。
40,000文字制限超過時は銘柄単位で分割する。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

from market_pipeline.news_delivery.models import NewsItem, WatchListEntry

logger = logging.getLogger(__name__)


SLOT_LABELS: dict[str, str] = {
    "morning": "🌅 朝のニュース配信",
    "noon": "☀️ 昼のニュース配信",
    "evening": "🌙 夜のニュース配信",
}

CATEGORY_EMOJI: dict[str, str] = {
    "disclosure": "📋",
    "news": "📰",
    "ir": "💼",
}

CATEGORY_LABEL: dict[str, str] = {
    "disclosure": "適時開示",
    "news": "ニュース",
    "ir": "IR",
}

# Slack の1メッセージ上限は40,000文字。安全マージンを取って分割しきい値を設定。
SPLIT_THRESHOLD_CHARS = 38_000


@dataclass(frozen=True)
class DeliveryStats:
    code_count: int
    new_items: int
    skipped_items: int


@dataclass(frozen=True)
class StockMetaInfo:
    long_name: Optional[str] = None
    sector: Optional[str] = None


class SlackFormatter:
    """ニュースを Slack Block Kit メッセージに整形する。"""

    def __init__(self) -> None:
        pass

    def build(
        self,
        slot: str,
        watchlist: list[WatchListEntry],
        items_by_code: dict[str, list[NewsItem]],
        stats: DeliveryStats,
        meta_resolver=None,  # type: ignore[no-untyped-def]
        now: Optional[datetime] = None,
        quiet_when_empty: bool = False,
    ) -> list[list[dict]]:
        """Block Kit メッセージ群(各メッセージは block list)を返す。

        Args:
            slot: morning/noon/evening
            watchlist: 銘柄順序を決めるエントリ列(items_by_codeのcodeに対応)
            items_by_code: code → 当該銘柄の未配信ニュース
            stats: 末尾メトリクス用
            meta_resolver: code を受け取り StockMetaInfo を返す callable。Noneなら未解決。
            now: タイムスタンプ(テスト用)
            quiet_when_empty: 新規ニュース0件のとき空リストを返す。
        """
        now = now or datetime.now()
        slot_label = SLOT_LABELS.get(slot, slot)
        header_text = f"{slot_label} ({now.strftime('%Y-%m-%d %H:%M')})"

        # 新規ニュースがあるエントリのみを順序保持して取り出す
        ordered_codes_with_items = [e for e in watchlist if items_by_code.get(e.code)]

        if stats.new_items == 0 or not ordered_codes_with_items:
            if quiet_when_empty:
                return []
            empty_blocks = [
                _header_block(header_text),
                _section_block("📭 本日新着なし"),
                _context_block(_metrics_line(stats)),
            ]
            return [empty_blocks]

        # 銘柄セクションごとに blocks を構築
        per_stock_blocks: list[list[dict]] = []
        for entry in ordered_codes_with_items:
            items = items_by_code.get(entry.code, [])
            if not items:
                continue
            meta = meta_resolver(entry.code) if meta_resolver is not None else None
            per_stock_blocks.append(_stock_blocks(entry, items, meta))

        return _split_messages(
            header_text=header_text,
            stock_blocks=per_stock_blocks,
            stats=stats,
        )


def _header_block(text: str) -> dict:
    return {
        "type": "header",
        "text": {"type": "plain_text", "text": text, "emoji": True},
    }


def _section_block(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _context_block(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _divider() -> dict:
    return {"type": "divider"}


def _metrics_line(stats: DeliveryStats) -> str:
    return (
        f"配信銘柄: {stats.code_count}件 | "
        f"新規ニュース: {stats.new_items}件 | "
        f"スキップ(重複): {stats.skipped_items}件"
    )


def _stock_blocks(
    entry: WatchListEntry,
    items: Iterable[NewsItem],
    meta: Optional[StockMetaInfo],
) -> list[dict]:
    """1銘柄分の Block 群を返す。先頭はヘッダ、続いて section。"""
    name = (meta.long_name if meta and meta.long_name else "").strip()
    name_part = f" {name}" if name else ""
    title = f"📊 {entry.code}{name_part} [{entry.tag} / {entry.priority}]"

    lines = []
    for it in items:
        emoji = CATEGORY_EMOJI.get(it.category, "•")
        cat_label = CATEGORY_LABEL.get(it.category, it.category)
        date_str = it.published_at.strftime("%Y/%m/%d") if it.published_at else "—"
        title_text = it.title.replace("\n", " ").strip()
        lines.append(f"{emoji} [{cat_label}] {title_text} ({date_str}) → {it.url}")

    section_text = "\n".join(lines)
    return [
        _header_block(title),
        _section_block(section_text),
    ]


def _block_chars(block: dict) -> int:
    return len(json.dumps(block, ensure_ascii=False))


def _blocks_chars(blocks: Iterable[dict]) -> int:
    return sum(_block_chars(b) for b in blocks)


def _split_messages(
    header_text: str,
    stock_blocks: list[list[dict]],
    stats: DeliveryStats,
) -> list[list[dict]]:
    """銘柄セクション境界で分割しつつ、最終メッセージにメトリクスを付与する。"""
    messages: list[list[dict]] = []
    current: list[dict] = [_header_block(header_text)]
    current_chars = _blocks_chars(current)

    for stock in stock_blocks:
        stock_chars = _blocks_chars(stock) + _block_chars(_divider())
        if current_chars + stock_chars > SPLIT_THRESHOLD_CHARS and len(current) > 1:
            messages.append(current)
            current = [_header_block(header_text + " (続き)")]
            current_chars = _blocks_chars(current)
        if len(current) > 1:
            current.append(_divider())
            current_chars += _block_chars(_divider())
        current.extend(stock)
        current_chars += _blocks_chars(stock)

    metrics = _context_block(_metrics_line(stats))
    metrics_chars = _block_chars(metrics)
    if current_chars + metrics_chars > SPLIT_THRESHOLD_CHARS and len(current) > 1:
        messages.append(current)
        current = [_header_block(header_text + " (続き)")]
    current.append(metrics)
    messages.append(current)
    return messages
