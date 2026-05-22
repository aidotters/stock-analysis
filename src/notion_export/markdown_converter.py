"""Convert Markdown text to Notion blocks (list of dict).

The converter is designed for investment-analysis reports produced by
``/analyze-stock``. It handles headings, paragraphs, lists (1-level nesting),
tables, quotes, code blocks, images, YAML frontmatter, horizontal rules and
toggle wrapping (for Deep Research). Inline markup (bold / italic /
inline_code / strikethrough / link) is converted to Notion ``rich_text``
annotations.
"""

from __future__ import annotations

import logging
import re
from typing import Callable, Iterable, Optional

from markdown_it import MarkdownIt
from markdown_it.token import Token

logger = logging.getLogger(__name__)

# Notion limits
_NOTION_TEXT_LIMIT = 2000

# Allowed link schemes (case-insensitive). Anything else falls back to plain text.
_ALLOWED_LINK_SCHEMES = ("http://", "https://", "mailto:")

# YAML frontmatter at file head (only matches when text starts with ``---\n``).
_FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)

ImageResolver = Callable[[str], Optional[str]]
"""Callable accepting an image src (string) and returning ``file_upload`` id or ``None`` for external."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def convert(
    markdown_text: str,
    *,
    image_resolver: Optional[ImageResolver] = None,
    wrap_in_toggle: Optional[str] = None,
) -> list[dict]:
    """Convert Markdown text to Notion blocks.

    Parameters
    ----------
    markdown_text:
        The Markdown source. May begin with a YAML frontmatter block
        (``---\n...\n---``) which is rendered as a leading ``callout`` block.
    image_resolver:
        Optional callable invoked for each ``![alt](src)`` occurrence with the
        image src. Returning a string is treated as a ``file_upload`` id;
        returning ``None`` falls back to ``external`` URL (only for absolute
        ``http(s)://`` URLs - relative paths become a placeholder image block
        without ``file_upload`` id when ``image_resolver`` returns ``None``).
    wrap_in_toggle:
        When provided, wraps the whole result in a single ``toggle`` block
        with this label. Used by Deep Research sections.

    Returns
    -------
    list of dict
        Notion block dicts ready to be sent to the REST API.
    """

    blocks: list[dict] = []

    frontmatter_match = _FRONTMATTER_RE.match(markdown_text)
    if frontmatter_match:
        fm_block = _build_frontmatter_callout(frontmatter_match.group(1))
        if fm_block is not None:
            blocks.append(fm_block)
        markdown_text = markdown_text[frontmatter_match.end() :]

    md = MarkdownIt("commonmark", {"html": False, "breaks": False})
    md.enable(["table", "strikethrough"])
    # Disable upstream link scheme filtering - we do our own checks so we can
    # emit a WARN log for non-standard schemes.
    md.validateLink = lambda _url: True  # type: ignore[assignment,method-assign]
    tokens = md.parse(markdown_text)

    body_blocks = _tokens_to_blocks(tokens, image_resolver=image_resolver)
    blocks.extend(body_blocks)

    if wrap_in_toggle is not None:
        return [
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": _plain_rich_text(wrap_in_toggle),
                    "children": blocks,
                },
            }
        ]
    return blocks


# ---------------------------------------------------------------------------
# Token walking
# ---------------------------------------------------------------------------


def _tokens_to_blocks(
    tokens: list[Token],
    *,
    image_resolver: Optional[ImageResolver],
    depth: int = 0,
) -> list[dict]:
    """Walk a flat token list and produce Notion blocks."""
    blocks: list[dict] = []
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok.type == "heading_open":
            level = int(tok.tag[1:]) if tok.tag.startswith("h") else 3
            level = min(max(level, 1), 3)  # h4+ flatten to h3
            inline = tokens[i + 1]
            rich = _inline_to_rich_text(inline.children or [])
            blocks.append(
                {
                    "object": "block",
                    "type": f"heading_{level}",
                    f"heading_{level}": {"rich_text": rich},
                }
            )
            i += 3  # heading_open, inline, heading_close
            continue

        if tok.type == "paragraph_open":
            inline = tokens[i + 1]
            i += 3
            # Bare image paragraph: single image inline -> image block
            image_block = _maybe_extract_image(inline, image_resolver)
            if image_block is not None:
                blocks.append(image_block)
                continue
            rich = _inline_to_rich_text(inline.children or [])
            blocks.extend(_split_paragraph_to_blocks(rich))
            continue

        if tok.type in ("bullet_list_open", "ordered_list_open"):
            ordered = tok.type == "ordered_list_open"
            close_type = "ordered_list_close" if ordered else "bullet_list_close"
            sub, consumed = _consume_until(tokens, i + 1, close_type)
            list_blocks = _list_tokens_to_blocks(
                sub,
                ordered=ordered,
                image_resolver=image_resolver,
                depth=depth,
            )
            blocks.extend(list_blocks)
            i += consumed + 2
            continue

        if tok.type == "blockquote_open":
            sub, consumed = _consume_until(tokens, i + 1, "blockquote_close")
            blocks.append(_blockquote_to_block(sub, image_resolver=image_resolver))
            i += consumed + 2
            continue

        if tok.type == "fence" or tok.type == "code_block":
            language = (tok.info or "").strip().split()[0] if tok.info else ""
            blocks.append(_code_block(tok.content, language))
            i += 1
            continue

        if tok.type == "hr":
            blocks.append({"object": "block", "type": "divider", "divider": {}})
            i += 1
            continue

        if tok.type == "table_open":
            sub, consumed = _consume_until(tokens, i + 1, "table_close")
            table_block = _table_to_block(sub)
            if table_block is not None:
                blocks.append(table_block)
            i += consumed + 2
            continue

        if tok.type == "html_block":
            logger.warning("Unsupported markdown syntax: html_block")
            blocks.extend(_split_paragraph_to_blocks(_plain_rich_text(tok.content)))
            i += 1
            continue

        # Unknown / unsupported.
        if tok.type not in {
            "paragraph_close",
            "heading_close",
            "bullet_list_close",
            "ordered_list_close",
            "blockquote_close",
            "list_item_close",
            "table_close",
            "thead_close",
            "tbody_close",
            "tr_close",
            "th_close",
            "td_close",
        }:
            logger.warning("Unsupported markdown syntax: %s", tok.type)
        i += 1

    return blocks


def _consume_until(
    tokens: list[Token], start: int, close_type: str
) -> tuple[list[Token], int]:
    """Collect tokens up to the matching closing tag at the same nesting depth."""
    depth = 1
    out: list[Token] = []
    j = start
    while j < len(tokens):
        t = tokens[j]
        if t.nesting == 1 and t.type == close_type.replace("_close", "_open"):
            depth += 1
        elif t.type == close_type:
            depth -= 1
            if depth == 0:
                return out, j - start
        out.append(t)
        j += 1
    return out, j - start


def _maybe_extract_image(
    inline: Token, image_resolver: Optional[ImageResolver]
) -> Optional[dict]:
    """If `inline` is a single ``![alt](src)`` paragraph, return an image block."""
    if not inline.children:
        return None
    # Allow surrounding whitespace
    significant = [c for c in inline.children if c.type != "softbreak"]
    if not significant:
        return None
    if len(significant) == 1 and significant[0].type == "image":
        return _image_block_from_token(significant[0], image_resolver)
    return None


def _image_block_from_token(
    token: Token, image_resolver: Optional[ImageResolver]
) -> dict:
    src = token.attrGet("src") or ""
    file_id = image_resolver(src) if image_resolver is not None else None
    if file_id is None:
        # External URL only valid for absolute http(s).
        if src.startswith(("http://", "https://")):
            return {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {"url": src},
                },
            }
        # Local relative path without resolver: emit placeholder block.
        return {
            "object": "block",
            "type": "image",
            "image": {
                "type": "external",
                "external": {"url": f"<unresolved:{src}>"},
            },
        }
    if isinstance(file_id, str) and file_id.startswith("<dry-run:"):
        return {
            "object": "block",
            "type": "image",
            "image": {
                "type": "file_upload",
                "file_upload": {"id": file_id},
            },
        }
    return {
        "object": "block",
        "type": "image",
        "image": {
            "type": "file_upload",
            "file_upload": {"id": file_id},
        },
    }


# ---------------------------------------------------------------------------
# List handling
# ---------------------------------------------------------------------------


def _list_tokens_to_blocks(
    tokens: list[Token],
    *,
    ordered: bool,
    image_resolver: Optional[ImageResolver],
    depth: int,
) -> list[dict]:
    """Convert the contents of a ``bullet_list`` / ``ordered_list`` token range."""
    item_block_type = "numbered_list_item" if ordered else "bulleted_list_item"
    blocks: list[dict] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.type != "list_item_open":
            i += 1
            continue
        item_inner, consumed = _consume_until(tokens, i + 1, "list_item_close")
        item_block = _build_list_item(
            item_inner,
            block_type=item_block_type,
            image_resolver=image_resolver,
            depth=depth,
        )
        blocks.append(item_block)
        i += consumed + 2
    return blocks


def _build_list_item(
    inner_tokens: list[Token],
    *,
    block_type: str,
    image_resolver: Optional[ImageResolver],
    depth: int,
) -> dict:
    """Build one ``bulleted_list_item`` / ``numbered_list_item`` block.

    The first paragraph becomes the item's rich_text. Nested lists/paragraphs
    become children. Depth >=1 is flattened to depth 1 (Notion accepts more
    but we keep it minimal to avoid runaway nesting).
    """
    rich_text: list[dict] = []
    children: list[dict] = []
    used_first_paragraph = False

    i = 0
    while i < len(inner_tokens):
        tok = inner_tokens[i]
        if not used_first_paragraph and tok.type == "paragraph_open":
            inline = inner_tokens[i + 1]
            rich_text = _inline_to_rich_text(inline.children or [])
            used_first_paragraph = True
            i += 3
            continue
        if tok.type in ("bullet_list_open", "ordered_list_open"):
            ordered = tok.type == "ordered_list_open"
            close_type = "ordered_list_close" if ordered else "bullet_list_close"
            sub, consumed = _consume_until(inner_tokens, i + 1, close_type)
            if depth < 1:
                child_blocks = _list_tokens_to_blocks(
                    sub,
                    ordered=ordered,
                    image_resolver=image_resolver,
                    depth=depth + 1,
                )
                children.extend(child_blocks)
            else:
                # Depth >=1: flatten - emit at same level as siblings via children
                child_blocks = _list_tokens_to_blocks(
                    sub,
                    ordered=ordered,
                    image_resolver=image_resolver,
                    depth=depth,
                )
                children.extend(child_blocks)
            i += consumed + 2
            continue
        if tok.type == "paragraph_open":
            inline = inner_tokens[i + 1]
            sub_rich = _inline_to_rich_text(inline.children or [])
            children.extend(_split_paragraph_to_blocks(sub_rich))
            i += 3
            continue
        i += 1

    block = {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": rich_text},
    }
    if children:
        block[block_type]["children"] = children
    return block


# ---------------------------------------------------------------------------
# Quote / Code / Table / Frontmatter
# ---------------------------------------------------------------------------


def _blockquote_to_block(
    inner_tokens: list[Token], *, image_resolver: Optional[ImageResolver]
) -> dict:
    """Combine consecutive paragraphs inside a blockquote into one quote block."""
    rich_text: list[dict] = []
    i = 0
    first_paragraph = True
    while i < len(inner_tokens):
        tok = inner_tokens[i]
        if tok.type == "paragraph_open":
            inline = inner_tokens[i + 1]
            if not first_paragraph:
                rich_text.append(_text_part("\n"))
            rich_text.extend(_inline_to_rich_text(inline.children or []))
            first_paragraph = False
            i += 3
            continue
        i += 1
    return {
        "object": "block",
        "type": "quote",
        "quote": {"rich_text": rich_text},
    }


def _code_block(content: str, language: str) -> dict:
    lang = (language or "").strip().lower()
    if not lang:
        lang = "plain text"
    # Strip trailing newline (markdown-it includes it).
    text = content.rstrip("\n")
    return {
        "object": "block",
        "type": "code",
        "code": {
            "rich_text": _plain_rich_text(text),
            "language": _normalize_code_language(lang),
        },
    }


# A small allowlist for Notion's code block language enum.
_NOTION_CODE_LANGUAGES = {
    "abap",
    "arduino",
    "bash",
    "basic",
    "c",
    "clojure",
    "coffeescript",
    "c++",
    "c#",
    "css",
    "dart",
    "diff",
    "docker",
    "elixir",
    "elm",
    "erlang",
    "flow",
    "fortran",
    "f#",
    "gherkin",
    "glsl",
    "go",
    "graphql",
    "groovy",
    "haskell",
    "html",
    "java",
    "javascript",
    "json",
    "julia",
    "kotlin",
    "latex",
    "less",
    "lisp",
    "livescript",
    "lua",
    "makefile",
    "markdown",
    "markup",
    "matlab",
    "mermaid",
    "nix",
    "objective-c",
    "ocaml",
    "pascal",
    "perl",
    "php",
    "plain text",
    "powershell",
    "prolog",
    "protobuf",
    "python",
    "r",
    "reason",
    "ruby",
    "rust",
    "sass",
    "scala",
    "scheme",
    "scss",
    "shell",
    "sql",
    "swift",
    "typescript",
    "vb.net",
    "verilog",
    "vhdl",
    "visual basic",
    "webassembly",
    "xml",
    "yaml",
}


def _normalize_code_language(lang: str) -> str:
    if lang in _NOTION_CODE_LANGUAGES:
        return lang
    aliases = {
        "py": "python",
        "js": "javascript",
        "ts": "typescript",
        "sh": "shell",
        "zsh": "shell",
        "yml": "yaml",
    }
    return aliases.get(lang, "plain text")


def _table_to_block(inner_tokens: list[Token]) -> Optional[dict]:
    """Build a ``table`` block from the contents of a ``table_open``/``table_close``."""
    rows: list[list[list[dict]]] = []  # rows -> cells -> rich_text
    has_header = False
    i = 0
    in_thead = False
    while i < len(inner_tokens):
        tok = inner_tokens[i]
        if tok.type == "thead_open":
            in_thead = True
            i += 1
            continue
        if tok.type == "thead_close":
            in_thead = False
            i += 1
            continue
        if tok.type == "tr_open":
            row_cells: list[list[dict]] = []
            j = i + 1
            while j < len(inner_tokens) and inner_tokens[j].type != "tr_close":
                if inner_tokens[j].type in ("th_open", "td_open"):
                    inline = inner_tokens[j + 1]
                    cell = _inline_to_rich_text(inline.children or [])
                    if not cell:
                        cell = _plain_rich_text("")
                    row_cells.append(cell)
                    j += 3  # *_open, inline, *_close
                    continue
                j += 1
            if in_thead:
                has_header = True
            rows.append(row_cells)
            i = j + 1
            continue
        i += 1

    if not rows:
        return None
    width = max(len(r) for r in rows)
    # Pad rows to uniform width.
    for r in rows:
        while len(r) < width:
            r.append(_plain_rich_text(""))

    return {
        "object": "block",
        "type": "table",
        "table": {
            "table_width": width,
            "has_column_header": has_header,
            "has_row_header": False,
            "children": [
                {
                    "object": "block",
                    "type": "table_row",
                    "table_row": {"cells": r},
                }
                for r in rows
            ],
        },
    }


def _build_frontmatter_callout(body: str) -> Optional[dict]:
    text = body.strip()
    if not text:
        return None
    return {
        "object": "block",
        "type": "callout",
        "callout": {
            "rich_text": _plain_rich_text(text),
            "icon": {"type": "emoji", "emoji": "📋"},
        },
    }


# ---------------------------------------------------------------------------
# Inline -> rich_text
# ---------------------------------------------------------------------------


def _inline_to_rich_text(
    children: list[Token],
    *,
    base_annotations: Optional[dict] = None,
    base_link: Optional[dict] = None,
) -> list[dict]:
    """Walk inline tokens producing Notion rich_text elements.

    A stack-based annotations tracker is used so nested formatting
    (``**[link](url)**``) yields both annotation flags and a ``text.link``
    on the same rich_text element.
    """
    state = {
        "bold": False,
        "italic": False,
        "strikethrough": False,
        "code": False,
        "underline": False,
    }
    if base_annotations:
        state.update({k: v for k, v in base_annotations.items() if k in state})
    link_stack: list[dict] = []
    if base_link is not None:
        link_stack.append(base_link)

    out: list[dict] = []

    def _current_annotations() -> dict:
        return {
            "bold": state["bold"],
            "italic": state["italic"],
            "strikethrough": state["strikethrough"],
            "underline": False,
            "code": state["code"],
            "color": "default",
        }

    def _current_link() -> Optional[dict]:
        return link_stack[-1] if link_stack else None

    def _emit(content: str) -> None:
        if not content:
            return
        annotations = _current_annotations()
        link = _current_link()
        if out:
            last = out[-1]
            if (
                last["type"] == "text"
                and last["annotations"] == annotations
                and last["text"].get("link") == link
            ):
                last["text"]["content"] += content
                return
        element: dict = {
            "type": "text",
            "text": {"content": content, "link": link},
            "annotations": annotations,
        }
        out.append(element)

    for token in children:
        ttype = token.type
        if ttype == "text":
            _emit(token.content)
        elif ttype == "softbreak":
            _emit(" ")
        elif ttype == "hardbreak":
            _emit("\n")
        elif ttype == "strong_open":
            state["bold"] = True
        elif ttype == "strong_close":
            state["bold"] = False
        elif ttype == "em_open":
            state["italic"] = True
        elif ttype == "em_close":
            state["italic"] = False
        elif ttype == "s_open":
            state["strikethrough"] = True
        elif ttype == "s_close":
            state["strikethrough"] = False
        elif ttype == "code_inline":
            state["code"] = True
            _emit(token.content)
            state["code"] = False
        elif ttype == "link_open":
            href = token.attrGet("href") or ""
            if _is_allowed_link(href):
                link_stack.append({"url": href})
            else:
                logger.warning("Unsupported markdown syntax: link with scheme %r", href)
                link_stack.append(None)  # placeholder so close pops cleanly
        elif ttype == "link_close":
            if link_stack:
                link_stack.pop()
        elif ttype == "image":
            # Inline image inside a paragraph that also has other content.
            # Notion has no inline image. Render the alt text as a fallback.
            alt = (token.content or token.attrGet("alt") or "").strip()
            if alt:
                _emit(alt)
            logger.warning(
                "Unsupported markdown syntax: inline image (rendered as alt text)"
            )
        elif ttype == "html_inline":
            logger.warning("Unsupported markdown syntax: html_inline")
        else:
            logger.warning("Unsupported markdown syntax: inline token %s", ttype)

    # Some link_stack entries may be ``None`` placeholders; treat them as no link.
    cleaned: list[dict] = []
    for el in out:
        if (
            el["type"] == "text"
            and el["text"].get("link") is None
            and "link" in el["text"]
        ):
            # Notion accepts link=None; keep as-is.
            pass
        cleaned.append(el)

    # 2000 char split per element.
    return list(_split_long_rich_text(cleaned))


def _split_long_rich_text(elements: Iterable[dict]) -> Iterable[dict]:
    for el in elements:
        if el["type"] != "text":
            yield el
            continue
        content = el["text"]["content"]
        if len(content) <= _NOTION_TEXT_LIMIT:
            yield el
            continue
        link = el["text"].get("link")
        annotations = el["annotations"]
        for chunk in _chunks(content, _NOTION_TEXT_LIMIT):
            yield {
                "type": "text",
                "text": {"content": chunk, "link": link},
                "annotations": annotations,
            }


def _chunks(text: str, size: int) -> Iterable[str]:
    for i in range(0, len(text), size):
        yield text[i : i + size]


def _is_allowed_link(href: str) -> bool:
    if not href:
        return False
    lower = href.lower()
    return any(lower.startswith(s) for s in _ALLOWED_LINK_SCHEMES)


# ---------------------------------------------------------------------------
# Paragraph splitting (2000 char limit at paragraph block level)
# ---------------------------------------------------------------------------


def _rich_text_total_len(rich: list[dict]) -> int:
    return sum(len(el["text"]["content"]) for el in rich if el["type"] == "text")


def _split_paragraph_to_blocks(rich: list[dict]) -> list[dict]:
    """Split a single paragraph's rich_text into multiple ``paragraph`` blocks
    so that each block stays under :data:`_NOTION_TEXT_LIMIT` characters.

    Annotation boundaries are respected by splitting individual rich_text
    elements when they straddle the cut.
    """
    if not rich:
        return [
            {"object": "block", "type": "paragraph", "paragraph": {"rich_text": []}}
        ]
    if _rich_text_total_len(rich) <= _NOTION_TEXT_LIMIT:
        return [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": rich},
            }
        ]

    blocks: list[dict] = []
    current: list[dict] = []
    current_len = 0
    for el in rich:
        if el["type"] != "text":
            current.append(el)
            continue
        content = el["text"]["content"]
        link = el["text"].get("link")
        annotations = el["annotations"]
        pos = 0
        while pos < len(content):
            remaining = _NOTION_TEXT_LIMIT - current_len
            if remaining <= 0:
                blocks.append(_paragraph_block(current))
                current = []
                current_len = 0
                remaining = _NOTION_TEXT_LIMIT
            take = min(remaining, len(content) - pos)
            piece = content[pos : pos + take]
            current.append(
                {
                    "type": "text",
                    "text": {"content": piece, "link": link},
                    "annotations": annotations,
                }
            )
            current_len += take
            pos += take
            if current_len >= _NOTION_TEXT_LIMIT:
                blocks.append(_paragraph_block(current))
                current = []
                current_len = 0
    if current:
        blocks.append(_paragraph_block(current))
    return blocks


def _paragraph_block(rich: list[dict]) -> dict:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": rich},
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _plain_rich_text(text: str) -> list[dict]:
    if text == "":
        return []
    return [
        {
            "type": "text",
            "text": {"content": text, "link": None},
            "annotations": _default_annotations(),
        }
    ]


def _default_annotations() -> dict:
    return {
        "bold": False,
        "italic": False,
        "strikethrough": False,
        "underline": False,
        "code": False,
        "color": "default",
    }


def _text_part(content: str) -> dict:
    return {
        "type": "text",
        "text": {"content": content, "link": None},
        "annotations": _default_annotations(),
    }


__all__ = ["convert", "ImageResolver"]
