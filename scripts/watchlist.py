"""ウォッチリストCRUD CLI。

使用例:
    python scripts/watchlist.py add 7203 --tag holding --priority high --note "押し目検討"
    python scripts/watchlist.py list
    python scripts/watchlist.py list --tag holding
    python scripts/watchlist.py update 7203 --priority mid
    python scripts/watchlist.py remove 7203
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from typing import Optional

# Allow running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from market_pipeline.news_delivery.exceptions import (  # noqa: E402
    WatchListError,
    WatchListSchemaError,
)
from market_pipeline.news_delivery.models import (  # noqa: E402
    VALID_PRIORITIES,
    VALID_TAGS,
)
from market_pipeline.news_delivery.watchlist import WatchList, make_entry  # noqa: E402

_CODE_RE = re.compile(r"^\d{4}$")


def _err(msg: str) -> None:
    print(msg, file=sys.stderr)


def _validate_args_for_add_or_update(
    code: Optional[str], tag: Optional[str], priority: Optional[str]
) -> Optional[str]:
    if code is not None and not _CODE_RE.match(code):
        return f"Invalid stock code: {code} (must be 4 digits)"
    if tag is not None and tag not in VALID_TAGS:
        return f"Invalid tag: {tag} (must be one of {', '.join(VALID_TAGS)})"
    if priority is not None and priority not in VALID_PRIORITIES:
        return f"Invalid priority: {priority} (must be one of {', '.join(VALID_PRIORITIES)})"
    return None


def cmd_add(args: argparse.Namespace) -> int:
    err = _validate_args_for_add_or_update(args.code, args.tag, args.priority)
    if err:
        _err(err)
        return 1
    wl = WatchList.load(args.watchlist)
    if wl.get(args.code) is not None:
        _err(f"Code {args.code} already exists in watchlist")
        return 1
    try:
        entry = make_entry(
            code=args.code,
            tag=args.tag,
            priority=args.priority,
            note=args.note,
            added_at=date.today(),
        )
        wl.add(entry)
        wl.save()
    except (WatchListError, WatchListSchemaError) as e:
        _err(str(e))
        return 1
    print(f"Added: {entry.code} tag={entry.tag} priority={entry.priority}")
    return 0


def cmd_remove(args: argparse.Namespace) -> int:
    if not _CODE_RE.match(args.code):
        _err(f"Invalid stock code: {args.code} (must be 4 digits)")
        return 1
    wl = WatchList.load(args.watchlist)
    if not wl.remove(args.code):
        _err(f"Code {args.code} not found in watchlist")
        return 1
    wl.save()
    print(f"Removed: {args.code}")
    return 0


def cmd_update(args: argparse.Namespace) -> int:
    err = _validate_args_for_add_or_update(args.code, args.tag, args.priority)
    if err:
        _err(err)
        return 1
    wl = WatchList.load(args.watchlist)
    changes: dict[str, object] = {}
    if args.tag:
        changes["tag"] = args.tag
    if args.priority:
        changes["priority"] = args.priority
    if args.note is not None:
        changes["note"] = args.note
    if not changes:
        _err("No fields to update (use --tag, --priority, --note)")
        return 1
    try:
        new_entry = wl.update(args.code, **changes)
    except WatchListError as e:
        _err(str(e))
        return 1
    except WatchListSchemaError as e:
        _err(str(e))
        return 1
    wl.save()
    print(
        f"Updated: {new_entry.code} tag={new_entry.tag} priority={new_entry.priority}"
    )
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    wl = WatchList.load(args.watchlist)
    entries = wl.list()
    if args.tag:
        if args.tag not in VALID_TAGS:
            _err(f"Invalid tag: {args.tag}")
            return 1
        entries = [e for e in entries if e.tag == args.tag]
    if args.priority:
        if args.priority not in VALID_PRIORITIES:
            _err(f"Invalid priority: {args.priority}")
            return 1
        entries = [e for e in entries if e.priority == args.priority]

    if not entries:
        print("(empty)")
        return 0

    header = f"{'code':<6} {'tag':<12} {'priority':<8} {'added_at':<12} note"
    print(header)
    print("-" * len(header))
    for e in entries:
        note = e.note or ""
        print(
            f"{e.code:<6} {e.tag:<12} {e.priority:<8} "
            f"{e.added_at.isoformat():<12} {note}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="watchlist", description="Watchlist CRUD CLI")
    p.add_argument(
        "--watchlist",
        default="default",
        help="ウォッチリスト名 (default: default)",
    )
    sub = p.add_subparsers(dest="command", required=True)

    # add
    pa = sub.add_parser("add", help="銘柄を追加")
    pa.add_argument("code")
    pa.add_argument("--tag", required=True)
    pa.add_argument("--priority", required=True)
    pa.add_argument("--note", default=None)
    pa.set_defaults(func=cmd_add)

    # remove
    pr = sub.add_parser("remove", help="銘柄を削除")
    pr.add_argument("code")
    pr.set_defaults(func=cmd_remove)

    # update
    pu = sub.add_parser("update", help="銘柄の属性を更新")
    pu.add_argument("code")
    pu.add_argument("--tag", default=None)
    pu.add_argument("--priority", default=None)
    pu.add_argument("--note", default=None)
    pu.set_defaults(func=cmd_update)

    # list
    pl = sub.add_parser("list", help="銘柄一覧を表示")
    pl.add_argument("--tag", default=None)
    pl.add_argument("--priority", default=None)
    pl.set_defaults(func=cmd_list)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
