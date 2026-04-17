#!/usr/bin/env python3
"""`/research-executives` スキルから呼ばれる CLI エントリポイント.

このスクリプトは Python 側のデータ取得・整形を担当する。
実際の WebSearch と LLM 呼び出しは Claude Code のスキル層が行い、
このスクリプトは「対象役員のリスト出力」「キャッシュ済み発信/評価の読み取り」
「Markdownレポートの組み立て」の責務のみを担う。

主な使い方:

1. スキル層が対象役員リストを取得する:
       python scripts/run_research_executives.py list-executives 7203 \
           --include-directors

2. スキル層が発信収集・スコアリングを外部実行した後、レポートを組み立てる:
       python scripts/run_research_executives.py build-report 7203 \
           --company "トヨタ自動車" --output output/reports/stocks/.../executive_report.md
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from market_pipeline.executives import (
    HIGHLIGHT_DAYS_RECENT,
    LOOKBACK_DAYS_TOTAL,
    ExecutiveRepository,
)

logger = logging.getLogger(__name__)

_CATEGORY_ICON = {
    "article": "📰",
    "blog": "✍️",
    "speech": "🎤",
    "interview": "🎙",
    "book": "📖",
}


def cmd_list_executives(args: argparse.Namespace) -> int:
    """対象役員をJSONで標準出力する."""
    repo = ExecutiveRepository()
    repo.initialize_tables()

    kwargs = {}
    if args.persons:
        kwargs["persons"] = [p.strip() for p in args.persons.split(",") if p.strip()]
    elif args.include_directors:
        kwargs["role_contains"] = "取締役"
    elif args.include_executive_officers:
        # 執行役系も代表フィルタを外す
        pass
    else:
        kwargs["is_representative"] = True

    out: list[dict] = []
    for code in args.codes:
        execs = repo.get_executives(code, **kwargs)
        for e in execs:
            out.append(
                {
                    "code": e.code,
                    "name": e.name,
                    "role": e.role,
                    "is_representative": e.is_representative,
                    "birthdate": e.birthdate,
                    "appointed_date": e.appointed_date,
                    "edinet_source_doc_id": e.edinet_source_doc_id,
                    "career_summary": e.career_summary,
                }
            )
    json.dump(out, sys.stdout, ensure_ascii=False, indent=2)
    return 0


def cmd_build_report(args: argparse.Namespace) -> int:
    """キャッシュ済みの evaluations と communications からレポートを組み立てる."""
    repo = ExecutiveRepository()
    repo.initialize_tables()

    kwargs = {}
    if args.persons:
        kwargs["persons"] = [p.strip() for p in args.persons.split(",") if p.strip()]
    elif args.include_directors:
        kwargs["role_contains"] = "取締役"
    elif not args.include_executive_officers:
        kwargs["is_representative"] = True

    execs = repo.get_executives(args.code, **kwargs)
    if not execs:
        content = (
            f"# {args.company} ({args.code}) 経営陣評価レポート\n\n対象役員なし。\n"
        )
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(content, encoding="utf-8")
        return 0

    # 収集・表示の対象期間（過去 lookback_days 日）と
    # ハイライト対象期間（直近 highlight_days 日）
    today = datetime.now().date()
    since_date = (today - timedelta(days=args.lookback_days)).strftime("%Y-%m-%d")
    highlight_since = (today - timedelta(days=args.highlight_days)).strftime("%Y-%m-%d")

    # 各役員の最新評価と発信を取得
    evals: dict[str, dict] = {}
    comms: dict[str, list[dict]] = {}
    for e in execs:
        ev = repo.get_latest_evaluation(e.code, e.name)
        if ev:
            evals[e.name] = ev
        comms[e.name] = repo.get_communications(e.name, since_date=since_date)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = [
        f"# {args.company} ({args.code}) 経営陣評価レポート",
        "",
        f"生成日時: {generated_at}",
        "",
        "## 1. 役員サマリー",
        "",
        "| 役員 | 役職 | 代表 | 総合スコア | 評価軸ハイライト |",
        "|------|------|:---:|---:|------|",
    ]

    for e in execs:
        ev = evals.get(e.name, {})
        overall = ev.get("overall_score")
        overall_str = f"{overall:.2f}" if isinstance(overall, (int, float)) else "—"
        hl = _top_axis_highlights(ev)
        rep = "✓" if e.is_representative else ""
        lines.append(f"| {e.name} | {e.role[:30]} | {rep} | {overall_str} | {hl} |")

    lines.extend(["", "## 2. 役員別評価", ""])
    for idx, e in enumerate(execs, start=1):
        ev = evals.get(e.name)
        lines.append(f"### 2.{idx} {e.name}（{e.role}）")
        if e.career_summary:
            lines.append("")
            lines.append("#### 略歴")
            lines.append(e.career_summary)
            lines.append("")
        if not ev or ev.get("overall_score") is None:
            lines.append("- 評価データなし／評価失敗")
        else:
            for axis_label, axis_key in (
                ("ビジョン一貫性", "vision_consistency"),
                ("実行力", "execution_track_record"),
                ("市場認識", "market_awareness"),
                ("リスク開示誠実性", "risk_disclosure_honesty"),
                ("コミュニケーション能力", "communication_clarity"),
                ("成長志向", "growth_ambition"),
            ):
                score = ev.get(axis_key)
                score_str = f"{score:.1f}" if isinstance(score, (int, float)) else "—"
                r = (ev.get("rationale") or {}).get(axis_key, "")
                lines.append(f"- **{axis_label} {score_str}**: {r}")
            lines.append(f"- **総合 {ev['overall_score']:.2f}**")
        lines.append("")

    lines.extend(
        [
            "## 3. タイムライン（発信日降順）",
            "",
            f"対象期間: 過去 {args.lookback_days} 日 ／ 🆕 は直近 "
            f"{args.highlight_days} 日のハイライト",
            "",
        ]
    )
    lines.append("| 日付 | カテゴリ | タイトル | URL |")
    lines.append("|------|--------|---------|-----|")
    timeline_rows: list[tuple[str, dict]] = []
    for name, rows in comms.items():
        for row in rows:
            timeline_rows.append((row.get("published_date") or "", row))
    # 発信日あり（降順）→ なし（末尾）の順に並べる
    timeline_rows.sort(key=lambda x: (x[0] == "", x[0]), reverse=False)
    timeline_rows.sort(key=lambda x: x[0], reverse=True)
    for published, row in timeline_rows[:50]:
        category = row.get("source_type") or "article"
        icon = _CATEGORY_ICON.get(category, "📰")
        title = (row.get("title") or "").replace("|", "｜")
        url = row.get("source_url") or ""
        is_recent = bool(published) and published >= highlight_since
        date_disp = published or "—"
        if is_recent:
            lines.append(
                f"| **{date_disp}** | 🆕 {icon} {category} | **{title}** | {url} |"
            )
        else:
            lines.append(f"| {date_disp} | {icon} {category} | {title} | {url} |")

    lines.extend(["", "## 4. 主要発信引用集", ""])
    for e in execs:
        entries = comms.get(e.name, [])
        if not entries:
            continue
        recent = [
            r
            for r in entries
            if (r.get("published_date") or "") >= highlight_since
            and r.get("published_date")
        ]
        recent.sort(key=lambda r: r.get("published_date") or "", reverse=True)
        older = [
            r
            for r in entries
            if not r.get("published_date")
            or (r.get("published_date") or "") < highlight_since
        ]
        older.sort(key=lambda r: r.get("published_date") or "", reverse=True)
        selected = (recent + older)[:5]
        if not selected:
            continue
        lines.append(f"### {e.name}")
        for row in selected:
            title = (row.get("title") or "").replace("|", "｜")
            url = row.get("source_url") or ""
            date_disp = row.get("published_date") or "—"
            summary = (row.get("summary") or "").replace("\n", " ").strip()
            lines.append(f"- 「{summary or title}」 — {title} ({date_disp}) — {url}")
        lines.append("")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {args.output}", file=sys.stderr)
    return 0


def _top_axis_highlights(ev: dict) -> str:
    """評価の最高軸・最低軸をハイライト表示する."""
    if not ev or ev.get("overall_score") is None:
        return "評価なし"
    axis_labels = {
        "vision_consistency": "ビジョン",
        "execution_track_record": "実行力",
        "market_awareness": "市場認識",
        "risk_disclosure_honesty": "誠実性",
        "communication_clarity": "発信力",
        "growth_ambition": "成長志向",
    }
    axis_scores = []
    for k, label in axis_labels.items():
        v = ev.get(k)
        if isinstance(v, (int, float)):
            axis_scores.append((label, v))
    if not axis_scores:
        return "—"
    axis_scores.sort(key=lambda x: x[1], reverse=True)
    high = axis_scores[0]
    low = axis_scores[-1]
    return f"{high[0]}{high[1]:.1f}／{low[0]}{low[1]:.1f}"


def main() -> int:
    parser = argparse.ArgumentParser(description="研究用 経営陣評価レポート CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list-executives", help="対象役員をJSON出力")
    p_list.add_argument("codes", nargs="+")
    p_list.add_argument("--include-directors", action="store_true")
    p_list.add_argument("--include-executive-officers", action="store_true")
    p_list.add_argument("--persons", default="")
    p_list.set_defaults(func=cmd_list_executives)

    p_build = sub.add_parser("build-report", help="キャッシュからレポート生成")
    p_build.add_argument("code")
    p_build.add_argument("--company", required=True)
    p_build.add_argument("--output", required=True)
    p_build.add_argument("--include-directors", action="store_true")
    p_build.add_argument("--include-executive-officers", action="store_true")
    p_build.add_argument("--persons", default="")
    p_build.add_argument(
        "--lookback-days",
        type=int,
        default=LOOKBACK_DAYS_TOTAL,
        help="タイムライン・引用集の対象期間（日数、デフォルト: 1095 = 過去3年）",
    )
    p_build.add_argument(
        "--highlight-days",
        type=int,
        default=HIGHLIGHT_DAYS_RECENT,
        help="直近ハイライト期間（日数、デフォルト: 365 = 直近1年）",
    )
    p_build.set_defaults(func=cmd_build_report)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
