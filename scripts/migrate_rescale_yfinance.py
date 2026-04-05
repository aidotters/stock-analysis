"""
yfinanceデータのリスケーリングマイグ��ーション

yfinanceのauto_adjust=Trueで取得した価格データは配当+分割の遡及調整済みで、
J-QuantsのAdjustmentClose（分割のみ調整）と基準が異なる。
境界日（yfinance最終日とjquants最初日）の価格比を用いて、
yfinanceデータをJ-Quants基準にリスケールする。

Usage:
    python scripts/migrate_rescale_yfinance.py
    python scripts/migrate_rescale_yfinance.py --dry-run
    python scripts/migrate_rescale_yfinance.py --symbols 4347 7203
"""

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from market_pipeline.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PRICE_COLUMNS = [
    "AdjustmentOpen",
    "AdjustmentHigh",
    "AdjustmentLow",
    "AdjustmentClose",
]


def get_boundary_ratios(
    conn: sqlite3.Connection, symbols: list[str] | None = None
) -> list[dict]:
    """各銘柄のyfinance→jquants境界での価格比率を計算する��

    Returns:
        list of dict: code, yf_last_date, yf_last_close, jq_first_date, jq_first_close, ratio
    """
    where_clause = ""
    params: list = []
    if symbols:
        codes_5d = [f"{s}0" if len(s) == 4 else s for s in symbols]
        placeholders = ",".join(["?" for _ in codes_5d])
        where_clause = f"AND Code IN ({placeholders})"
        params = codes_5d

    # yfinanceの最終日を取得
    query = f"""
    SELECT Code, MAX(Date) as last_date
    FROM daily_quotes
    WHERE source = 'yfinance' {where_clause}
    GROUP BY Code
    """
    yf_last = conn.execute(query, params).fetchall()

    results = []
    for code, yf_last_date in yf_last:
        # yfinance最終日の終値
        row = conn.execute(
            "SELECT AdjustmentClose FROM daily_quotes WHERE Code = ? AND Date = ? AND source = 'yfinance'",
            (code, yf_last_date),
        ).fetchone()
        if not row or row[0] is None or row[0] == 0:
            continue
        yf_close = row[0]

        # jquants最初日の終値（yfinance最終日の翌日以降）
        row = conn.execute(
            "SELECT Date, AdjustmentClose FROM daily_quotes WHERE Code = ? AND Date > ? AND source = 'jquants' ORDER BY Date LIMIT 1",
            (code, yf_last_date),
        ).fetchone()
        if not row or row[1] is None or row[1] == 0:
            continue
        jq_first_date, jq_close = row

        ratio = jq_close / yf_close

        results.append(
            {
                "code": code,
                "yf_last_date": yf_last_date,
                "yf_last_close": yf_close,
                "jq_first_date": jq_first_date,
                "jq_first_close": jq_close,
                "ratio": ratio,
            }
        )

    return results


def rescale_yfinance_data(
    conn: sqlite3.Connection,
    code: str,
    ratio: float,
) -> int:
    """指定銘柄のyfinanceデータを比率で乗算更新する。"""
    cursor = conn.execute(
        f"""
        UPDATE daily_quotes
        SET {", ".join(f"{col} = {col} * ?" for col in PRICE_COLUMNS)}
        WHERE Code = ? AND source = 'yfinance'
        """,
        [ratio] * len(PRICE_COLUMNS) + [code],
    )
    return cursor.rowcount


def main():
    parser = argparse.ArgumentParser(
        description="yfinanceデータをJ-Quants基準にリスケール"
    )
    parser.add_argument("--dry-run", action="store_true", help="DB更新をスキップ")
    parser.add_argument("--symbols", nargs="+", help="指定銘���のみ処理")
    args = parser.parse_args()

    settings = get_settings()
    db_path = str(settings.paths.jquants_db)

    logger.info(f"DB: {db_path}")
    logger.info(f"dry-run: {args.dry_run}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    start_time = time.time()

    # 境界比率を計算
    ratios = get_boundary_ratios(conn, args.symbols)
    logger.info(f"境界比率計算完了: {len(ratios)}銘柄")

    # 統計
    normal = [r for r in ratios if 0.9 <= r["ratio"] <= 1.1]
    skewed = [r for r in ratios if not (0.9 <= r["ratio"] <= 1.1)]
    logger.info(f"  正常（±10%以内）: {len(normal)}銘柄")
    logger.info(f"  要リスケール（±10%超）: {len(skewed)}銘柄")

    if skewed:
        # 極端な例を表示
        skewed_sorted = sorted(skewed, key=lambda r: abs(r["ratio"] - 1), reverse=True)
        logger.info("  上位5件の乖離:")
        for r in skewed_sorted[:5]:
            code_4d = r["code"][:4]
            logger.info(
                f"    {code_4d}: yf={r['yf_last_close']:.2f} ({r['yf_last_date']}) "
                f"-> jq={r['jq_first_close']:.2f} ({r['jq_first_date']}) ratio={r['ratio']:.4f}"
            )

    if args.dry_run:
        logger.info("dry-run: DB更新をスキップ")
        conn.close()
        return

    # リスケール実行
    total_updated = 0
    rescaled_count = 0
    skipped_count = 0

    for i, r in enumerate(ratios, 1):
        # ±10%以内は正常とみなしスキップ
        if 0.9 <= r["ratio"] <= 1.1:
            skipped_count += 1
            continue

        updated = rescale_yfinance_data(conn, r["code"], r["ratio"])
        total_updated += updated
        rescaled_count += 1

        if i % 500 == 0:
            conn.commit()
            logger.info(
                f"  進捗: {i}/{len(ratios)} ({rescaled_count}銘柄リスケール, {total_updated}レコード更新)"
            )

    conn.commit()
    conn.close()

    elapsed = time.time() - start_time
    logger.info(
        f"完了: {rescaled_count}銘柄リスケール, {skipped_count}銘柄スキップ, {total_updated}レコード更新, {elapsed:.1f}s"
    )


if __name__ == "__main__":
    main()
