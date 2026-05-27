#!/usr/bin/env python3
"""J-Quants V2 sustained rate-limit smoke test。

`JQuantsClient` で `/v2/equities/master` を一定時間 sustain 呼び出しし、
429 が発生しないことを確認するためのコマンドラインツール。

ユニットテストでは sliding-window 起因の 429 を顕在化させづらいため、
- レート制限設定変更時の動作確認
- 本番事故調査時の再現
- 新しい API キーや plan(Light/Standard/Premium) の挙動確認

を目的として手動実行する(CI 対象外)。

Usage:
    # デフォルト(60 秒、デフォルトレート 55req/min)
    python scripts/check_jquants_v2_sustained.py

    # 90 秒間 sustain、レートを 60req/min(spec ピッタリ)で試す
    python scripts/check_jquants_v2_sustained.py --duration 90 --rate 60

    # ドライラン(設定だけ表示して終了)
    python scripts/check_jquants_v2_sustained.py --dry-run

Exit codes:
    0: 期間内 429 ゼロ
    1: 429 を 1 回以上検出
    2: 認証エラー・ネットワーク異常等で sustain 不可
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from dotenv import load_dotenv

from market_pipeline.jquants.client import JQuantsClient
from market_pipeline.jquants.exceptions import (
    JQuantsAuthError,
    JQuantsRateLimitError,
    JQuantsServerError,
)

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="sustain させる秒数(デフォルト: 60)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=55,
        help="rate_limit_per_minute(デフォルト: 55、Light spec=60 の安全マージン)",
    )
    parser.add_argument(
        "--path",
        type=str,
        default="/v2/equities/master",
        help="ヘルスチェック用エンドポイント(デフォルト: /v2/equities/master)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="設定だけ表示して実行しない",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger(__name__)

    args = parse_args()

    logger.info("=== J-Quants V2 sustained smoke test ===")
    logger.info("duration: %.1f sec", args.duration)
    logger.info("rate_limit_per_minute: %d", args.rate)
    logger.info("path: %s", args.path)

    if args.dry_run:
        logger.info("(dry-run) 実行をスキップしました")
        return 0

    try:
        client = JQuantsClient(rate_limit_per_minute=args.rate)
    except Exception as exc:  # noqa: BLE001
        logger.error("client 生成失敗: %s", exc)
        return 2

    start = time.monotonic()
    request_count = 0
    rate_limit_count = 0
    last_log = start

    try:
        while time.monotonic() - start < args.duration:
            try:
                client.get(args.path)
                request_count += 1
            except JQuantsRateLimitError:
                # client 内の retry を使い切った場合のみここに到達(リトライ成功時は捕捉せず計上)
                rate_limit_count += 1
                logger.warning("429 を検出(累計 %d 回)", rate_limit_count)
            except JQuantsAuthError as exc:
                logger.error("認証失敗(.env の JQUANTS_API_KEY を確認): %s", exc)
                return 2

            now = time.monotonic()
            if now - last_log >= 10.0:
                elapsed = now - start
                effective_rpm = request_count / elapsed * 60 if elapsed > 0 else 0
                logger.info(
                    "経過 %.1fs / req=%d / 実効 %.1freq/min / 429=%d",
                    elapsed,
                    request_count,
                    effective_rpm,
                    rate_limit_count,
                )
                last_log = now
    except JQuantsServerError as exc:
        logger.error("ネットワーク/サーバーエラーで中断: %s", exc)
        return 2
    except KeyboardInterrupt:
        logger.warning("中断されました")

    elapsed = time.monotonic() - start
    effective_rpm = request_count / elapsed * 60 if elapsed > 0 else 0
    logger.info(
        "=== 完了: %.1fs sustain / 総 req=%d / 実効 %.1freq/min / 429=%d ===",
        elapsed,
        request_count,
        effective_rpm,
        rate_limit_count,
    )

    return 0 if rate_limit_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
