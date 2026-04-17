"""EDINET 書類ID (doc_id) 解決の高速化モジュール.

Phase 0 PoC 確定事項:
- 初回: 直近 `doc_scan_fallback_months` ヶ月（既定18ヶ月）を日次スキャン
- 2回目以降: 前回の `edinet_source_doc_id` が記録済みなら、期末月前後 `doc_scan_narrow_days` 日
  （既定30日）のみスキャン。見つからない場合はフルスキャンへフォールバック
- 書類一覧APIのレスポンスはメモリキャッシュ（同一日付を複数銘柄が問い合わせても1回で済ませる）
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Optional

import requests

from market_pipeline.config import get_settings
from market_pipeline.executives.edinet_executive_fetcher import (
    ASR_FORM_CODE,
    ASR_ORDINANCE_CODE,
)
from market_pipeline.executives.exceptions import EdinetFetchError
from market_pipeline.executives.repository import ExecutiveRepository

logger = logging.getLogger(__name__)


class EdinetDocResolver:
    """銘柄コードから最新有価証券報告書の doc_id を解決する.

    1日分の書類一覧はメモリキャッシュし、同一バッチ内で複数銘柄を処理する際の
    API呼び出しを最小化する。
    """

    def __init__(
        self,
        repository: Optional[ExecutiveRepository] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_list: Optional[int] = None,
        fallback_months: Optional[int] = None,
        narrow_days: Optional[int] = None,
        request_delay: float = 0.05,
    ) -> None:
        settings = get_settings()
        self._repo = repository or ExecutiveRepository()
        self._api_key = api_key or settings.edinet.api_key
        self._base_url = base_url or settings.edinet.base_url
        self._timeout_list = (
            timeout_list if timeout_list is not None else settings.edinet.timeout_list
        )
        self._fallback_months = (
            fallback_months
            if fallback_months is not None
            else settings.executives.doc_scan_fallback_months
        )
        self._narrow_days = (
            narrow_days
            if narrow_days is not None
            else settings.executives.doc_scan_narrow_days
        )
        self._request_delay = request_delay
        self._list_cache: dict[date, list[dict]] = {}

    def resolve(
        self,
        code4: str,
        *,
        fiscal_year_end_month: Optional[int] = None,
        today: Optional[date] = None,
    ) -> Optional[str]:
        """銘柄コードから最新有価証券報告書の doc_id を返す.

        Args:
            code4: 4桁銘柄コード
            fiscal_year_end_month: 決算月 (1-12)。不明時は None。
                指定されると narrow スキャンの対象月を絞り込める
            today: 起点日（テスト時に固定するためのフック）

        Returns:
            doc_id。見つからない場合は None
        """
        if not self._api_key:
            raise EdinetFetchError("EDINET_API_KEY が設定されていません")

        today = today or date.today()
        sec_code5 = f"{code4}0"

        # 1) doc_idキャッシュ（前回実行時の値）が存在すれば narrow スキャン
        cached_doc_id = self._repo.get_latest_doc_id(code4)
        if cached_doc_id:
            doc_id = self._narrow_scan(
                sec_code5,
                today=today,
                fiscal_year_end_month=fiscal_year_end_month,
            )
            if doc_id:
                return doc_id
            logger.info(
                "narrowスキャンで新規doc_idなし code=%s 前回=%s",
                code4,
                cached_doc_id,
            )
            # 新規なければ前回のdoc_idをそのまま返す（既存データの維持）
            return cached_doc_id

        # 2) フルスキャンフォールバック（初回 or キャッシュミス）
        return self._full_scan(sec_code5, today=today)

    def _narrow_scan(
        self,
        sec_code5: str,
        *,
        today: date,
        fiscal_year_end_month: Optional[int],
    ) -> Optional[str]:
        """期末月前後 narrow_days 日のみスキャン."""
        target_months: list[date] = []
        if fiscal_year_end_month is not None:
            # 決算月の翌々月〜3ヶ月後が提出期限（例: 3月決算→6月末）
            # 直近1年以内の期末月をアンカーにする
            anchor = date(today.year, fiscal_year_end_month, 1)
            if anchor > today:
                anchor = anchor.replace(year=anchor.year - 1)
            # 提出期限の中心: 期末+3ヶ月後あたり
            month = anchor.month + 3
            year = anchor.year + (1 if month > 12 else 0)
            month = ((month - 1) % 12) + 1
            target_months.append(date(year, month, 15))
        else:
            # 決算月不明時: 直近3ヶ月の中日をアンカー
            for offset in range(0, 3):
                year = today.year
                month = today.month - offset
                if month < 1:
                    month += 12
                    year -= 1
                target_months.append(date(year, month, 15))

        half = self._narrow_days // 2
        for anchor in target_months:
            start = anchor - timedelta(days=half)
            end = anchor + timedelta(days=half)
            doc_id = self._scan_range(sec_code5, start=start, end=end)
            if doc_id:
                return doc_id
        return None

    def _full_scan(self, sec_code5: str, *, today: date) -> Optional[str]:
        """直近 fallback_months ヶ月を日次スキャン."""
        end = today
        start = today - timedelta(days=self._fallback_months * 31)
        return self._scan_range(sec_code5, start=start, end=end)

    def _scan_range(self, sec_code5: str, *, start: date, end: date) -> Optional[str]:
        """日付範囲の書類一覧をスキャンし、該当する有価証券報告書 doc_id を返す."""
        cursor = end
        while cursor >= start:
            documents = self._get_documents_for_date(cursor)
            for entry in documents:
                if (
                    entry.get("secCode") == sec_code5
                    and entry.get("ordinanceCode") == ASR_ORDINANCE_CODE
                    and entry.get("formCode") == ASR_FORM_CODE
                ):
                    return entry.get("docID")
            cursor -= timedelta(days=1)
        return None

    def _get_documents_for_date(self, target_date: date) -> list[dict]:
        """指定日付の書類一覧を取得（日付キャッシュ付き）."""
        if target_date in self._list_cache:
            return self._list_cache[target_date]

        time.sleep(self._request_delay)
        resp = requests.get(
            f"{self._base_url}/documents.json",
            params={
                "date": target_date.strftime("%Y-%m-%d"),
                "type": "2",
                "Subscription-Key": self._api_key,
            },
            timeout=self._timeout_list,
        )
        if resp.status_code == 404:
            self._list_cache[target_date] = []
            return []
        if resp.status_code != 200:
            logger.warning(
                "EDINET documents list HTTP %d for %s",
                resp.status_code,
                target_date,
            )
            self._list_cache[target_date] = []
            return []

        data = resp.json()
        results = data.get("results") or []
        self._list_cache[target_date] = results
        return results

    def clear_cache(self) -> None:
        """インメモリキャッシュをクリア（長期バッチで初期化したい場合に使用）."""
        self._list_cache.clear()
