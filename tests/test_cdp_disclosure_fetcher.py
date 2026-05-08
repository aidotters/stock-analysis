"""CdpDisclosureFetcher テスト。

Playwright の Page を Fake オブジェクトで差し替え、HTML→NewsItem 変換と
リトライ動作を検証する。実ブラウザは起動しない。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path


from market_pipeline.news_delivery.exceptions import DisclosureFetchError
from market_pipeline.news_delivery.fetchers.cdp_disclosure_fetcher import (
    CdpDisclosureFetcher,
)


CONFIG = Path("config/news_sources.yaml")


SAMPLE_HTML = """
<html><body>
<div class="newsList">
  <div class="newsList__item">
    <div class="newsList__inner">
      <a href="/viewer-pdf?fileName=1.pdf">
        <div class="newsList__title">2026年5月期 第3四半期 決算短信</div>
      </a>
      <div class="newsList__category-wrap">
        <div class="newsList__stocks">4443 サンサン</div>
        <div class="newsList__date">04/10 15:30</div>
      </div>
    </div>
  </div>
  <div class="newsList__item">
    <div class="newsList__inner">
      <a href="/viewer-pdf?fileName=2.pdf">
        <div class="newsList__title">コーポレート・ガバナンスに関する報告書</div>
      </a>
      <div class="newsList__category-wrap">
        <div class="newsList__stocks">4443 サンサン</div>
        <div class="newsList__date">04/01 09:00</div>
      </div>
    </div>
  </div>
  <div class="newsList__item">
    <div class="newsList__inner">
      <a href="/viewer-pdf?fileName=3.pdf">
        <div class="newsList__title">業績予想の修正に関するお知らせ</div>
      </a>
      <div class="newsList__category-wrap">
        <div class="newsList__stocks">4443 サンサン</div>
        <div class="newsList__date">03/15 16:00</div>
      </div>
    </div>
  </div>
</div>
</body></html>
"""


class FakePage:
    """Playwright Page の最小モック。"""

    def __init__(self, html_by_url: dict[str, str] | str = "", *, fail_first: int = 0):
        self._html_by_url = html_by_url if isinstance(html_by_url, dict) else None
        self._html = html_by_url if isinstance(html_by_url, str) else ""
        self._fail_first = fail_first
        self._goto_calls = 0
        self._last_url = ""

    def goto(self, url: str, wait_until: str = "load", timeout: int = 30000) -> None:
        self._goto_calls += 1
        if self._goto_calls <= self._fail_first:
            raise RuntimeError(f"simulated nav failure #{self._goto_calls}")
        self._last_url = url

    def wait_for_selector(self, selector: str, timeout: int = 5000) -> None:
        return None

    def content(self) -> str:
        if self._html_by_url is not None:
            for k, v in self._html_by_url.items():
                if k in self._last_url:
                    return v
            return ""
        return self._html

    def close(self) -> None:
        pass


def _build_fetcher(page: FakePage, **kwargs) -> CdpDisclosureFetcher:
    return CdpDisclosureFetcher(
        config_path=CONFIG,
        page_factory=lambda: page,
        sleep_fn=lambda _s: None,
        **kwargs,
    )


def test_parse_extracts_items_and_skips_excluded(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _build_fetcher(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])

    assert errors == {}
    titles = [i.title for i in items]
    # ガバナンス報告書は exclude フィルタで除外される
    assert "コーポレート・ガバナンスに関する報告書" not in titles
    assert "2026年5月期 第3四半期 決算短信" in titles
    assert "業績予想の修正に関するお知らせ" in titles


def test_parse_url_is_absolute(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _build_fetcher(page, profile_dir=tmp_path)
    items = fetcher.fetch_for_codes(["4443"])
    assert items
    for it in items:
        assert it.url.startswith("https://shikiho.toyokeizai.net")


def test_parse_published_at_inferred(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _build_fetcher(page, profile_dir=tmp_path)
    items = fetcher.fetch_for_codes(["4443"])
    for it in items:
        assert isinstance(it.published_at, datetime)


def test_retry_then_success(tmp_path):
    page = FakePage(SAMPLE_HTML, fail_first=2)
    fetcher = _build_fetcher(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])
    assert errors == {}
    assert items, "retry 成功時は items が返る"


def test_retry_exhausted_records_error(tmp_path):
    page = FakePage(SAMPLE_HTML, fail_first=10)
    fetcher = _build_fetcher(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])
    assert items == []
    assert "4443" in errors
    assert isinstance(errors["4443"], DisclosureFetchError)


def test_multi_code_isolation(tmp_path):
    page = FakePage({"qtext=7203": SAMPLE_HTML, "qtext=4443": ""})
    fetcher = _build_fetcher(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["7203", "4443"])
    # 7203 はサンプル4443を返すが、stocks セルの 4443 と一致しないため空
    # 4443 は空HTMLで0件
    # → 両方とも0件で例外なし
    assert errors == {}
    assert all(it.code in {"7203", "4443"} for it in items)


def test_lookback_filter_excludes_old_items(tmp_path):
    # 「現在」を 04/15 12:00 と固定。lookback=7 なら 04/08 以降のみ通る。
    fixed_now = datetime(datetime.now().year, 4, 15, 12, 0, 0)
    page = FakePage(SAMPLE_HTML)
    fetcher = CdpDisclosureFetcher(
        config_path=CONFIG,
        page_factory=lambda: page,
        sleep_fn=lambda _s: None,
        profile_dir=tmp_path,
        lookback_days=7,
        now_fn=lambda: fixed_now,
    )
    items = fetcher.fetch_for_codes(["4443"])
    titles = [i.title for i in items]
    # 04/10 のみ残る (03/15 と 04/01 は範囲外)。ガバナンスは exclude で除外
    assert "2026年5月期 第3四半期 決算短信" in titles
    assert "業績予想の修正に関するお知らせ" not in titles


def test_lookback_none_disables_filter(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = CdpDisclosureFetcher(
        config_path=CONFIG,
        page_factory=lambda: page,
        sleep_fn=lambda _s: None,
        profile_dir=tmp_path,
        lookback_days=None,
    )
    items = fetcher.fetch_for_codes(["4443"])
    # 全期間対象 (ガバナンスのみ exclude)
    assert len(items) == 2


def test_empty_codes_returns_empty(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _build_fetcher(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors([])
    assert items == []
    assert errors == {}
