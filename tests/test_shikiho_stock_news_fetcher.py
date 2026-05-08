"""ShikihoStockNewsFetcher テスト。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path


from market_pipeline.news_delivery.exceptions import DisclosureFetchError
from market_pipeline.news_delivery.fetchers.shikiho_stock_news_fetcher import (
    ShikihoStockNewsFetcher,
)


CONFIG = Path("config/news_sources.yaml")


SAMPLE_HTML = """
<html><body>

<!-- 採用対象セクション -->
<div class="news">
  <div class="news__title">この銘柄の関連記事</div>
  <div class="news__body">
    <div class="news__body__item">
      <a href="/news/8/384261">Sansan、調整後営業利益は過去最高益を達成</a>
      <div class="news__body__item__right">
        <div>Sansan、調整後営業利益は過去最高益を達成</div>
        <div>2026/04/14 17:00</div>
        <div>ログミーファイナンス</div>
      </div>
    </div>
    <div class="news__body__item">
      <a href="/news/0/941114">4月10日に業績・配当予想を修正した会社はこちら</a>
      <div class="news__body__item__right">
        <div>4月10日に業績・配当予想を修正した会社はこちら</div>
        <div>2026/04/10 16:30</div>
        <div>サプライズ決算</div>
      </div>
    </div>
    <div class="news__body__item">
      <a href="/news/0/935528">古い記事</a>
      <div class="news__body__item__right">
        <div>古い記事</div>
        <div>2026/01/01 09:00</div>
        <div>お宝銘柄</div>
      </div>
    </div>
    <div class="news__bottom">もっと見る</div>
  </div>
</div>

<!-- 除外対象セクション -->
<div class="news">
  <div class="news__title">東洋経済オンライン掲載記事</div>
  <div class="news__body">
    <div class="news__body__item">
      <a href="https://toyokeizai.net/articles/-/640153">「伸び盛り」の中小型成長銘柄ランキングTOP50</a>
      <div class="news__body__item__right">
        <div>「伸び盛り」の中小型成長銘柄ランキングTOP50</div>
        <div>2022/12/17 07:30</div>
      </div>
    </div>
  </div>
</div>

</body></html>
"""

EMPTY_HTML = "<html><body><div>nothing</div></body></html>"


class FakePage:
    def __init__(self, html: str = SAMPLE_HTML, *, fail_first: int = 0):
        self._html = html
        self._fail_first = fail_first
        self._goto_calls = 0
        self.last_url = ""

    def goto(self, url, wait_until="load", timeout=30000):
        self._goto_calls += 1
        if self._goto_calls <= self._fail_first:
            raise RuntimeError(f"simulated nav failure #{self._goto_calls}")
        self.last_url = url

    def wait_for_selector(self, selector, timeout=5000):
        return None

    def content(self) -> str:
        return self._html

    def close(self):
        pass


def _make(page: FakePage, **kw) -> ShikihoStockNewsFetcher:
    return ShikihoStockNewsFetcher(
        config_path=CONFIG,
        page_factory=lambda: page,
        sleep_fn=lambda _s: None,
        **kw,
    )


def test_extracts_only_target_section(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _make(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])
    assert errors == {}
    titles = [i.title for i in items]
    # 採用セクションの3件
    assert "Sansan、調整後営業利益は過去最高益を達成" in titles
    assert "4月10日に業績・配当予想を修正した会社はこちら" in titles
    assert "古い記事" in titles
    # 「東洋経済オンライン掲載記事」セクションは含まれない
    assert "「伸び盛り」の中小型成長銘柄ランキングTOP50" not in titles


def test_url_is_made_absolute(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _make(page, profile_dir=tmp_path)
    items = fetcher.fetch_for_codes(["4443"])
    assert items
    for it in items:
        assert it.url.startswith("https://shikiho.toyokeizai.net/")


def test_published_at_parsed(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _make(page, profile_dir=tmp_path)
    items = fetcher.fetch_for_codes(["4443"])
    by_title = {i.title: i for i in items}
    assert by_title[
        "Sansan、調整後営業利益は過去最高益を達成"
    ].published_at == datetime(2026, 4, 14, 17, 0)
    assert by_title[
        "4月10日に業績・配当予想を修正した会社はこちら"
    ].published_at == datetime(2026, 4, 10, 16, 30)


def test_lookback_filter_excludes_old(tmp_path):
    fixed_now = datetime(2026, 5, 1, 12, 0, 0)
    page = FakePage(SAMPLE_HTML)
    fetcher = ShikihoStockNewsFetcher(
        config_path=CONFIG,
        page_factory=lambda: page,
        sleep_fn=lambda _s: None,
        profile_dir=tmp_path,
        lookback_days=30,
        now_fn=lambda: fixed_now,
    )
    items = fetcher.fetch_for_codes(["4443"])
    titles = [i.title for i in items]
    # 04/14 と 04/10 は含まれる、01/01 は除外
    assert "Sansan、調整後営業利益は過去最高益を達成" in titles
    assert "4月10日に業績・配当予想を修正した会社はこちら" in titles
    assert "古い記事" not in titles


def test_target_section_missing_returns_empty(tmp_path):
    page = FakePage(EMPTY_HTML)
    fetcher = _make(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])
    assert items == []
    assert errors == {}


def test_retry_then_success(tmp_path):
    page = FakePage(SAMPLE_HTML, fail_first=2)
    fetcher = _make(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])
    assert errors == {}
    assert items, "リトライ成功時は items が返る"


def test_retry_exhausted_records_error(tmp_path):
    page = FakePage(SAMPLE_HTML, fail_first=10)
    fetcher = _make(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])
    assert items == []
    assert "4443" in errors
    assert isinstance(errors["4443"], DisclosureFetchError)


def test_summary_carries_category_label(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _make(page, profile_dir=tmp_path)
    items = fetcher.fetch_for_codes(["4443"])
    by_title = {i.title: i for i in items}
    assert (
        by_title["Sansan、調整後営業利益は過去最高益を達成"].summary
        == "ログミーファイナンス"
    )


def test_empty_codes_returns_empty(tmp_path):
    page = FakePage(SAMPLE_HTML)
    fetcher = _make(page, profile_dir=tmp_path)
    items, errors = fetcher.fetch_for_codes_with_errors([])
    assert items == []
    assert errors == {}
