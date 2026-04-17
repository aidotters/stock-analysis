"""`/research-executives 7203` のキャッシュ動作を実測するスクリプト.

acceptance-test 条件: 30日キャッシュにより連続実行で WebSearch / LLM 呼び出しが 0 回.
"""

from __future__ import annotations

from market_pipeline.executives.communication_collector import CommunicationCollector
from market_pipeline.executives.repository import ExecutiveRepository


class WebSearchCounter:
    """WebSearch 呼び出し回数を計測するスタブ."""

    def __init__(self, canned_results: dict[str, list[dict]]):
        self.call_count = 0
        self.canned = canned_results
        self.queries: list[str] = []

    def __call__(self, query: str) -> list[dict]:
        self.call_count += 1
        self.queries.append(query)
        for name, results in self.canned.items():
            if name in query:
                return results
        return []


def main() -> None:
    repo = ExecutiveRepository()
    repo.initialize_tables()
    company = "トヨタ自動車"
    code = "7203"
    executives = repo.get_executives(code, is_representative=True)
    names = [e.name for e in executives]
    print(f"対象代表: {len(names)}名")
    for n in names:
        print(f"  {n}  cache_valid(30d)={repo.is_cache_valid(n, 30)}")

    canned = {
        name: [
            {
                "url": f"https://example.com/{i}-{name}",
                "title": f"{name}インタビュー {i}",
                "snippet": f"{name}の発信ダミー {i}",
                "published_date": "2026-03-01",
            }
            for i in range(3)
        ]
        for name in names
    }

    print("\n=== 1回目の /research-executives 7203 (キャッシュ無効) ===")
    search1 = WebSearchCounter(canned)
    collector1 = CommunicationCollector(web_search_fn=search1, repository=repo)
    for name in names:
        collector1.collect(name, company, code=code)
    print(f"1回目 WebSearch 呼び出し回数: {search1.call_count}")

    print("\n=== 2回目の /research-executives 7203 (キャッシュ想定ヒット) ===")
    search2 = WebSearchCounter(canned)
    collector2 = CommunicationCollector(web_search_fn=search2, repository=repo)
    for name in names:
        collector2.collect(name, company, code=code)
    print(f"2回目 WebSearch 呼び出し回数: {search2.call_count}")

    print("\n=== 3回目: --force-refresh (必ず再発火) ===")
    search3 = WebSearchCounter(canned)
    collector3 = CommunicationCollector(web_search_fn=search3, repository=repo)
    for name in names:
        collector3.collect(name, company, code=code, force_refresh=True)
    print(f"3回目 WebSearch 呼び出し回数: {search3.call_count}")

    print("\n=== 判定 ===")
    ok = (
        (search1.call_count == len(names))
        and (search2.call_count == 0)
        and (search3.call_count == len(names))
    )
    print(
        f"1回目={search1.call_count} / 2回目={search2.call_count} / 3回目(force)={search3.call_count}"
    )
    print("PASS" if ok else "FAIL")


if __name__ == "__main__":
    main()
