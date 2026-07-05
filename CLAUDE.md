# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Japanese stock market data collection and analysis system using J-Quants API. Collects daily prices, financial statements, and master data into SQLite databases, then runs various analysis strategies (Minervini, HL ratio, relative strength, chart pattern classification). yfinance バリュエーション指標、EDINET 有報ベースの経営陣6軸評価、ウォッチリストのニュースSlack配信、Notion へのレポート投入も統合している。

## ドキュメント索引（詳細は必ずこちらを参照）

- `docs/core/architecture.md`: アーキテクチャ設計書（システム全体像・レイヤー構成・DB設計・設計原則）
- `docs/core/repo-structure.md`: リポジトリ構造・ファイル一覧（テストファイル一覧含む）
- `docs/core/api-reference.md`: モジュール別API仕様（config / jquants / yfinance / executives / news_delivery / market_reader / technical_tools / notion_export / scripts の全クラス・関数・環境変数）
- `docs/core/dev-guidelines.md`: 開発ガイドライン（コーディング規約・DB/並列/非同期/キャッシュパターン・コミット規約・チェックリスト）
- `docs/core/skills.md`: Claude Code スキルガイド（全スキル索引と `/discover-stocks` `/analyze-stock` `/research-stock-news` `/research-executives` `/watch` `/sync-notion` の詳細）
- `docs/core/launchd-operations.md`: launchd運用ガイド（ジョブ一覧・plist登録・チェーン実行・トラブルシューティング）
- `docs/core/diagrams.md`: データフロー図・コンポーネント図（Mermaid）
- `docs/core/CHANGELOG.md`: 変更履歴（V2移行・レート制限調整等の経緯と実機検証記録）

## Commands

### Tests / Lint
```bash
pytest                          # 全テスト
pytest tests/test_minervini.py  # 個別ファイル
pytest -v

ruff check .
black .
mypy .
```

### Daily Operations (launchd-scheduled)
```bash
# Daily pipeline: J-Quants取得 → Daily Analysis → Integrated Analysis (平日18:00, チェーン実行)
python scripts/run_daily_jquants.py            # --no-chain で取得のみ
python scripts/run_daily_analysis.py           # --no-chain / --modules hl_ratio rsp 等
python scripts/run_adhoc_integrated_analysis.py

# Weekly (土曜06:00): 財務諸表 + 統合分析
python scripts/run_weekly_tasks.py             # --statements-only / --analysis-only

# Monthly (毎月1日20:30): マスターデータ更新
python scripts/run_monthly_master.py

# 役員マスター月次更新（EDINET）
python scripts/run_executive_master_update.py  # --codes 7203 9984 / --limit N / --dry-run

# ウォッチリストCRUD / ニュース配信 (平日 08:00 / 12:30 / 19:30)
python scripts/watchlist.py {add|list|update|remove} ...
python scripts/run_news_delivery.py --slot {morning|noon|evening}  # --dry-run / --lookback-days / --sources
python scripts/cleanup_news_db.py              # delivered_news クリーンアップ（デフォルト90日）
```

### One-time / Migration
```bash
python scripts/create_database_indexes.py      # DBインデックス作成（初回）
python scripts/run_historical_prices.py        # yfinance過去株価（--dry-run / --symbols / --years）
python scripts/migrate_add_source_column.py    # daily_quotesにsourceカラム追加（historical prices前に実行）
python scripts/migrate_refetch_yfinance.py     # yfinance再取得（auto_adjust=False化）
python scripts/migrate_rescale_yfinance.py     # yfinanceをJ-Quants基準にリスケール
python scripts/migrate_executives_add_career_column.py
python scripts/migrate_executives_add_growth_axis.py
python scripts/check_jquants_v2_sustained.py   # レート制限smoke test（--duration / --rate / --dry-run）
```

### Chart Classification
```bash
python src/market_pipeline/analysis/chart_classification.py --mode sample-adaptive   # サンプル実行
python src/market_pipeline/analysis/chart_classification.py --mode full-optimized    # 全銘柄
```

## Architecture

### Data Flow
1. **Price Collection** (scripts/run_daily_jquants.py) -> J-Quants API -> data/jquants.db
2. **Historical Prices** (scripts/run_historical_prices.py) -> yfinance -> data/jquants.db (daily_quotes, source='yfinance') ⚠ 品質問題あり（下記「重要な制約」）
3. **Financial Data** (scripts/run_weekly_tasks.py) -> J-Quants Statements API -> data/statements.db
4. **Analysis** (scripts/run_daily_analysis.py) -> reads jquants.db -> writes to data/analysis_results.db (includes integrated_scores daily)
5. **Integration** (src/market_pipeline/analysis/integrated_analysis2.py) -> reads analysis_results.db + statements.db -> outputs to DB/CSV/Excel

### Key Databases (data/)
- `jquants.db`: Daily stock prices (daily_quotes、sourceカラムで'jquants'/'yfinance'を区別)
- `statements.db`: Financial statements, calculated fundamentals, yfinance_valuation, executives / executive_communications / executive_evaluations
- `analysis_results.db`: minervini, hl_ratio, relative_strength, classification_results, integrated_scores
- `master.db`: Stock master data
- `news_delivery.db`: delivered_news（ニュース配信の重複排除）

### Packages (src/)
- `market_pipeline/jquants/`: J-Quants API V2 連携（`JQuantsClient` + `_v2_translator` アダプタ層、data_processor / statements_processor / fundamentals_calculator）
- `market_pipeline/yfinance/`: `ValuationFetcher`（ネットキャッシュ比率・cash_neutral_per のローリング取得）、`HistoricalPriceFetcher`（最大20年の過去日足）
- `market_pipeline/master/`: `StockMasterDB`（東証銘柄マスター）
- `market_pipeline/analysis/`: minervini / high_low_ratio / relative_strength / chart_classification / integrated_analysis(2) / integrated_scores_repository
- `market_pipeline/executives/`: EDINET有報から法定役員取得 + WebSearch発信収集 + Claude LLM 6軸スコアリング
- `market_pipeline/news/`: ニュース巡回先YAML設定パーサー（config/news_sources.yaml）
- `market_pipeline/news_delivery/`: ウォッチリスト銘柄のニュース取得・重複排除・Slack配信（fetchers: 四季報CDP / Google News RSS / TDnet Atom / 四季報関連記事）
- `market_pipeline/config/`: Pydantic Settings 一元設定（`get_settings()`。paths / jquants / analysis / database / slack / edinet / executives 等）
- `market_pipeline/utils/`: parallel_processor, slack_notifier（`JobContext` コンテキストマネージャで launchd ジョブの成功/エラーをSlack通知）ほか
- `market_reader/`: `DataReader` — pandas_datareader風の株価読み出し（4/5桁コード正規化、strict モード）
- `technical_tools/`: Jupyter向け — `TechnicalAnalyzer`（チャート/指標/クロス検出）、`StockScreener`+`ScreenerFilter`、`Backtester`、`StrategyOptimizer`、`VirtualPortfolio`
- `notion_export/`: `/analyze-stock` レポートの Notion 投入（Markdown→blocks変換、File Upload API、既存ページ自動アーカイブ）

各クラスの使用例・シグネチャ・オプションは `docs/core/api-reference.md` を参照。

### Performance
コードベースは大幅に最適化済み（5時間 → 15-20分）: 並列処理（parallel_processor）、aiohttp非同期API、バッチDB操作、NumPy/Pandasベクトル化、テンプレートキャッシュ、DBインデックス。詳細は `docs/refs/OPTIMIZATION_TECHNIQUES_GUIDE.md` 等。

## 重要な制約・規約

### J-Quants API V2（2026-05-31 V1廃止）
- 認証は `x-api-key` ヘッダ。`.env` に `JQUANTS_API_KEY` を設定（`settings.jquants.api_key`）。
- アダプタ層パターン: `JQuantsClient`（HTTP、トークンバケット式レート制限 デフォルト55req/min、指数バックオフリトライ）+ `_v2_translator`（V2短縮カラム名→V1ロング名 rename）。
- **スコープ外（アダプタ層は恒久的に残す方針）**: DBスキーマ・分析モジュール・`market_reader`/`technical_tools` は **V1カラム名（Open/High/Low/Close/AdjustmentClose等）のまま維持**する。V2カラム名を下流に波及させないこと。
- launchdエントリスクリプトは起動直後に `client.health_check()` を実行、失敗時はSlackエラー通知後 exit 1。

### yfinance 過去株価の既知の品質問題
旧バージョン（auto_adjust=True）で取得済みのyfinance価格は配当+分割の遡及調整済みで、J-QuantsのAdjustmentClose（分割のみ調整）と調整基準が異なる。`migrate_refetch_yfinance.py` / `migrate_rescale_yfinance.py` で修正可能。INSERT OR IGNORE により既存J-Quantsデータは上書きされない。

### 環境変数（`.env` / `.env.example` 参照）
- `JQUANTS_API_KEY`（必須）、`EDINET_API_KEY`（executives系で必須）
- `SLACK_WEBHOOK_URL` / `SLACK_ERROR_WEBHOOK_URL` / `SLACK_ENABLED` ほかSlack設定
- `NOTION_PARENT_PAGE_ID` / `NOTION_API_TOKEN`（/sync-notion）
- `STOCK_NEWS_LOOKBACK_DAYS` / `STOCK_NEWS_QUIET_WHEN_EMPTY` / `STOCK_NEWS_SLACK_WEBHOOK_URL`（ニュース配信）

### Testing
- pytest + `tests/conftest.py` の fixtures。モックDBはメモリ/一時ファイルで分離。
- `pythonpath = ["src", "."]` が pyproject.toml に設定済み（import 用）。
- テストファイル一覧と対応モジュールは `docs/core/repo-structure.md` を参照。

### Claude Code スキル
投資分析（`/discover-stocks` `/analyze-stock` `/research-stock-news` `/research-executives` `/watch` `/sync-notion`）、ドキュメント作成、開発フロー、品質管理の各スキルを提供。索引・使用例・オプション・前提条件（CDP接続等）は **`docs/core/skills.md`** を参照。定義は `.claude/skills/<skill-name>/SKILL.md`。

**表記ルール:** ドキュメント本文ではスキル名は単純表記（例: `/watch`）に統一する。
