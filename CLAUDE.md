# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Japanese stock market data collection and analysis system using J-Quants API. Collects daily prices, financial statements, and master data into SQLite databases, then runs various analysis strategies (Minervini, HL ratio, relative strength, chart pattern classification).

## Commands

### Running Tests
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_minervini.py

# Run tests with verbose output
pytest -v
```

### Daily Operations (launchd-scheduled)
```bash
# Fetch daily stock prices from J-Quants API (weekdays 18:00)
python scripts/run_daily_jquants.py

# Run daily analysis (weekdays 18:30)
python scripts/run_daily_analysis.py

# Run specific analysis modules only
python scripts/run_daily_analysis.py --modules hl_ratio rsp

# Run yfinance valuation rolling update only
python scripts/run_daily_analysis.py --modules yfinance_valuation

# Run adhoc integrated analysis (weekdays 19:00)
python scripts/run_adhoc_integrated_analysis.py

# Weekly tasks: financial statements data + integrated analysis (Saturday 06:00)
python scripts/run_weekly_tasks.py

# Weekly tasks options
python scripts/run_weekly_tasks.py --statements-only  # Fetch financial data only
python scripts/run_weekly_tasks.py --analysis-only    # Run integrated analysis only

# Monthly master data update (1st of month 20:30)
python scripts/run_monthly_master.py
```

### Chart Classification
```bash
# Sample run with adaptive windows
python src/market_pipeline/analysis/chart_classification.py --mode sample-adaptive

# Full optimized analysis for all stocks
python src/market_pipeline/analysis/chart_classification.py --mode full-optimized
```

### Database Setup
```bash
# Create database indexes (run once for performance)
python scripts/create_database_indexes.py
```

### Linting/Formatting
```bash
ruff check .
black .
mypy .
```

## Architecture

### Data Flow
1. **Price Collection** (scripts/run_daily_jquants.py) -> J-Quants API -> data/jquants.db
2. **Financial Data** (scripts/run_weekly_tasks.py) -> J-Quants Statements API -> data/statements.db
3. **Analysis** (scripts/run_daily_analysis.py) -> reads jquants.db -> writes to data/analysis_results.db (includes integrated_scores daily)
4. **Integration** (src/market_pipeline/analysis/integrated_analysis2.py) -> reads analysis_results.db + statements.db -> outputs to DB/CSV/Excel

### Key Databases (data/)
- `jquants.db`: Daily stock prices (daily_quotes table)
- `statements.db`: Financial statements and calculated fundamentals (financial_statements, calculated_fundamentals tables)
- `analysis_results.db`: Analysis outputs (minervini, hl_ratio, relative_strength, classification_results, integrated_scores tables)
- `master.db`: Stock master data

### J-Quants Modules (src/market_pipeline/jquants/)
- `data_processor.py`: Daily price data fetcher with async processing
- `statements_processor.py`: Financial statements API fetcher
- `fundamentals_calculator.py`: Calculates PER, PBR, ROE, ROA, etc. from raw statements

### yfinance Valuation (src/market_pipeline/yfinance/)
- `valuation_fetcher.py`: `ValuationFetcher` class for rolling yfinance BS data collection
  - Fetches cash & equivalents, total debt, market cap, PER from yfinance
  - Calculates net_cash_ratio and cash_neutral_per
  - Rolling update: processes N stocks/day (default 150), prioritizing stale/missing data
  - Data stored in `statements.db` → `yfinance_valuation` table
  - Integrated into `run_daily_analysis.py` as `yfinance_valuation` module

### Master Modules (src/market_pipeline/master/)
- `master_db.py`: `StockMasterDB` class for managing stock master data (TSE listed stocks)
  - Downloads and parses TSE stock list Excel files
  - Manages `stocks_master` table (code, name, sector, market, yfinance_symbol, jquants_code, is_active)
  - Query methods: `get_all_stocks()`, `get_stock_by_code()`, `get_stocks_by_sector()`, `get_stocks_by_market()`, `get_statistics()`

### Analysis Modules (src/market_pipeline/analysis/)
- `minervini.py`: Minervini trend screening strategy
- `high_low_ratio.py`: 52-week high/low position ratio
- `relative_strength.py`: RSP (relative strength percentage) and RSI calculations
- `chart_classification.py`: ML-based chart pattern classification with adaptive window selection (20/60/120/240/960/1200 days)
- `integrated_analysis.py`: Combines analysis results for multi-factor stock screening
- `integrated_analysis2.py`: Outputs integrated analysis to DB, with optional CSV/Excel export
- `integrated_scores_repository.py`: Repository for integrated_scores table CRUD operations

### Performance Optimizations
The codebase has been heavily optimized (5 hours -> 15-20 minutes):
- Parallel processing via `src/market_pipeline/utils/parallel_processor.py`
- Async API calls with aiohttp in `src/market_pipeline/jquants/data_processor.py`
- Batch database operations
- Vectorized calculations with NumPy/Pandas
- Template caching for chart classification
- Database indexes (run `scripts/create_database_indexes.py`)

### Slack Notifications (src/market_pipeline/utils/slack_notifier.py)
launchdスクリプトの実行結果をSlack Incoming Webhookで通知:

```python
from market_pipeline.utils import JobContext

with JobContext("ジョブ名") as job:
    # ジョブ処理
    job.add_metric("レコード数", "1,000")
    job.add_warning("一部データ欠損")
# 正常終了時は成功通知、例外時はエラー通知を自動送信
```

**設定（環境変数）:**
- `SLACK_WEBHOOK_URL`: Webhook URL（未設定時は通知スキップ）
- `SLACK_ERROR_WEBHOOK_URL`: エラー専用チャンネル（オプション）
- `SLACK_ENABLED`: 通知有効/無効（デフォルト: true）
- `SLACK_TIMEOUT_SECONDS`: HTTPタイムアウト（デフォルト: 10秒）
- `SLACK_MAX_RETRIES`: リトライ回数（デフォルト: 3回）

**特徴:**
- 通知失敗がジョブの処理結果に影響しない
- リトライロジック（最大3回、1秒間隔）
- 4つのlaunchdスクリプト全てに統合済み

### News Discovery Skill (`/discover-stocks`)
ニュースや分析記事から有望銘柄を抽出するClaude Codeスキル。Playwright MCPでサイトを巡回し、銘柄コード・推奨理由を抽出、裏付け情報収集とリスク分析を経てレポートを生成する。

```bash
# 基本実行（直近7日間、全カテゴリ）
/discover-stocks

# テーマ絞り込み
/discover-stocks --theme "AI"

# カテゴリ・期間指定
/discover-stocks --category analysis --from 2026-02-20 --to 2026-02-28

# 適時開示のみの巡回
/discover-stocks --category disclosure
```

**構成ファイル:**
- `config/news_sources.yaml`: 巡回先サイト設定（カテゴリ別）
- `.claude/skills/discover-stocks/SKILL.md`: スキル定義
- `src/market_pipeline/news/config_parser.py`: YAML設定パーサー
- `docs/reports/adhoc/`: レポート出力先

**巡回先カテゴリ:**
- `news`: ニュースサイト（日経電子版, Reuters Japan）
- `analysis`: 分析サイト（トウシル, 会社四季報オンライン）
- `disclosure`: 適時開示情報（会社四季報、`filter_keywords`によるフィルタリング）
- `financial`: 個別銘柄ページ（Phase 2用）

**認証方式:**
- `auth: cdp` — Chrome DevTools Protocol経由（要: `open -a 'Google Chrome' --args --remote-debugging-port=9222`）
- `auth: none` — Playwright MCPで直接アクセス

### Stock News Research Skill (`/research-stock-news`)
特定銘柄のニュース・適時開示・IR情報を包括的に調査し、レポートを生成する。

```bash
# 銘柄コード指定
/research-stock-news 4443

# 複数銘柄
/research-stock-news 4443 7203

# 期間指定
/research-stock-news 4443 --from 2026-02-01 --to 2026-02-28
```

**構成ファイル:**
- `.claude/skills/research-stock-news/SKILL.md`: スキル定義
- `docs/reports/stocks/`: レポート出力先（`{code}-news.md`）

**情報ソース:**
- 四季報適時開示ページ（`auth: none`、`?qtext={code}`で銘柄絞り込み）
- 四季報銘柄ページのニュースタブ（CDP経由）
- WebSearchによる企業IR・一般ニュース

### Stock Analysis Skill (`/analyze-stock`)
銘柄コードまたはPhase 1候補リストから、企業分析・財務分析・テクニカル分析を統合した投資判断レポートを生成する。

```bash
# 銘柄コード直接指定
/analyze-stock 7203

# 複数銘柄の一括分析
/analyze-stock 7203 9984

# Phase 1候補リストから全銘柄を分析
/analyze-stock --from-report docs/reports/adhoc/2026-02-28-candidates.md

# Phase 1候補リストから特定銘柄のみ分析
/analyze-stock --from-report docs/reports/adhoc/2026-02-28-candidates.md 7203 9984

# Deep Researchも含めて即実行（確認プロンプトをスキップ）
/analyze-stock 7203 --deep-research

# 既存レポートにDeep Research結果を後から統合
/analyze-stock 7203 --merge-deep-research
```

**構成ファイル:**
- `.claude/skills/analyze-stock/SKILL.md`: スキル定義
- `config/news_sources.yaml`: `financial`カテゴリの銘柄ページ設定
- `output/reports/stocks/`: レポート出力先（タイムスタンプ付きディレクトリ）

**出力ディレクトリ構成:**
```
output/reports/stocks/YYYYMMDD-HHMM-{code}-analysis/
├── base_report.md              # Phase 1レポート
├── deep_research_report.md     # Deep Research結果（--deep-research実行時のみ）
└── chart.png                   # 株価チャート（kaleido利用可能時のみ）
```

**情報ソース:**
- 会社四季報銘柄ページ（CDP経由、フォールバック: WebSearch）
- 企業IR・業界分析・セグメント分析・SWOT分析（gemini CLI、フォールバック: WebSearch）
- 既存テクニカルツール（StockScreener, TechnicalAnalyzer, DataReader）
- Gemini Advanced Deep Research（`--deep-research`オプション時、Playwright MCP + CDP経由）

**Deep Research前提条件:**
- Gemini Advanced有料会員であること
- Chrome が `--remote-debugging-port=9222` で起動中であること（CDP接続）
- Deep Researchは5〜15分の実行時間を要する（タイムアウト: 1500秒）
- Deep Research失敗時もPhase 1レポートは保持される

**レポート内容（8セクション構成）:**
- 1. 企業概要
- 2. 事業構造・セグメント分析（セグメント別売上・利益構成、成長性・競争力、CAGR）
- 3. 財務分析（PER/PBR/ROE等、財務状況、キャッシュフロー、ネットキャッシュ分析、業績推移）
- 4. テクニカル分析（統合スコア/Minervini/RSP、株価チャートPNG）
- 5. 業界・競合分析（業界動向、四季報ライバル比較テーブル、SWOT分析）
- 6. 直近の適時開示・ニュース（`/research-stock-news`相当の情報を自動統合）
- 7. リスク要因
- 8. 投資判断サマリー（5段階評価、セグメント分析・成長性を含む判断根拠）

**チャート生成依存:** `kaleido`（オプショナル）。未インストール時はチャート生成をスキップし、テキストのみのレポートを生成する。

### Document Creation & Quality Assurance Skills
Claude Codeスキルとして、ドキュメント作成と品質管理のためのスキルも提供:

**ドキュメント作成スキル:**
- `/architecture-design`: アーキテクチャ設計書の作成
- `/functional-design`: 機能設計書の作成
- `/development-guidelines`: 開発ガイドラインの作成
- `/repository-structure`: リポジトリ構造定義書の作成
- `/prd-writing`: PRD（製品要件定義書）の作成
- `/glossary-creation`: 用語集の作成

**品質管理スキル:**
- `/steering`: 作業計画・タスクリスト管理（実装フローの全体管理）
- `/validation`: コード品質検証と受け入れテスト

**構成ファイル:** `.claude/skills/<skill-name>/SKILL.md`

### Technical Tools Package (src/technical_tools/)
Jupyter Notebook用のテクニカル分析ツール。日本株(J-Quants)と米国株(yfinance)の統一インターフェースを提供:

```python
from technical_tools import TechnicalAnalyzer

# 日本株（J-Quants）
analyzer = TechnicalAnalyzer(source="jquants")
fig = analyzer.plot_chart("7203", show_sma=[25, 75], show_rsi=True, show_macd=True)
fig.show()

# 米国株（yfinance）
analyzer = TechnicalAnalyzer(source="yfinance")
fig = analyzer.plot_chart("AAPL", show_sma=[50, 200], show_bb=True, period="1y")
fig.show()

# クロスシグナル検出
signals = analyzer.detect_crosses("7203", patterns=[(5, 25), (25, 75)])

# 既存分析結果との連携
existing = analyzer.load_existing_analysis("7203")
```

**機能:**
- データソース統一（J-Quants via market_reader, yfinance）
- 株式分割考慮済みの調整後価格を使用（AdjustmentOpen/High/Low/Close/Volume）
- テクニカル指標計算（SMA, EMA, RSI, MACD, Bollinger Bands）
- ゴールデンクロス/デッドクロス自動検出
- plotlyによるインタラクティブチャート
- 既存分析結果（Minervini, RSP）との連携

### StockScreener (src/technical_tools/screener.py)
Jupyter Notebook用の銘柄スクリーニングツール。統合分析結果をDBから取得し、柔軟にフィルタリング:

```python
from technical_tools import StockScreener, ScreenerFilter

screener = StockScreener()

# テクニカル指標でフィルタリング
results = screener.filter(
    composite_score_min=70.0,
    hl_ratio_min=80.0,
    rsi_max=70.0
)

# 財務指標と組み合わせ
results = screener.filter(
    composite_score_min=70.0,
    market_cap_min=100000000000,  # 1000億円以上
    per_max=15.0,
    roe_min=15.0,
    equity_ratio_min=40.0,       # 自己資本比率40%以上
    roa_min=5.0,                 # ROA 5%以上
)

# ScreenerFilterオブジェクトを使用（パラメータの構造化）
config = ScreenerFilter(
    composite_score_min=70.0,
    market_cap_min=100_000_000_000,
    per_max=15.0,
)
results = screener.filter(config)

# バリュエーション指標でフィルタリング（yfinance_valuation連携）
results = screener.filter(
    net_cash_ratio_min=0.3,
    cash_neutral_per_max=10.0,
    composite_score_min=70.0,
)

# includeでカラムグループを追加（フィルタ未使用でもグループ全カラムを返却）
results = screener.filter(include=["fundamentals"])              # 基本5 + fundamentals全6カラム
results = screener.filter(composite_score_min=70.0, include=["fundamentals"])  # + composite_score
results = screener.filter(include=["fundamentals", "valuation"]) # 複数グループ
results = screener.filter(include="all")                         # 全22カラム

# チャートパターンでフィルタリング
results = screener.filter(
    pattern_window=60,
    pattern_labels=["上昇", "急上昇"]
)

# 順位変動が大きい銘柄を取得
movers = screener.rank_changes(days=7, direction="up", min_change=50)

# 特定銘柄の履歴
history = screener.history("7203", days=30)
```

**機能:**
- 統合スコア（composite_score）と順位の日次蓄積
- テクニカル指標（hl_ratio, rsi）でのフィルタリング
- 財務指標（時価総額、PER、PBR、ROE、ROA、自己資本比率、配当利回り）でのフィルタリング
- バリュエーション指標（net_cash_ratio, cash_neutral_per）でのフィルタリング（yfinance_valuation連携）
- `include`パラメータによるカラムグループ制御（"scores", "fundamentals", "valuation", "all"）
- デフォルトでフィルタ使用項目のみ返却（常時5カラム: date, code, long_name, sector, market_cap）
- 出力カラム名は全てsnake_case（例: trailing_pe, return_on_equity, hl_ratio, rsp, rsi）
- market_capはyfinance_valuation優先のCOALESCE（フォールバック: calculated_fundamentals）
- チャートパターン（60日/120日など）でのフィルタリング
- 順位変動分析（rank_changes）：metricバリデーション対応
- 銘柄別時系列データ取得（history）
- ScreenerFilterクラスによる構造化されたパラメータ指定（`available_filters()`, `available_categories()`, `filters_by_category()` classmethodで利用可能フィルタを確認可能）
- TechnicalAnalyzerとのシームレスな連携

### Backtester (src/technical_tools/backtester.py)
シグナルベースのバックテストを実行し、投資戦略の有効性を評価:

```python
from technical_tools import Backtester

bt = Backtester(cash=1_000_000)

# シグナル追加
bt.add_signal("golden_cross", short=5, long=25)
bt.add_signal("rsi_oversold", threshold=30)

# エグジットルール追加
bt.add_exit_rule("stop_loss", threshold=-0.10)
bt.add_exit_rule("take_profit", threshold=0.20)

# バックテスト実行
results = bt.run(symbols=["7203", "9984"], start="2023-01-01", end="2024-12-31")

# 結果確認
print(results.summary())  # 勝率、平均リターン、シャープレシオ等
results.plot().show()     # 資産推移チャート
trades_df = results.trades()  # 個別取引一覧
```

**対応シグナル:**
- `golden_cross`: ゴールデンクロス（短期MAが長期MAを上抜け）
- `dead_cross`: デッドクロス（短期MAが長期MAを下抜け）
- `rsi_oversold`: RSI売られすぎ（RSIがthreshold以下）
- `rsi_overbought`: RSI買われすぎ（RSIがthreshold以上）
- `macd_cross`: MACDクロス（MACD線がシグナル線を上抜け）
- `bollinger_breakout`: ボリンジャーバンドブレイクアウト（価格がバンドを突破）
- `bollinger_squeeze`: ボリンジャースクイーズ（バンド収縮後の拡大）
- `volume_spike`: 出来高急増（出来高が移動平均のN倍超）
- `volume_breakout`: 出来高確認付きブレイクアウト（高値更新+出来高増）

**対応ルール:**
- `stop_loss`: 損切り（threshold: 負の値、例: -0.10）
- `take_profit`: 利確（threshold: 正の値、例: 0.20）
- `max_holding_days`: 最大保有日数
- `trailing_stop`: トレーリングストップ

**StockScreener連携バックテスト:**
```python
# スクリーナー条件でバックテスト
results = bt.run_with_screener(
    screener_filter={"composite_score_min": 70, "hl_ratio_min": 80},
    start="2023-01-01",
    end="2024-12-31",
    exit_rules={"stop_loss": -0.10, "take_profit": 0.20}
)
```

**レポート出力:**
```python
results.export("report.xlsx")  # Excel出力（Summary, Trades, By Symbol, Monthly Returns シート）
results.export("report.csv")   # CSV出力
results.export("report.html")  # HTML出力

# 詳細分析
results.by_symbol()        # 銘柄別パフォーマンス
results.by_sector(map)     # セクター別パフォーマンス（セクターマップ必要）
results.monthly_returns()  # 月次リターン
results.yearly_returns()   # 年次リターン
```

### StrategyOptimizer (src/technical_tools/optimizer.py)
投資戦略のパラメータを自動最適化し、最適な戦略を発見:

```python
from technical_tools import StrategyOptimizer

optimizer = StrategyOptimizer(cash=1_000_000)

# 探索空間の定義
optimizer.add_search_space("ma_short", [5, 10, 20, 25])
optimizer.add_search_space("ma_long", [50, 75, 100, 200])
optimizer.add_search_space("stop_loss", [-0.05, -0.10, -0.15])

# 制約条件の追加
optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])

# グリッドサーチで最適化
results = optimizer.run(
    symbols=["7203", "9984"],
    start="2023-01-01",
    end="2024-12-31",
    method="grid",        # "grid" or "random"
    metric="sharpe_ratio" # 最適化対象指標
)

# 結果分析
best = results.best()           # 最良の戦略
print(best.params)              # {'ma_short': 10, 'ma_long': 75, 'stop_loss': -0.10}
print(best.metrics)             # {'sharpe_ratio': 1.5, 'win_rate': 0.6, ...}

top10 = results.top(10)         # 上位10件をDataFrameで取得

# 可視化
fig = results.plot_heatmap("ma_short", "ma_long", metric="sharpe_ratio")
fig.show()

# 結果の保存・読み込み
results.save("optimization_results.json")
loaded = OptimizationResults.load("optimization_results.json")
```

**探索手法:**
- `grid`: グリッドサーチ（全組み合わせ探索）
- `random`: ランダムサーチ（n_trials回のサンプリング）

**対応パラメータ:**
- MAクロス: `ma_short`, `ma_long`
- RSI: `rsi_threshold`
- MACD: `macd_fast`, `macd_slow`, `macd_signal`
- エグジット: `stop_loss`, `take_profit`

**評価指標:**
- `total_return`: トータルリターン
- `sharpe_ratio`: シャープレシオ
- `max_drawdown`: 最大ドローダウン（最小化）
- `win_rate`: 勝率
- `profit_factor`: プロフィットファクター

**複合評価（重み付け）:**
```python
results = optimizer.run(
    ...,
    metric={
        "sharpe_ratio": 0.5,
        "max_drawdown": 0.3,
        "win_rate": 0.2
    }
)
```

**ウォークフォワード分析（過学習対策）:**
```python
results = optimizer.run(
    ...,
    validation="walk_forward",
    train_ratio=0.7,
    n_splits=5
)
print(results.best().oos_metrics)  # アウトオブサンプル評価
```

**タイムアウト設定:**
```python
from technical_tools import OptimizationTimeoutError

try:
    results = optimizer.run(
        ...,
        timeout=60.0  # 60秒でタイムアウト
    )
except OptimizationTimeoutError as e:
    print(f"タイムアウト: {e.completed}/{e.total}件完了")
```

**ストリーミング保存（大量試行時のメモリ効率化）:**
```python
# 試行結果を逐次JSONL形式で保存
results = optimizer.run(
    ...,
    streaming_output="results.jsonl"
)

# JONLファイルから結果を読み込み
from technical_tools import OptimizationResults
loaded = OptimizationResults.load_streaming("results.jsonl", metric="sharpe_ratio")
```

### VirtualPortfolio (src/technical_tools/virtual_portfolio.py)
仮想ポートフォリオを作成し、パフォーマンスを追跡:

```python
from technical_tools import VirtualPortfolio

# ポートフォリオ作成（data/portfolios/に永続化）
vp = VirtualPortfolio("my_strategy_2025")

# 銘柄購入
vp.buy("7203", shares=100, price=2500)  # 株数指定
vp.buy("9984", amount=500000)           # 金額指定（現在価格で株数計算）

# サマリー確認
print(vp.summary())  # 投資額、評価額、損益、リターン率

# 保有銘柄一覧
holdings = vp.holdings()  # DataFrame

# パフォーマンス推移
perf = vp.performance(days=30)  # 日次評価額推移

# チャート表示
vp.plot().show()

# 売却
vp.sell("7203", shares=50)  # 一部売却
vp.sell_all("9984")         # 全売却
```

**スクリーナー連携:**
```python
# スクリーナー結果から一括購入
vp.buy_from_screener(
    screener_filter={"composite_score_min": 80},
    amount_per_stock=100000,  # 各銘柄10万円
    max_stocks=10
)

# ScreenerFilterオブジェクトも使用可能
from technical_tools import ScreenerFilter
config = ScreenerFilter(composite_score_min=80, hl_ratio_min=75)
vp.buy_from_screener(screener_filter=config)
```

**機能:**
- JSON永続化（data/portfolios/*.json）
- 平均取得単価の自動計算
- スクリーナー結果からの一括銘柄追加
- 現在価格はmarket_readerから自動取得
- 取引履歴の記録
- plotlyによるインタラクティブチャート

### Market Reader Package (src/market_reader/)
pandas_datareader-like interface for accessing J-Quants price data:

```python
from market_reader import DataReader

reader = DataReader()  # Uses default DB path from settings
# Or with explicit path and strict mode
reader = DataReader(db_path="data/jquants.db", strict=True)

# Single stock (returns DataFrame with Date index)
df = reader.get_prices("7203", start="2024-01-01", end="2024-12-31")

# Multiple stocks (returns MultiIndex DataFrame with (Date, Code) index)
df = reader.get_prices(["7203", "9984"], start="2024-01-01", end="2024-12-31")

# Column selection: "simple" (default), "full", or list
df = reader.get_prices("7203", columns=["Open", "Close"])
```

**機能:**
- Automatic date defaults (end=latest in DB, start=5 years before end)
- 4/5-digit code normalization (output always 4-digit)
- `strict=True` raises exceptions, `strict=False` (default) returns empty DataFrame with warning
- PRAGMA optimizations for read performance (WAL mode, cache settings)

### Configuration (src/market_pipeline/config/)
Centralized Pydantic Settings-based configuration system:

```python
from market_pipeline.config import get_settings, reload_settings

settings = get_settings()
db_path = settings.paths.jquants_db
statements_db = settings.paths.statements_db

# 設定をキャッシュクリアして再読み込み
settings = reload_settings()
```

**Configuration categories:**
- `settings.paths`: Database and directory paths (jquants_db, statements_db, analysis_db, etc.)
- `settings.jquants`: J-Quants API settings (rate limits, batch size)
- `settings.analysis`: Technical analysis parameters (SMA periods, thresholds)
- `settings.database`: SQLite PRAGMA settings
- `settings.slack`: Slack notification settings (webhook_url, enabled, timeout, retries)
- `settings.yfinance`: yfinance API settings (legacy)
- `settings.logging`: Logging configuration (level, format)

**Environment variables:** See `.env.example` for all options. Key settings:
- `EMAIL`, `PASSWORD`: J-Quants API credentials (required)

## Testing
- Tests use pytest with fixtures defined in `tests/conftest.py`
- Mock databases are created in memory/temp files for isolation
- `pythonpath = ["src", "."]` is set in pyproject.toml for imports
- Key test files:
  - `tests/test_minervini.py`: Minervini分析テスト
  - `tests/test_high_low_ratio.py`: HL比率計算テスト
  - `tests/test_relative_strength.py`: RSP/RSI計算テスト
  - `tests/test_chart_classification.py`: チャートパターン分類テスト
  - `tests/test_integrated_analysis.py`: 統合分析テスト
  - `tests/test_integrated_scores.py`: IntegratedScoresRepositoryテスト
  - `tests/test_stock_screener.py`: StockScreenerクラステスト
  - `tests/test_jquants_data_processor.py`: J-Quants APIテスト
  - `tests/test_statements_processor.py`: Statements API processor tests
  - `tests/test_fundamentals_calculator.py`: Financial metric calculation tests
  - `tests/test_stock_reader.py`: market_readerパッケージテスト（DataReaderクラス）
  - `tests/test_technical_tools.py`: technical_toolsパッケージテスト（TechnicalAnalyzerクラス）
  - `tests/test_backtester.py`: Backtesterクラステスト
  - `tests/test_backtest_results.py`: BacktestResultsクラステスト
  - `tests/test_backtest_signals.py`: バックテストシグナルテスト
  - `tests/test_virtual_portfolio.py`: VirtualPortfolioクラステスト
  - `tests/test_optimizer.py`: StrategyOptimizerクラステスト
  - `tests/test_optimization_results.py`: OptimizationResultsクラステスト
  - `tests/test_slack_notifier.py`: SlackNotifier/JobContext/JobResultテスト
  - `tests/test_news_config.py`: ニュース巡回先設定パーサーテスト
  - `tests/test_analysis_integration.py`: 分析統合テスト
  - `tests/test_data_processor.py`: データプロセッサテスト
  - `tests/test_valuation_fetcher.py`: ValuationFetcherテスト（yfinance BS取得・バリュエーション計算）
