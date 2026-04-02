# アーキテクチャ設計書

## 概要

Stock-Analysisは、日本株式市場データの自動収集・分析システムです。J-Quants APIを利用して日次株価、財務諸表、マスターデータを収集し、複数の分析戦略（Minervini、HL比率、相対力、チャートパターン分類）を実行します。

## システム全体像

```text
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                            外部データソース                                             │
├───────────────────────┬─────────────────────────┬──────────────────┬──────────────────┤
│    J-Quants API       │  J-Quants Statements    │  Master Data API │  yfinance API    │
│   (日次株価四本値)      │  (財務諸表データ)         │  (銘柄マスター)   │ (BS・時価総額)    │
└───────────┬───────────┴────────────┬────────────┴────────┬─────────┴──────┬───────────┘
            │                        │                          │
            ▼                        ▼                          ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                          データ収集レイヤー                                    │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────┐ │
│  │ run_daily_jquants   │ │ run_weekly_tasks    │ │ run_monthly_master      │ │
│  │ (平日 18:00)         │ │ (土曜 06:00)         │ │ (毎月1日 20:30)          │ │
│  └─────────┬───────────┘ └─────────┬───────────┘ └───────────┬─────────────┘ │
└────────────┼───────────────────────┼─────────────────────────┼───────────────┘
             │                       │                         │
             ▼                       ▼                         ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           データストレージ (SQLite)                            │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ ┌────────────────┐ │
│  │  jquants.db     │ │  statements.db  │ │  master.db   │ │analysis_results│ │
│  │  (820MB)        │ │  (30MB)         │ │  (964KB)     │ │   .db (1.7GB)  │ │
│  │  daily_quotes   │ │  financial_     │ │ stocks_master│ │ hl_ratio,      │ │
│  │                 │ │  statements     │ │              │ │ minervini,     │ │
│  │                 │ │  calculated_    │ │              │ │ relative_      │ │
│  │                 │ │  fundamentals   │ │              │ │ strength, etc  │ │
│  └────────┬────────┘ └────────┬────────┘ └──────────────┘ └───────┬────────┘ │
└───────────┼────────────────────┼──────────────────────────────────┼──────────┘
            │                    │                                  ▲
            └────────────────────┼──────────────────────────────────┘
                                 ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                             分析レイヤー                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    run_daily_analysis.py (平日 18:30)                  │  │
│  │  ┌──────────────┐ ┌────────────────┐ ┌──────────────────────────────┐ │  │
│  │  │ minervini.py │ │ high_low_ratio │ │ relative_strength.py         │ │  │
│  │  │ トレンド選別   │ │   HL比率計算    │ │ RSP/RSI計算                   │ │  │
│  │  └──────────────┘ └────────────────┘ └──────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │            chart_classification.py (MLベースチャートパターン分類)        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                            統合・出力レイヤー                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  integrated_analysis.py       │  integrated_analysis2.py              │  │
│  │  複数指標のSQL統合            │  DB保存 + CSV/Excel出力                 │  │
│  │                               │         ↓                              │  │
│  │                               │  analysis_results.db:                 │  │
│  │                               │    integrated_scores (日次蓄積)        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                 │                                            │
│                                 ▼                                            │
│                    ┌─────────────────────────┐                              │
│                    │ output/*.xlsx, *.csv     │                              │
│                    │ analysis_YYYY-MM-DD.xlsx │                              │
│                    └─────────────────────────┘                              │
└───────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                         クエリインターフェース                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  src/technical_tools/screener.py                                   │  │
│  │    StockScreener クラス                                                │  │
│  │    - filter(): 条件フィルタリング（テクニカル/財務/バリュエーション/パターン）│  │
│  │      - includeパラメータでカラムグループ制御（scores/fundamentals/valuation）│  │
│  │      - デフォルトで常時5カラム+フィルタ使用カラムのみ返却                  │  │
│  │    - rank_changes(): 順位変動取得                                       │  │
│  │    - history(): 銘柄時系列取得                                          │  │
│  │                               │                                        │  │
│  │                               ▼                                        │  │
│  │    TechnicalAnalyzer との連携（チャート表示）                            │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                       バックテスト・シミュレーション                             │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  src/technical_tools/backtester.py                                 │  │
│  │    Backtester クラス                                                   │  │
│  │    - add_signal(): シグナル追加（プラグイン形式）                         │  │
│  │    - add_exit_rule(): エグジットルール追加                              │  │
│  │    - run(): バックテスト実行（並列処理対応）                             │  │
│  │    - run_with_screener(): スクリーナー連携バックテスト                   │  │
│  │                               │                                        │  │
│  │                               ▼                                        │  │
│  │    BacktestResults: 結果分析・可視化・エクスポート                       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  src/technical_tools/virtual_portfolio.py                          │  │
│  │    VirtualPortfolio クラス                                             │  │
│  │    - buy()/sell(): 売買記録                                           │  │
│  │    - summary()/holdings(): 現状確認                                   │  │
│  │    - buy_from_screener(): スクリーナー連携                             │  │
│  │                               │                                        │  │
│  │                               ▼                                        │  │
│  │    data/portfolios/*.json: JSON永続化                                  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
```

## レイヤー構成

### 1. データ収集レイヤー (`scripts/`)

| スクリプト | 実行タイミング | 役割 |
|-----------|---------------|------|
| `run_daily_jquants.py` | 平日 18:00 | J-Quants APIから日次株価データを取得 |
| `run_daily_analysis.py` | 平日 18:30 | 日次分析実行（HL比率、Minervini、RSP/RSI） |
| `run_adhoc_integrated_analysis.py` | 平日 19:00 | アドホック統合分析実行 |
| `run_weekly_tasks.py` | 土曜 06:00 | 財務諸表取得 + 統合分析実行 |
| `run_monthly_master.py` | 毎月1日 20:30 | 銘柄マスターデータ更新 |

### 2. API連携レイヤー (`src/market_pipeline/jquants/`)

| モジュール | 機能 |
|-----------|------|
| `data_processor.py` | 非同期処理による日次株価データ取得 |
| `statements_processor.py` | 財務諸表APIフェッチャー |
| `fundamentals_calculator.py` | PER, PBR, ROE, ROA等の財務指標計算 |

### 2.5 yfinanceバリュエーション (`src/market_pipeline/yfinance/`)

| モジュール | 機能 |
|-----------|------|
| `valuation_fetcher.py` | BSデータ（現金等・有利子負債）・時価総額・PER取得、ネットキャッシュ指標計算 |

- ローリング更新: 毎日150銘柄ずつ処理（約20営業日で全銘柄一巡）
- 優先順: BS未取得(PER低い順) → 90日経過(PER低い順) → 更新日古い順
- 出力: `statements.db` → `yfinance_valuation`テーブル
- StockScreenerから`net_cash_ratio`, `cash_neutral_per`でフィルタリング可能

### 3. 分析レイヤー (`src/market_pipeline/analysis/`)

| モジュール | 分析手法 | 出力テーブル |
|-----------|---------|-------------|
| `minervini.py` | マーク・ミネルヴィニのトレンドスクリーニング | minervini |
| `high_low_ratio.py` | 52週高値・安値位置比率 | hl_ratio |
| `relative_strength.py` | 相対力指数（RSP/RSI） | relative_strength |
| `chart_classification.py` | MLベースチャートパターン分類 | classification_results |
| `integrated_analysis.py` | 複数指標の統合クエリ | - |
| `integrated_analysis2.py` | DB保存 + CSV/Excel出力 | integrated_scores |
| `integrated_scores_repository.py` | integrated_scoresテーブルCRUD | integrated_scores |

### 4. ユーティリティレイヤー (`src/market_pipeline/utils/`)

| モジュール | 機能 |
|-----------|------|
| `parallel_processor.py` | ProcessPoolExecutor/ThreadPoolExecutorラッパー |
| `cache_manager.py` | APIレスポンス・計算結果のメモリキャッシュ |
| `slack_notifier.py` | Slack Incoming Webhook通知（SlackNotifier, JobContext, JobResult） |

### 4.1 ニュース巡回設定 (`src/market_pipeline/news/`)

`/discover-stocks`、`/research-stock-news`スキルで使用するニュース巡回先サイトのYAML設定パーサー:

| モジュール | 機能 |
|-----------|------|
| `config_parser.py` | YAML設定読み込み・バリデーション（NewsSource, NewsConfig, FilterKeywords） |

設定ファイル: `config/news_sources.yaml`（カテゴリ: news, analysis, disclosure, financial）

`disclosure` カテゴリは適時開示情報の巡回に使用され、`filter_keywords`（`include`/`exclude`リスト）によるタイトルベースのフィルタリングをサポートする。

### 5. データアクセスレイヤー (`src/market_reader/`)

pandas_datareader風のシンプルなAPIでJ-Quantsデータにアクセス:

```python
from market_reader import DataReader

reader = DataReader()
df = reader.get_prices("7203", start="2024-01-01", end="2024-12-31")
```

| モジュール | 機能 |
|-----------|------|
| `reader.py` | DataReaderクラス（株価データ取得） |
| `exceptions.py` | カスタム例外（StockNotFoundError等） |
| `utils.py` | ユーティリティ関数 |

### 5.1 テクニカル分析レイヤー (`src/technical_tools/`)

Jupyter Notebook向けのテクニカル分析ツール。日本株（J-Quants）と米国株（yfinance）の統一インターフェースを提供:

```python
from technical_tools import TechnicalAnalyzer

# 日本株（J-Quants）
analyzer = TechnicalAnalyzer(source="jquants")
fig = analyzer.plot_chart("7203", show_sma=[25, 75], show_rsi=True)
fig.show()

# 米国株（yfinance）
analyzer = TechnicalAnalyzer(source="yfinance")
fig = analyzer.plot_chart("AAPL", show_sma=[50, 200], show_bb=True, period="1y")
fig.show()

# クロスシグナル検出
signals = analyzer.detect_crosses("7203", patterns=[(5, 25), (25, 75)])
```

| モジュール | 機能 |
|-----------|------|
| `analyzer.py` | TechnicalAnalyzerファサードクラス |
| `screener.py` | StockScreenerクラス（銘柄スクリーニング） |
| `indicators.py` | テクニカル指標計算（SMA, EMA, RSI, MACD, BB） |
| `signals.py` | シグナル検出（ゴールデンクロス/デッドクロス） |
| `charts.py` | plotlyインタラクティブチャート生成 |
| `integration.py` | 既存分析結果との連携 |
| `data_sources/` | データソース抽象化（J-Quants, yfinance） |
| `backtester.py` | Backtesterクラス（シグナルベースバックテスト） |
| `backtest_results.py` | BacktestResultsクラス（結果分析・可視化・エクスポート） |
| `virtual_portfolio.py` | VirtualPortfolioクラス（仮想ポートフォリオ管理） |
| `optimizer.py` | StrategyOptimizerクラス（戦略パラメータ最適化） |
| `optimization_results.py` | OptimizationResultsクラス（最適化結果分析・可視化） |
| `backtest_signals/` | バックテスト用シグナル定義（プラグイン形式） |

### 6. 設定レイヤー (`src/market_pipeline/config/`)

Pydantic Settingsベースの型安全な設定管理システム:

```python
from market_pipeline.config import get_settings

settings = get_settings()

# パス設定
db_path = settings.paths.jquants_db

# API設定
max_requests = settings.jquants.max_concurrent_requests

# 分析設定
sma_short = settings.analysis.sma_short
```

## データベース設計

### jquants.db - 日次株価データ

```sql
CREATE TABLE daily_quotes (
    Date TEXT,
    Code TEXT,
    Open REAL,
    High REAL,
    Low REAL,
    Close REAL,
    UpperLimit TEXT,
    LowerLimit TEXT,
    Volume REAL,
    TurnoverValue REAL,
    AdjustmentFactor REAL,
    AdjustmentOpen REAL,
    AdjustmentHigh REAL,
    AdjustmentLow REAL,
    AdjustmentClose REAL,
    AdjustmentVolume REAL,
    PRIMARY KEY (Date, Code)
);

CREATE INDEX idx_daily_quotes_code ON daily_quotes (Code);
CREATE INDEX idx_daily_quotes_date ON daily_quotes (Date);
```

### statements.db - 財務諸表データ

```sql
CREATE TABLE financial_statements (
    Code TEXT,
    DisclosedDate TEXT,
    ReportType TEXT,
    ...
);

CREATE TABLE calculated_fundamentals (
    Code TEXT,
    Date TEXT,
    PER REAL,
    PBR REAL,
    ROE REAL,
    ROA REAL,
    ...
);

CREATE TABLE yfinance_valuation (
    code TEXT PRIMARY KEY,
    cash_and_equivalents REAL,
    interest_bearing_debt REAL,
    bs_period_end TEXT,
    market_cap REAL,
    per REAL,
    net_cash_ratio REAL,
    cash_neutral_per REAL,
    bs_updated_at TEXT,
    updated_at TEXT
);
```

### analysis_results.db - 分析結果

```sql
CREATE TABLE hl_ratio (
    Code TEXT,
    Date TEXT,
    hl_ratio REAL,
    PRIMARY KEY (Code, Date)
);

CREATE TABLE minervini (
    Code TEXT,
    Date TEXT,
    passed INTEGER,
    score REAL,
    ...
);

CREATE TABLE relative_strength (
    Code TEXT,
    Date TEXT,
    rsp REAL,
    rsi REAL,
    ...
);

CREATE TABLE classification_results (
    Code TEXT,
    Date TEXT,
    window INTEGER,
    pattern_type TEXT,
    confidence REAL,
    ...
);

CREATE TABLE integrated_scores (
    Date TEXT NOT NULL,
    Code TEXT NOT NULL,
    composite_score REAL,
    composite_score_rank INTEGER,
    hl_ratio_rank INTEGER,
    rsp_rank INTEGER,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (Date, Code)
);

CREATE INDEX idx_integrated_scores_date ON integrated_scores (Date);
CREATE INDEX idx_integrated_scores_code ON integrated_scores (Code);
CREATE INDEX idx_integrated_scores_composite_rank ON integrated_scores (Date, composite_score_rank);
```

## 設計原則

### 1. 疎結合アーキテクチャ

各レイヤーは明確に分離されており、独立してテスト・変更が可能:

- API層: 外部APIとの通信を担当
- 計算層: ビジネスロジック（分析アルゴリズム）
- データ層: SQLiteデータベース操作

### 2. 設定の一元管理

`src/market_pipeline/config/settings.py`で全設定を集約:

- 環境変数からの読み込み
- 型安全なアクセス
- シングルトンパターンでインスタンス共有

### 3. パフォーマンス最適化

処理時間を5時間から15-20分に短縮:

- 非同期処理: aiohttp + asyncio
- 並列処理: ProcessPoolExecutor
- バッチ処理: 一括データベース操作
- ベクトル化: NumPy/Pandas
- キャッシュ: テンプレート・計算結果

### 4. エラー耐性

- 個別銘柄のエラーが全体処理を停止しない設計
- エラーログの出力(`output/errors/`)
- リトライ機構（API呼び出し）

## 技術スタック

| カテゴリ | 技術 |
|---------|------|
| 言語 | Python 3.10+ |
| データ処理 | pandas, numpy |
| 非同期処理 | asyncio, aiohttp |
| データベース | SQLite (WALモード) |
| 設定管理 | pydantic-settings |
| 機械学習 | scikit-learn |
| テスト | pytest |
| コード品質 | black, ruff, mypy |
