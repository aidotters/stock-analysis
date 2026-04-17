# アーキテクチャ設計書

## 概要

Stock-Analysisは、日本株式市場データの自動収集・分析システムです。J-Quants APIを利用して日次株価、財務諸表、マスターデータを収集し、複数の分析戦略（Minervini、HL比率、相対力、チャートパターン分類）を実行します。さらに yfinance バリュエーション指標（ネットキャッシュ比率・キャッシュニュートラルPER）の取得と、EDINET 有価証券報告書からの経営陣6軸スコアリングも統合しています。

## システム全体像

```text
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                            外部データソース                                             │
├───────────────────────┬─────────────────────────┬──────────────────┬──────────────────┤
│    J-Quants API       │  J-Quants Statements    │  Master Data API │  yfinance / EDINET API │
│   (日次株価四本値)      │  (財務諸表データ)         │  (銘柄マスター)   │ (BS・時価総額・有報)     │
└───────────┬───────────┴────────────┬────────────┴────────┬─────────┴──────┬───────────┘
            │                        │                          │
            ▼                        ▼                          ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                          データ収集レイヤー                                    │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────┐ │
│  │ run_daily_jquants   │ │ run_weekly_tasks    │ │ run_monthly_master      │ │
│  │ (平日 18:00)         │ │ (土曜 06:00)         │ │ (毎月1日 20:30)          │ │
│  │ → daily_analysis    │ │                     │ │                         │ │
│  │   (yfinance_valu.)  │ │                     │ │                         │ │
│  │ → integrated_anal.  │ │                     │ │                         │ │
│  └─────────┬───────────┘ └─────────┬───────────┘ └───────────┬─────────────┘ │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ run_executive_master_update.py (月次 / EDINET有報→役員マスター)         │  │
│  │ /research-executives, /analyze-stock --with-executive-research         │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└────────────┼───────────────────────┼─────────────────────────┼───────────────┘
             │                       │                         │
             ▼                       ▼                         ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           データストレージ (SQLite)                            │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ ┌────────────────┐ │
│  │  jquants.db     │ │  statements.db  │ │  master.db   │ │analysis_results│ │
│  │  daily_quotes   │ │  financial_     │ │ stocks_master│ │ hl_ratio,      │ │
│  │  (source区別:   │ │  statements     │ │              │ │ minervini,     │ │
│  │   jquants/yf)   │ │  calculated_    │ │              │ │ relative_      │ │
│  │                 │ │  fundamentals   │ │              │ │ strength,      │ │
│  │                 │ │  yfinance_      │ │              │ │ classification,│ │
│  │                 │ │  valuation      │ │              │ │ integrated_    │ │
│  │                 │ │  executives,    │ │              │ │ scores         │ │
│  │                 │ │  executive_com. │ │              │ │                │ │
│  │                 │ │  executive_eval.│ │              │ │                │ │
│  └────────┬────────┘ └────────┬────────┘ └──────────────┘ └───────┬────────┘ │
└───────────┼────────────────────┼──────────────────────────────────┼──────────┘

> サイズ参考値（2026-04時点）: jquants.db ≒ 2.7GB（yfinance過去20年取得後）／ analysis_results.db ≒ 2.0GB ／ statements.db ≒ 63MB ／ master.db ≒ 964KB。yfinance/経営陣データ投入状況により変動する。
            │                    │                                  ▲
            └────────────────────┼──────────────────────────────────┘
                                 ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                             分析レイヤー                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    run_daily_analysis.py (チェーン実行)                 │  │
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
| `run_daily_jquants.py` | 平日 18:00 (launchd) | J-Quants APIから日次株価取得 → チェーンで daily_analysis → integrated_analysis を順次実行 |
| `run_daily_analysis.py` | チェーン実行 | 日次分析実行（HL比率、Minervini、RSP/RSI）→ チェーンで integrated_analysis を実行 |
| `run_adhoc_integrated_analysis.py` | チェーン実行 / 手動 | アドホック統合分析実行 |
| `run_weekly_tasks.py` | 土曜 06:00 | 財務諸表取得 + 統合分析実行 |
| `run_monthly_master.py` | 毎月1日 20:30 | 銘柄マスターデータ更新 |
| `run_historical_prices.py` | 手動（初回） | yfinanceから過去20年分の日足データを取得 |
| `migrate_add_source_column.py` | 手動（初回） | daily_quotesにsourceカラムを追加するマイグレーション |
| `migrate_refetch_yfinance.py` | 手動（必要時） | yfinanceデータを削除してauto_adjust=Falseで再取得 |
| `migrate_rescale_yfinance.py` | 手動（必要時） | yfinanceデータをJ-Quants境界比率でリスケール |

### 2. API連携レイヤー (`src/market_pipeline/jquants/`)

| モジュール | 機能 |
|-----------|------|
| `data_processor.py` | 非同期処理による日次株価データ取得 |
| `statements_processor.py` | 財務諸表APIフェッチャー |
| `fundamentals_calculator.py` | PER, PBR, ROE, ROA等の財務指標計算 |

### 2.5 yfinance連携 (`src/market_pipeline/yfinance/`)

| モジュール | 機能 |
|-----------|------|
| `valuation_fetcher.py` | BSデータ（現金等・有利子負債）・時価総額・PER取得、ネットキャッシュ指標計算 |
| `historical_price_fetcher.py` | 過去20年分の日足データ取得（J-Quantsデータ範囲外を補完） |

**ValuationFetcher:**
- ローリング更新: 毎日150銘柄ずつ処理（約20営業日で全銘柄一巡）
- 優先順: BS未取得(PER低い順) → 90日経過(PER低い順) → 更新日古い順
- 出力: `statements.db` → `yfinance_valuation`テーブル
- StockScreenerから`net_cash_ratio`, `cash_neutral_per`でフィルタリング可能

**HistoricalPriceFetcher:**
- J-Quants Light契約（過去5年分）の範囲外を補完し、最大20年分の日足データを提供
- yfinance OHLCVをAdjustmentOpen/High/Low/Close/Volumeにマッピング（未調整カラムはNULL）
- INSERT OR IGNOREにより既存J-Quantsデータを優先（重複なし）
- ThreadPoolExecutor + リトライ（最大3回、1秒間隔）
- 出力: `jquants.db` → `daily_quotes`テーブル（`source='yfinance'`）

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

### 3.1 経営陣評価レイヤー (`src/market_pipeline/executives/`)

EDINET有価証券報告書から法定役員（取締役・監査役・執行役）情報＋略歴を取得し、外部発信を WebSearch + Claude LLM で6軸スコアリング（ビジョン一貫性・実行力・市場認識・リスク開示誠実性・コミュニケーション能力・成長志向）するモジュール群。

| モジュール | 機能 | 入出力 |
|-----------|------|--------|
| `edinet_executive_fetcher.py` | EDINET API ダウンロード + `0104010_*_ixbrl.htm` の iXBRL パース（取締役系＋執行役系の両タグ、略歴含む） | XBRL → `Executive` dataclass |
| `edinet_doc_resolver.py` | 銘柄コード→`doc_id` 解決（初回18ヶ月フルスキャン、2回目以降は期末月前後30日のみ）。月次バッチは `documents.json` から取得した `docID` を前回キャッシュと比較し、一致すれば XBRL ZIP の DL・パース・upsert を全てスキップ（`status=unchanged`） | `executives.edinet_source_doc_id` キャッシュ参照 |
| `repository.py` | 3テーブル（`executives`/`executive_communications`/`executive_evaluations`）のCRUD・UPSERT・差分更新 | `statements.db` |
| `communication_collector.py` | WebSearch（DI）+ 30日キャッシュで発信収集、URLドメイン→カテゴリ一次分類、発信日抽出フォールバック | WebSearch → `executive_communications` |
| `published_date_extractor.py` | URL HTMLから発信日を抽出（JSON-LD / meta / time / URLパス） | URL → YYYY-MM-DD |
| `evaluator.py` | Claude LLM（DI）で6軸スコアリング（成長志向含む）、スキーマ違反時は最大3回リトライ | Communications → `executive_evaluations` |
| `exceptions.py` | `ExecutiveError` 基底＋派生（`EdinetFetchError` / `EdinetParseError` / `EvaluationError`） | - |

プロンプトは `src/market_pipeline/prompts/executive_evaluation.md` にファイル管理。

スキル層 `/analyze-stock --with-executive-research` および `/research-executives` から共通モジュールとして呼び出される。

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
    source TEXT,  -- 'jquants' or 'yfinance'
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

-- 経営陣評価モジュール用（executives/）
CREATE TABLE executives (
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    is_representative INTEGER NOT NULL DEFAULT 0,
    appointed_date TEXT,
    birthdate TEXT,
    edinet_source_doc_id TEXT,
    career_summary TEXT,  -- EDINET XBRL の略歴テキスト（~400字）
    updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (code, name, role)
);
CREATE INDEX idx_executives_code ON executives (code);
CREATE INDEX idx_executives_repr ON executives (code, is_representative);

CREATE TABLE executive_communications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    executive_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_type TEXT,  -- interview/blog/speech/book/article
    published_date TEXT,
    title TEXT,
    summary TEXT,
    collected_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE (executive_name, source_url)
);
CREATE INDEX idx_comm_code_name ON executive_communications (code, executive_name);
CREATE INDEX idx_comm_collected ON executive_communications (executive_name, collected_at);

CREATE TABLE executive_evaluations (
    code TEXT NOT NULL,
    executive_name TEXT NOT NULL,
    evaluation_date TEXT NOT NULL,
    vision_consistency REAL,
    execution_track_record REAL,
    market_awareness REAL,
    risk_disclosure_honesty REAL,
    communication_clarity REAL,
    growth_ambition REAL,  -- 成長志向・戦略性（Phase 2改善で追加）
    overall_score REAL,
    rationale TEXT,  -- JSON: {axis: comment}
    PRIMARY KEY (code, executive_name, evaluation_date)
);
CREATE INDEX idx_eval_code ON executive_evaluations (code);
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
