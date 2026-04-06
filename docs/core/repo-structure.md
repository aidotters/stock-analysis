# リポジトリ構造

## ディレクトリツリー

```
Stock-Analysis/
├── src/                              # バックエンドパッケージ群
│   ├── __init__.py
│   │
│   ├── market_pipeline/              # コアロジックとデータ処理（旧core/）
│   │   ├── __init__.py
│   │   ├── py.typed                      # PEP 561型ヒントマーカー
│   │   ├── analysis/                     # 分析アルゴリズム
│   │   │   ├── minervini.py              # ミネルヴィニトレンドスクリーニング
│   │   │   ├── high_low_ratio.py         # 52週高値・安値比率
│   │   │   ├── relative_strength.py      # RSP/RSI計算
│   │   │   ├── chart_classification.py   # MLベースチャートパターン分類
│   │   │   ├── integrated_analysis.py    # 複数指標統合クエリ
│   │   │   ├── integrated_analysis2.py   # DB保存 + CSV/Excel出力
│   │   │   ├── integrated_scores_repository.py  # integrated_scoresテーブルCRUD
│   │   │   ├── demo_integrated_analysis.py
│   │   │   └── _old/                     # 旧バージョン（参照用）
│   │   │       ├── minervini.py
│   │   │       ├── high_low_ratio.py
│   │   │       └── relative_strength.py
│   │   │
│   │   ├── config/                       # 設定管理
│   │   │   ├── __init__.py               # get_settings()エクスポート
│   │   │   └── settings.py               # Pydantic Settings定義
│   │   │
│   │   ├── jquants/                      # J-Quants API連携
│   │   │   ├── data_processor.py         # 日次株価データ取得（非同期）
│   │   │   ├── statements_processor.py   # 財務諸表API
│   │   │   ├── fundamentals_calculator.py # PER/PBR/ROE等計算
│   │   │   └── _old/
│   │   │       └── data_processor.py
│   │   │
│   │   ├── master/                       # マスターデータ処理
│   │   │   └── master_db.py
│   │   │
│   │   ├── news/                         # ニュース巡回設定
│   │   │   ├── __init__.py              # エクスポート定義
│   │   │   └── config_parser.py          # YAML設定読み込み・バリデーション
│   │   │
│   │   ├── utils/                        # ユーティリティ
│   │   │   ├── __init__.py              # エクスポート定義
│   │   │   ├── parallel_processor.py     # 並列処理フレームワーク
│   │   │   ├── cache_manager.py          # キャッシュ管理
│   │   │   └── slack_notifier.py         # Slack通知（SlackNotifier, JobContext, JobResult）
│   │   │
│   │   └── yfinance/                     # yfinance連携
│   │       ├── data_processor.py          # レガシー株価取得
│   │       ├── valuation_fetcher.py       # BSデータ・バリュエーション指標取得（ローリング更新）
│   │       └── historical_price_fetcher.py # 過去20年分日足データ取得（J-Quantsデータ補完）
│   │
│   ├── market_reader/                    # pandas_datareader風データアクセスAPI（旧stock_reader/）
│   │   ├── __init__.py                   # パッケージエクスポート
│   │   ├── py.typed                      # PEP 561型ヒントマーカー
│   │   ├── reader.py                     # DataReaderクラス実装
│   │   ├── utils.py                      # ユーティリティ関数
│   │   └── exceptions.py                 # カスタム例外クラス
│   │
│   └── technical_tools/                  # Jupyter Notebook用テクニカル分析ツール
│       ├── __init__.py                   # パッケージエクスポート
│       ├── analyzer.py                   # TechnicalAnalyzerファサードクラス
│       ├── screener.py                   # StockScreenerクラス（銘柄スクリーニング）
│       ├── indicators.py                 # テクニカル指標計算（SMA, EMA, RSI, MACD, BB）
│       ├── signals.py                    # シグナル検出（ゴールデンクロス/デッドクロス）
│       ├── charts.py                     # plotlyによるチャート生成
│       ├── integration.py                # 既存分析結果との連携
│       ├── exceptions.py                 # カスタム例外クラス
│       ├── backtester.py                 # Backtesterクラス（バックテスト実行）
│       ├── backtest_results.py           # BacktestResultsクラス（結果分析）
│       ├── virtual_portfolio.py          # VirtualPortfolioクラス（仮想ポートフォリオ）
│       ├── optimizer.py                  # StrategyOptimizerクラス（戦略最適化）
│       ├── optimization_results.py       # OptimizationResultsクラス（最適化結果）
│       ├── data_sources/                 # データソース抽象化
│       │   ├── __init__.py
│       │   ├── base.py                   # DataSource抽象基底クラス
│       │   ├── jquants.py                # J-Quantsデータソース（market_reader経由）
│       │   └── yfinance.py               # yfinanceデータソース
│       └── backtest_signals/             # バックテスト用シグナル定義
│           ├── __init__.py               # SignalRegistryエクスポート
│           ├── base.py                   # BaseSignal抽象基底クラス
│           ├── moving_average.py         # GoldenCross/DeadCrossシグナル
│           ├── rsi.py                    # RSIOversold/RSIOverboughtシグナル
│           ├── macd.py                   # MACDCrossシグナル
│           ├── bollinger.py              # BollingerBreakout/Squeezeシグナル
│           └── volume.py                 # VolumeSpike/VolumeBreakoutシグナル
│
├── scripts/                          # 実行スクリプト（launchd用）
│   ├── run_daily_jquants.py          # 日次株価取得（平日18:00）→ チェーンで日次分析・統合分析を順次実行
│   ├── run_daily_analysis.py         # 日次分析（チェーン実行 / --no-chainで単独実行可）
│   ├── run_weekly_tasks.py           # 週次タスク（土曜06:00）
│   ├── run_monthly_master.py         # 月次マスター更新（1日20:30）
│   ├── run_adhoc_integrated_analysis.py # アドホック統合分析
│   ├── create_database_indexes.py    # DBインデックス作成
│   ├── run_historical_prices.py      # yfinance過去データ一括取得（手動、初回）
│   ├── migrate_add_source_column.py  # daily_quotesにsourceカラム追加マイグレーション
│   ├── migrate_refetch_yfinance.py  # yfinanceデータ削除→auto_adjust=Falseで再取得
│   ├── migrate_rescale_yfinance.py  # yfinanceデータをJ-Quants境界比率でリスケール
│   └── _old/
│       ├── run_daily_jquants.py
│       └── run_daily_analysis.py
│
├── tests/                            # テストコード
│   ├── conftest.py                   # 共有フィクスチャ
│   ├── fixtures/                     # テスト用データファイル
│   │   └── sample_prices.csv         # サンプル株価データ
│   ├── test_minervini.py
│   ├── test_high_low_ratio.py
│   ├── test_relative_strength.py
│   ├── test_chart_classification.py
│   ├── test_integrated_analysis.py
│   ├── test_jquants_data_processor.py
│   ├── test_statements_processor.py
│   ├── test_fundamentals_calculator.py
│   ├── test_data_processor.py        # yfinance（レガシー）
│   ├── test_valuation_fetcher.py    # ValuationFetcher（yfinance BS取得）
│   ├── test_analysis_integration.py
│   ├── test_type8_optimization.py
│   ├── test_rsi_optimization.py
│   ├── test_fixes.py
│   ├── test_stock_reader.py          # market_readerパッケージテスト
│   ├── test_slack_notifier.py       # SlackNotifier/JobContext/JobResult
│   ├── test_technical_tools.py      # TechnicalAnalyzerクラステスト
│   ├── test_integrated_scores.py    # IntegratedScoresRepositoryテスト
│   ├── test_stock_screener.py       # StockScreenerクラステスト
│   ├── test_backtester.py           # Backtesterクラステスト
│   ├── test_backtest_results.py     # BacktestResultsクラステスト
│   ├── test_backtest_signals.py     # バックテストシグナルテスト
│   ├── test_virtual_portfolio.py    # VirtualPortfolioクラステスト
│   ├── test_optimizer.py            # StrategyOptimizerクラステスト
│   ├── test_optimization_results.py # OptimizationResultsクラステスト
│   ├── test_news_config.py          # ニュース巡回先設定パーサーテスト
│   ├── simple_test.py
│   ├── benchmark_integrated_analysis_optimization.py  # パフォーマンスベンチマーク
│   ├── benchmark_jquants_performance.py               # J-Quantsパフォーマンスベンチマーク
│   └── benchmark_optimizations.py                     # 最適化ベンチマーク
│
├── data/                             # SQLiteデータベース
│   ├── jquants.db                    # 日次株価（820MB）
│   ├── statements.db                 # 財務諸表（30MB）
│   ├── analysis_results.db           # 分析結果（1.7GB）
│   ├── master.db                     # 銘柄マスター（964KB）
│   ├── yfinance.db                   # レガシー（1.4MB）
│   └── portfolios/                   # VirtualPortfolio用JSONファイル（.gitignore）
│       └── *.json
│
├── output/                           # 出力ファイル
│   ├── analysis_YYYY-MM-DD.xlsx      # 日次分析レポート
│   ├── reports/stocks/               # 銘柄詳細分析レポート（analyze-stock出力）
│   │   └── YYYYMMDD-HHMM-{code}-analysis/  # タイムスタンプ付きディレクトリ
│   │       ├── base_report.md        # Phase 1レポート
│   │       ├── deep_research_report.md  # Deep Research結果（実行時のみ）
│   │       └── chart.png             # 株価チャート（kaleido利用可能時のみ）
│   └── errors/                       # エラーログ
│
├── logs/                             # 実行ログ
│   └── *.log
│
├── docs/                             # ドキュメント
│   ├── core/                         # コアドキュメント
│   │   ├── architecture.md
│   │   ├── api-reference.md
│   │   ├── diagrams.md
│   │   ├── CHANGELOG.md
│   │   ├── dev-guidelines.md
│   │   └── repo-structure.md
│   ├── refs/                         # 参照用ドキュメント
│   │   ├── technical_design.md
│   │   ├── OPTIMIZATION_TECHNIQUES_GUIDE.md
│   │   ├── JQUANTS_OPTIMIZATION_README.md
│   │   └── ANALYSIS_OPTIMIZATION_README.md
│   ├── plan/                         # 実装計画ドキュメント
│   │   └── *.md
│   ├── ideas/                        # アイデア・検討用ドキュメント
│   └── reports/                      # レポート出力
│       ├── adhoc/                    # アドホック分析レポート（discover-stocks等）
│       └── stocks/                   # 銘柄ニュース調査レポート（research-stock-news出力）
│
├── notebooks/                        # Jupyter Notebook（分析・可視化用）
│   └── *.ipynb
│
├── sandbox/                          # 実験用コード
│
├── config/                          # 設定ファイル
│   └── news_sources.yaml            # ニュース巡回先設定
│
├── .claude/skills/                  # Claude Codeスキル定義
│   ├── acceptance-test/             # 受け入れテストスキル
│   │   └── SKILL.md
│   ├── analyze-stock/               # 銘柄詳細分析スキル
│   │   └── SKILL.md
│   ├── architecture-design/         # アーキテクチャ設計書作成スキル
│   │   └── SKILL.md
│   ├── brainstorm/                  # アイデア壁打ちスキル
│   │   └── SKILL.md
│   ├── development-guidelines/      # 開発ガイドライン作成スキル
│   │   └── SKILL.md
│   ├── discover-stocks/             # ニュースドリブン銘柄発見スキル
│   │   └── SKILL.md
│   ├── functional-design/           # 機能設計書作成スキル
│   │   └── SKILL.md
│   ├── gen-all-docs/                # 全ドキュメント一括生成スキル
│   │   └── SKILL.md
│   ├── glossary-creation/           # 用語集作成スキル
│   │   └── SKILL.md
│   ├── implement-feature/           # 機能実装スキル
│   │   └── SKILL.md
│   ├── initial-setup/               # プロジェクト初期セットアップスキル
│   │   └── SKILL.md
│   ├── plan-feature/                # 機能計画スキル
│   │   └── SKILL.md
│   ├── prd-writing/                 # PRD作成スキル
│   │   └── SKILL.md
│   ├── repository-structure/        # リポジトリ構造定義書作成スキル
│   │   └── SKILL.md
│   ├── research-stock-news/         # 銘柄ニュース調査スキル
│   │   └── SKILL.md
│   ├── review-docs/                 # ドキュメントレビュースキル
│   │   └── SKILL.md
│   ├── steering/                    # 作業計画・タスクリスト管理スキル
│   │   └── SKILL.md
│   ├── update-docs/                 # ドキュメント更新スキル
│   │   └── SKILL.md
│   ├── validate-code/               # コード品質検証スキル
│   │   └── SKILL.md
│   └── validation/                  # コード品質検証共通ロジック
│       └── SKILL.md
│
├── .env                              # 環境変数（gitignore）
├── .env.example                      # 環境変数テンプレート
├── .gitignore
├── CLAUDE.md                         # Claude Code用ガイド
├── README.md
├── pyproject.toml                    # プロジェクト設定
└── uv.lock                           # 依存関係ロック
```

## 主要ファイル説明

### 設定ファイル

| ファイル | 説明 |
|---------|------|
| `pyproject.toml` | プロジェクト設定、依存関係、ツール設定 |
| `.env` | 環境変数（API認証情報等） |
| `.env.example` | 環境変数テンプレート |
| `CLAUDE.md` | Claude Code用のプロジェクトガイド |

### バックエンドモジュール

| パス | 説明 |
|-----|------|
| `src/market_pipeline/config/settings.py` | Pydantic Settings による設定管理 |
| `src/market_pipeline/jquants/data_processor.py` | 非同期株価データ取得（~500行） |
| `src/market_pipeline/jquants/statements_processor.py` | 財務諸表取得（~400行） |
| `src/market_pipeline/jquants/fundamentals_calculator.py` | 財務指標計算（~300行） |
| `src/market_pipeline/analysis/minervini.py` | ミネルヴィニ分析 |
| `src/market_pipeline/analysis/high_low_ratio.py` | HL比率計算 |
| `src/market_pipeline/analysis/relative_strength.py` | RSP/RSI計算 |
| `src/market_pipeline/analysis/chart_classification.py` | チャートパターン分類 |
| `src/market_pipeline/analysis/integrated_analysis2.py` | DB保存 + CSV/Excel出力 |
| `src/market_pipeline/analysis/integrated_scores_repository.py` | integrated_scoresテーブルCRUD |
| `src/market_pipeline/utils/parallel_processor.py` | 並列処理ラッパー |
| `src/market_pipeline/utils/cache_manager.py` | キャッシュ管理 |
| `src/market_pipeline/utils/slack_notifier.py` | Slack Incoming Webhook通知（SlackNotifier, JobContext, JobResult） |
| `src/market_pipeline/news/config_parser.py` | ニュース巡回先YAML設定パーサー |
| `src/market_pipeline/yfinance/historical_price_fetcher.py` | HistoricalPriceFetcher（yfinance過去20年分日足データ取得） |
| `src/market_reader/reader.py` | DataReaderクラス（pandas_datareader風API） |
| `src/market_reader/exceptions.py` | カスタム例外クラス |
| `src/technical_tools/analyzer.py` | TechnicalAnalyzerファサードクラス（テクニカル分析統合） |
| `src/technical_tools/indicators.py` | テクニカル指標計算（SMA, EMA, RSI, MACD, BB） |
| `src/technical_tools/signals.py` | シグナル検出（ゴールデンクロス/デッドクロス） |
| `src/technical_tools/charts.py` | plotlyインタラクティブチャート生成 |
| `src/technical_tools/screener.py` | StockScreenerクラス（銘柄スクリーニング） |
| `src/technical_tools/backtester.py` | Backtesterクラス（シグナルベースバックテスト） |
| `src/technical_tools/backtest_results.py` | BacktestResultsクラス（結果分析・可視化） |
| `src/technical_tools/virtual_portfolio.py` | VirtualPortfolioクラス（仮想ポートフォリオ） |
| `src/technical_tools/backtest_signals/` | バックテスト用シグナル定義（プラグイン形式） |
| `src/technical_tools/optimizer.py` | StrategyOptimizerクラス（戦略パラメータ最適化） |
| `src/technical_tools/optimization_results.py` | OptimizationResultsクラス（最適化結果分析・可視化） |

### スクリプト

| パス | 実行タイミング | 説明 |
|-----|--------------|------|
| `scripts/run_daily_jquants.py` | 平日18:00 (launchd) | J-Quants APIから株価取得 → チェーンで daily_analysis → integrated_analysis を順次実行 |
| `scripts/run_daily_analysis.py` | チェーン実行 | 日次分析実行 → チェーンで integrated_analysis を実行（`--no-chain`で単独実行可） |
| `scripts/run_adhoc_integrated_analysis.py` | チェーン実行 / 手動 | アドホック統合分析実行 |
| `scripts/run_weekly_tasks.py` | 土曜06:00 | 財務諸表取得 + 統合分析 |
| `scripts/run_monthly_master.py` | 毎月1日20:30 | マスターデータ更新 |
| `scripts/create_database_indexes.py` | 初回のみ | DBインデックス作成 |
| `scripts/run_historical_prices.py` | 手動（初回） | yfinanceから過去20年分の日足データ取得 |
| `scripts/migrate_add_source_column.py` | 手動（初回） | daily_quotesにsourceカラム追加マイグレーション |
| `scripts/migrate_refetch_yfinance.py` | 手動（必要時） | yfinanceデータを削除してauto_adjust=Falseで再取得 |
| `scripts/migrate_rescale_yfinance.py` | 手動（必要時） | yfinanceデータをJ-Quants境界比率でリスケール |

### データベース

| ファイル | サイズ | 主要テーブル |
|---------|-------|-------------|
| `data/jquants.db` | 820MB | daily_quotes（sourceカラムで'jquants'/'yfinance'を区別） |
| `data/statements.db` | 30MB | financial_statements, calculated_fundamentals |
| `data/analysis_results.db` | 1.7GB | hl_ratio, minervini, relative_strength, classification_results, integrated_scores |
| `data/master.db` | 964KB | stocks_master |

### テストファイル

| ファイル | テスト対象 |
|---------|----------|
| `tests/conftest.py` | 共有フィクスチャ |
| `tests/test_minervini.py` | ミネルヴィニ分析 |
| `tests/test_high_low_ratio.py` | HL比率 |
| `tests/test_relative_strength.py` | RSP/RSI |
| `tests/test_chart_classification.py` | チャートパターン分類 |
| `tests/test_integrated_analysis.py` | 統合分析 |
| `tests/test_analysis_integration.py` | 分析モジュール統合テスト |
| `tests/test_statements_processor.py` | 財務諸表処理 |
| `tests/test_fundamentals_calculator.py` | 財務指標計算 |
| `tests/test_jquants_data_processor.py` | J-Quants API |
| `tests/test_data_processor.py` | yfinanceデータ処理（レガシー） |
| `tests/test_valuation_fetcher.py` | ValuationFetcher（yfinance BS・バリュエーション取得） |
| `tests/test_stock_reader.py` | market_readerパッケージ |
| `tests/test_technical_tools.py` | technical_toolsパッケージ |
| `tests/test_integrated_scores.py` | IntegratedScoresRepository |
| `tests/test_stock_screener.py` | StockScreenerクラス |
| `tests/test_backtester.py` | Backtesterクラス |
| `tests/test_backtest_results.py` | BacktestResultsクラス |
| `tests/test_backtest_signals.py` | バックテストシグナル |
| `tests/test_virtual_portfolio.py` | VirtualPortfolioクラス |
| `tests/test_optimizer.py` | StrategyOptimizerクラス |
| `tests/test_optimization_results.py` | OptimizationResultsクラス |
| `tests/test_slack_notifier.py` | SlackNotifier/JobContext/JobResult |
| `tests/test_news_config.py` | ニュース巡回先設定パーサー |
| `tests/test_historical_price_fetcher.py` | HistoricalPriceFetcher（yfinance過去データ取得・カラムマッピング・マイグレーション） |
| `tests/test_type8_optimization.py` | Type8最適化 |
| `tests/test_rsi_optimization.py` | RSI最適化 |
| `tests/test_fixes.py` | バグ修正検証 |
| `tests/simple_test.py` | 簡易テスト |

### ベンチマークファイル

| ファイル | 対象 |
|---------|-----|
| `tests/benchmark_integrated_analysis_optimization.py` | 統合分析パフォーマンス |
| `tests/benchmark_jquants_performance.py` | J-Quantsデータ取得パフォーマンス |
| `tests/benchmark_optimizations.py` | 各種最適化パフォーマンス |

## ディレクトリ命名規則

- **`_old/`**: 旧バージョンのファイル（参照用に保持）
- **`core/`**: コアドキュメント

## ファイル命名規則

- **Pythonモジュール**: `snake_case.py`
- **テストファイル**: `test_<module_name>.py`
- **設定ファイル**: `lowercase.toml`, `.env`
- **ドキュメント**: `kebab-case.md` または `UPPERCASE.md`
