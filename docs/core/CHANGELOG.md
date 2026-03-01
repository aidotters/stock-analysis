# 変更履歴

このファイルはgitログから自動生成されました。

## [Unreleased]

### Added
- ニュース駆動型銘柄発見 Phase 3: 適時開示情報対応と銘柄ニュース調査スキル
  - `FilterKeywords` dataclass: タイトルベースのキーワードフィルタリング設定（`include`/`exclude`リスト）
  - `NewsSource.filter_keywords` フィールド追加: 適時開示等のフィルタリング対応
  - `config/news_sources.yaml`: `disclosure`カテゴリ追加（会社四季報適時開示ページ、16個のinclude / 3個のexcludeキーワード）
  - `.claude/skills/research-stock-news/SKILL.md`: 銘柄ニュース調査スキル新規作成
  - `tests/test_news_config.py`: `FilterKeywords`バリデーション・パーステスト追加

### Changed
- `.claude/skills/discover-stocks/SKILL.md`: 適時開示巡回ステップ追加（`--category disclosure`対応）
- `.claude/skills/analyze-stock/SKILL.md`: `/research-stock-news`相当のニュース情報を自動統合
- `backend/market_pipeline/news/__init__.py`: `FilterKeywords`エクスポート追加

### Added
- `backend/market_pipeline/news/config_parser.py`: ニュース巡回先設定パーサーモジュール
  - `NewsSource` frozenデータクラス: 巡回先サイト情報（name, auth, url, selector等）
  - `NewsConfig`データクラス: カテゴリ別ソース管理
  - `load_config()`: YAML設定ファイルの読み込みとバリデーション
  - `get_sources_by_category()`: カテゴリ別ソース取得
  - `auth`フィールドのバリデーション（`none`または`cdp`のみ許可）
- `config/news_sources.yaml`: ニュース巡回先設定ファイル（news, analysis, financialカテゴリ）
- `tests/test_news_config.py`: ニュース巡回先設定パーサーのテスト
- `pyproject.toml`: `pyyaml>=6.0`, `types-PyYAML>=6.0`, `nbformat>=5.10.4` 依存関係追加

- `backend/market_pipeline/utils/slack_notifier.py`: Slack Incoming Webhook通知モジュール
  - `SlackNotifier`クラス: 成功・エラー・警告の3種類の通知送信
  - `JobContext`コンテキストマネージャ: `with`文によるジョブの自動通知
  - `JobResult`データクラス: ジョブ実行結果（メトリクス、警告、実行時間）を保持
  - リトライロジック（最大3回、1秒間隔）
  - 通知失敗がジョブの処理結果に影響しない設計
- `backend/market_pipeline/utils/__init__.py`: SlackNotifier, JobContext, JobResultのエクスポート
- `backend/market_pipeline/config/settings.py`: `SlackSettings`クラス追加
  - `SLACK_WEBHOOK_URL`, `SLACK_ERROR_WEBHOOK_URL`, `SLACK_ENABLED`, `SLACK_TIMEOUT_SECONDS`, `SLACK_MAX_RETRIES`環境変数サポート
- `tests/test_slack_notifier.py`: SlackNotifier/JobContext/JobResultのテスト
- `.env.example`: Slack通知設定セクション追加

### Changed
- `scripts/run_daily_jquants.py`: JobContext統合（レコード数・銘柄数・データ期間を通知）
- `scripts/run_daily_analysis.py`: JobContext統合（対象日・実行モジュールを通知）
- `scripts/run_weekly_tasks.py`: JobContext統合（財務データ・統合分析の完了状況を通知）
- `scripts/run_monthly_master.py`: JobContext統合（総銘柄数・アクティブ銘柄数を通知）

- `backend/technical_tools/optimizer.py`: 戦略パラメータ最適化エンジン
  - `StrategyOptimizer`クラス: Backtesterを利用したパラメータ最適化
  - `add_search_space()`: 探索パラメータの定義（MA期間、RSI閾値、エグジットルール等）
  - `add_constraint()`: パラメータ制約条件の追加
  - `run()`: グリッドサーチ/ランダムサーチによる最適化実行
  - 並列処理対応（ThreadPoolExecutor）
  - ウォークフォワード分析（過学習対策）
  - タイムアウト機能
  - ストリーミング出力（JSONL形式）
- `backend/technical_tools/optimization_results.py`: 最適化結果分析クラス
  - `OptimizationResults`: 結果の保持・分析・可視化
  - `TrialResult`: 個別試行結果データクラス
  - `best()`: 最良パラメータの取得
  - `top()`: 上位N件をDataFrameで取得
  - `plot_heatmap()`: パラメータ空間のヒートマップ可視化
  - `save()`/`load()`: JSON/CSV形式での永続化
  - `load_streaming()`: JSONL形式からの読み込み
- `backend/technical_tools/exceptions.py`: 最適化関連例外追加
  - `OptimizerError`, `InvalidSearchSpaceError`, `NoValidParametersError`, `OptimizationTimeoutError`
- テストファイル追加:
  - `tests/test_optimizer.py`: StrategyOptimizerクラステスト
  - `tests/test_optimization_results.py`: OptimizationResultsクラステスト

- `backend/technical_tools/backtester.py`: シグナルベースバックテストエンジン
  - `Backtester`クラス: backtesting.pyをラップしたシンプルなAPI
  - `add_signal()`: プラグイン形式のシグナル追加
  - `add_exit_rule()`: エグジットルール追加（stop_loss, take_profit, max_holding_days, trailing_stop）
  - `run()`: 並列処理対応のバックテスト実行
  - `run_with_screener()`: StockScreener連携バックテスト
- `backend/technical_tools/backtest_results.py`: バックテスト結果分析クラス
  - `BacktestResults`: 結果の保持・分析・可視化
  - `summary()`: パフォーマンス指標（勝率、シャープレシオ、最大DD等）
  - `plot()`: plotlyによる資産推移・ドローダウンチャート
  - `export()`: CSV/Excel/HTML出力
  - `by_symbol()`, `by_sector()`, `monthly_returns()`, `yearly_returns()`: 詳細分析
  - `Trade`データクラス: 個別取引情報
- `backend/technical_tools/virtual_portfolio.py`: 仮想ポートフォリオ管理
  - `VirtualPortfolio`クラス: JSON永続化対応の仮想ポートフォリオ
  - `buy()`: 株数指定または金額指定での購入
  - `sell()`, `sell_all()`: 売却
  - `summary()`, `holdings()`, `performance()`: 状態確認
  - `plot()`: plotlyによるポートフォリオチャート
  - `buy_from_screener()`: StockScreener連携の一括購入
- `backend/technical_tools/backtest_signals/`: バックテスト用シグナルモジュール
  - `BaseSignal`: シグナル抽象基底クラス
  - `SignalRegistry`: シグナルのプラグイン登録・取得
  - 対応シグナル: golden_cross, dead_cross, rsi_oversold, rsi_overbought, macd_cross, bollinger_breakout, bollinger_squeeze, volume_spike, volume_breakout
- `backend/technical_tools/exceptions.py`: バックテスト・ポートフォリオ関連例外追加
  - `BacktestError`, `BacktestInsufficientDataError`, `InvalidSignalError`, `InvalidRuleError`, `PortfolioError`
- テストファイル追加:
  - `tests/test_backtester.py`: Backtesterクラステスト
  - `tests/test_backtest_results.py`: BacktestResultsクラステスト
  - `tests/test_backtest_signals.py`: バックテストシグナルテスト
  - `tests/test_virtual_portfolio.py`: VirtualPortfolioクラステスト
- `data/portfolios/`: VirtualPortfolio用JSONファイル格納ディレクトリ（.gitignore追加）
- `pyproject.toml`: `backtesting>=0.3.3` 依存関係追加

### Fixed
- `backend/technical_tools/data_sources/jquants.py`: 株式分割を考慮した調整後価格（AdjustmentOpen/High/Low/Close/Volume）を使用するように修正
  - 以前は未調整価格（Open/High/Low/Close/Volume）を使用していたため、株式分割時にチャートにギャップが生じていた

### Changed
- `backend/technical_tools/__init__.py`: StrategyOptimizer, OptimizationResults, TrialResult, 最適化例外クラスをエクスポート追加
- `backend/technical_tools/__init__.py`: Backtester, BacktestResults, Trade, VirtualPortfolio, 新例外クラスをエクスポート追加
- パッケージバージョンを0.2.0に更新

### Documentation
- `CLAUDE.md`: StrategyOptimizer使用例とAPI説明追加
- `CLAUDE.md`: Backtester, VirtualPortfolio使用例とAPI説明追加
- `README.md`: Backtester, VirtualPortfolioセクション追加
- `docs/core/api-reference.md`: StrategyOptimizer, OptimizationResults API仕様追加
- `docs/core/api-reference.md`: Backtester, BacktestResults, VirtualPortfolio API仕様追加
- `docs/core/architecture.md`: バックテスト・シミュレーションレイヤー追加
- `docs/core/repo-structure.md`: 新規ファイル・ディレクトリ追加

---

## [Previous]

### Added
- `backend/market_reader/` パッケージ: pandas_datareader風のデータアクセスAPI（旧 `stock_reader/`）
  - `DataReader`クラス: コンストラクタで `db_path` と `strict` パラメータをサポート
  - `get_prices()`: 単一/複数銘柄対応、日付自動デフォルト（end=DB最新日、start=5年前）
  - カラム選択: "simple"（OHLCV + AdjustmentClose）、"full"（全16カラム）、カスタムリスト
  - 4/5桁コード自動正規化（出力は常に4桁）
  - カスタム例外クラス: `StockReaderError`, `StockNotFoundError`, `DatabaseConnectionError`, `InvalidDateRangeError`
  - ユーティリティ関数: `normalize_code()`, `to_5digit_code()`, `validate_date()`
  - PRAGMA最適化（WALモード、キャッシュ設定）
- `notebooks/` ディレクトリ: 分析・可視化用Jupyterノートブック
- `py.typed` マーカーファイル: PEP 561準拠の型ヒントサポート
  - `backend/market_pipeline/py.typed`
  - `backend/market_reader/py.typed`
- パフォーマンスベンチマークファイル（テストから分離）:
  - `tests/benchmark_integrated_analysis_optimization.py`
  - `tests/benchmark_jquants_performance.py`
  - `tests/benchmark_optimizations.py`

### Changed
- yfinanceからJ-Quantsデータ計算への切り替え (`refactor/yfinance-to-jquants`ブランチ)
- FundamentalsCalculator: J-Quantsデータを使用した財務指標計算への移行
- ドキュメント構造の再編成（`docs/refs/`、`docs/core/`）
- リポジトリ構造のリファクタリング:
  - `core/` → `backend/market_pipeline/` へ移動
  - `stock_reader/` → `backend/market_reader/` へ移動
- `notebook/` を `notebooks/` にリネーム
- テストファイルの整理:
  - パフォーマンステストを `benchmark_*.py` に分離
  - `test_functions.py` を削除（機能を他のテストに統合）

### Removed
- `tests/test_functions.py` - 他のテストファイルに統合
- `tests/test_integrated_analysis_optimization.py` - ベンチマークに移行
- `tests/test_jquants_performance.py` - ベンチマークに移行
- `tests/test_optimizations.py` - ベンチマークに移行

---

## 最近のコミット履歴

### 6703a19 - yfinanceからjquantsデータ計算への切り替え
- **タイプ**: refactor
- **スコープ**: yfinance-to-jquants
- **概要**: yfinanceからJ-Quantsへのデータソース移行

### 86b372f - config設定の導入
- **概要**: Pydantic Settings ベースの設定システム導入
- 環境変数からの設定読み込み
- 型安全な設定アクセス

### a6c22b9 - ファイル名変更
- **概要**: `*_optimized.py` を `*.py` にリネーム
- 最適化版をメインバージョンとして採用

### 06b0118 - chart_classificationを週次から日次タスクへ移動
- **概要**: チャート分類処理の実行タイミング変更
- 週次実行から日次実行へ

### 70b589e - パフォーマンス問題の解決とその他更新
- **概要**: 処理時間の大幅短縮（5時間 → 15-20分）
- 並列処理の導入
- バッチ処理の最適化
- データベースインデックスの追加

### b2eb89f - get_refresh_token関数の追加
- **スコープ**: jquants/data_processing.py
- **概要**: J-Quants API認証のリフレッシュトークン取得機能

### 38a9118 - integrated_analysis2.pyの追加とREADME更新
- **概要**: Excel出力機能の追加
- READMEドキュメントの更新

### cf38fb6 - ノートブック追加
- Jupyterノートブックの追加

### 340763b - 分析機能とテスト機能の追加
- **概要**: 新しい分析アルゴリズムの実装
- テストカバレッジの拡充

### 7bd2483 - READMEの更新
- **タイプ**: Docs
- **概要**: 新しい分析機能と使用方法の説明を追加

### f50576a - 初期コミット
- **概要**: プロジェクトの初期セットアップ

---

## 主要マイルストーン

### パフォーマンス最適化 (70b589e)
処理時間を5時間から15-20分に短縮（約15-20倍の高速化）

**採用した最適化技術:**
1. 並列処理 (ProcessPoolExecutor)
2. 非同期API呼び出し (aiohttp)
3. バッチデータベース操作
4. ベクトル化計算 (NumPy/Pandas)
5. テンプレートキャッシュ
6. データベースインデックス

### 設定システム導入 (86b372f)
Pydantic Settingsベースの型安全な設定管理システム導入

**機能:**
- 環境変数からの自動読み込み
- 階層的な設定構造
- シングルトンパターン

### yfinance → J-Quants移行 (6703a19)
データソースをyfinanceからJ-Quants APIに完全移行

**理由:**
- より信頼性の高いデータソース
- 財務諸表データの統合
- 日本株市場に特化

---

## バージョン命名規則

このプロジェクトは現在開発中であり、正式なバージョン番号は付与されていません。

将来的には[セマンティックバージョニング](https://semver.org/lang/ja/)を採用予定:
- MAJOR: 後方互換性のないAPI変更
- MINOR: 後方互換性のある機能追加
- PATCH: 後方互換性のあるバグ修正
