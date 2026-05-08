# 変更履歴

このファイルはgitログから自動生成されました。

## [Unreleased]

> このセクションは `feature/exexutive-communication-analysis` ブランチで進行中の作業をまとめたもの。main にマージされた時点で次バージョン（[0.10.0]）として切り出し予定。最終更新: 2026-05-08。

### Added
- 経営陣コミュニケーション分析モジュール (`src/market_pipeline/executives/`)
  - `EdinetExecutiveFetcher`: EDINET API から有報をダウンロードし、`0104010_*_ixbrl.htm` の役員情報（取締役系＋執行役系の両タグ系統、略歴含む）をパース
  - `EdinetDocResolver`: `executives.edinet_source_doc_id` キャッシュによるバッチ高速化（2回目以降は期末月前後30日のみスキャン）
  - `ExecutiveRepository`: 3テーブル（`executives` / `executive_communications` / `executive_evaluations`）のCRUD（`statements.db` に格納）
  - `CommunicationCollector`: WebSearch + 30日キャッシュで発信を収集、`PublishedDateExtractor` で URL HTML から JSON-LD/meta/time/URLパスで発信日を抽出
  - `ExecutiveEvaluator`: Claude LLM による6軸スコアリング（ビジョン一貫性・実行力・市場認識・リスク開示誠実性・コミュニケーション能力・**成長志向**）、スキーマ違反時は最大3回リトライ
- `/research-executives` スキル: 独立した `executive_report.md` 生成（4セクション構成: 役員サマリー / 役員別評価 / タイムライン / 主要発信引用集）
  - 期間定数: `LOOKBACK_DAYS_TOTAL=1095`（過去3年）、`HIGHLIGHT_DAYS_RECENT=365`（直近1年）
  - 検索キーワード: `SEARCH_KEYWORDS`（10語: インタビュー／講演／対談／コラム／ブログ／記事／寄稿／note／メッセージ／登壇）
  - 出力: `output/reports/stocks/YYYYMMDD-HHMM-{code}-analysis/executive_report.md`
- `scripts/run_executive_master_update.py`: 月次バッチ（全アクティブ銘柄の役員マスター更新、`--codes` / `--limit` / `--dry-run` 対応）
  - docID キャッシュ比較によるDLスキップ最適化（Phase F）: 一致時は XBRL ZIP DL・パース・upsert を全てスキップ（`status=unchanged`、Slack通知に「スキップ（有報未更新）」メトリクス）
- `scripts/run_research_executives.py`: CLIエントリ（`list-executives` / `build-report` サブコマンド、`--lookback-days` / `--highlight-days` 対応）
- マイグレーション:
  - `scripts/migrate_executives_add_career_column.py`: `executives` テーブルに `career_summary` カラム追加
  - `scripts/migrate_executives_add_growth_axis.py`: `executive_evaluations` テーブルに `growth_ambition` カラム追加
- `src/market_pipeline/config/settings.py`: `EdinetSettings`（`EDINET_API_KEY` 等）と `ExecutivesSettings`（`cache_ttl_days` / `max_parallel_fetch` / `doc_scan_fallback_months` / `doc_scan_narrow_days`）を追加
- `.env.example`: `EDINET_API_KEY` を追加
- 関連テスト: `test_edinet_executive_fetcher.py`, `test_executive_repository.py`, `test_executive_batch.py`, `test_communication_collector.py`, `test_executive_evaluator.py`, `test_executive_integration.py`, `test_published_date_extractor.py`
- `/analyze-stock --with-executive-research`: 既存の投資判断レポートに経営陣評価セクションを追加するオプション
- ウォッチリストニュース配信モジュール (`src/market_pipeline/news_delivery/`)
  - `WatchList` クラス（アトミック書込・CRUD・filter_by_tag/priority・`master.db` メタ解決）
  - `Deduplicator`（`delivered_news` テーブル + URL SHA256ハッシュPKでUPSERT、90日クリーンアップ）
  - `SlackFormatter`（Block Kit、銘柄ごとセクション、38000文字しきい値で分割）
  - `RateLimiter`（トークンバケット式）+ `RateLimitError`
  - `DeliveryService` / `build_default_service`（fetcher 合成、`RateLimitError` 時 priority=high のみ自動再試行）
  - フェッチャ群: `CdpDisclosureFetcher`（四季報CDP）/ `GoogleNewsRssFetcher`（Google News RSS）/ `TdnetRssFetcher`（yanoshin Atom）/ `ShikihoStockNewsFetcher`（四季報銘柄関連記事）/ `DisclosureFetcher`（旧HTTP版、テスト/レガシー）
- `scripts/run_news_delivery.py`: launchd 連携の Slack 配信スクリプト（`--slot {morning|noon|evening}`、`--dry-run`、`--lookback-days`、`--sources`）
- `scripts/watchlist.py`: ウォッチリストCRUD CLI（add / list / update / remove）
- `scripts/cleanup_news_db.py`: `delivered_news` クリーンアップスクリプト（デフォルト90日）
- `launchd/com.stock-analysis.news-delivery-{morning,noon,evening}.plist.template`: 平日 08:00 / 12:30 / 19:30 用 plist テンプレート（`RunAtLoad=false`）
- `config/news_sources.yaml`: ニュースカテゴリ拡張
  - `general_news`（Google News RSS、`query_template` / `max_items_per_code` / `exclude` フィルタ）
  - `ir_release`（yanoshin TDnet Atom）
  - `stock_news`（四季報銘柄ページ「この銘柄の関連記事」）
  - 旧 `financial` カテゴリは廃止（`stock_news` に統合）
- `src/market_pipeline/news/config_parser.py`: `NewsSource` に `query_template` / `max_items_per_code` フィールド追加
- `FilterKeywords` バリデーション緩和: `include` / `exclude` のいずれか少なくとも一方を指定（両方空はエラー）
- `src/market_pipeline/config/settings.py`: `NewsDeliverySettings` クラス追加（`STOCK_NEWS_LOOKBACK_DAYS`）
- `.env.example`: `STOCK_NEWS_SLACK_WEBHOOK_URL` / `STOCK_NEWS_QUIET_WHEN_EMPTY` / `STOCK_NEWS_LOOKBACK_DAYS` / `NEWS_DELIVERY_MAX_PARALLEL_FETCH`
- `pyproject.toml`: `playwright>=1.59.0`, `feedparser>=6.0.12` 依存関係追加
- `docs/core/launchd-operations.md`: ニュース配信ジョブ登録手順・スロットの考え方・`--sources` 選択・lookback設定・四季報初回ログイン手順（オプション）
- 新規テスト: `test_watchlist.py`, `test_news_delivery_dedupe.py`, `test_news_delivery_integration.py`, `test_news_formatter.py`, `test_rate_limiter.py`, `test_cdp_disclosure_fetcher.py`, `test_disclosure_fetcher.py`, `test_google_news_rss_fetcher.py`, `test_tdnet_rss_fetcher.py`, `test_shikiho_stock_news_fetcher.py`
---

## [0.9.0] - 2026-04-08

main ブランチに統合済みの最新リリース。yfinance 20年分の過去日足データ補完、`/analyze-stock` の Gemini Deep Research 統合、cash-neutral-PER バリュエーション、`backend/` → `src/` リネームを含む。

### Added
- `scripts/migrate_refetch_yfinance.py`: yfinanceデータを削除してauto_adjust=Falseで再取得するマイグレーションスクリプト（--dry-run, --symbols, --years対応）
- `scripts/migrate_rescale_yfinance.py`: yfinanceデータをJ-Quants境界比率でリスケールするマイグレーションスクリプト（--dry-run, --symbols対応）
- yfinance過去日足データ取得機能（J-Quants Light 5年分の範囲外を補完）
  - `src/market_pipeline/yfinance/historical_price_fetcher.py`: `HistoricalPriceFetcher`クラス新規追加
    - yfinanceから最大20年分の日足データを取得し、daily_quotesテーブルに挿入
    - J-Quantsデータの最古日以前のデータのみを取得（INSERT OR IGNOREでJ-Quants優先）
    - yfinance OHLCVをAdjustmentOpen/High/Low/Close/Volumeにマッピング（未調整カラムはNULL）
    - ThreadPoolExecutor + リトライ（最大3回、1秒間隔）
    - `--dry-run`, `--symbols`, `--years` オプション対応
  - `scripts/run_historical_prices.py`: 一括取得実行スクリプト（JobContext Slack通知統合）
  - `scripts/migrate_add_source_column.py`: daily_quotesテーブルにsourceカラム追加マイグレーション
    - 既存レコードは`source='jquants'`に一括UPDATE（10万件単位で分割）
    - 冪等性確保（カラム存在時はスキップ）
  - `tests/test_historical_price_fetcher.py`: HistoricalPriceFetcherの単体テスト
- `/analyze-stock` スキルにDeep Research統合を追加（Phase 2）
  - `--deep-research`: 確認プロンプトをスキップしてGemini Advanced Deep Researchを即実行
  - `--merge-deep-research`: 既存レポートにDeep Research結果を後から統合
  - Playwright MCP + CDP接続でGemini Advanced Web UIを自動操作
  - Deep Research結果を`base_report.md`のセクション2, 5, 7, 8に統合
  - 全エラーでPhase 1レポートを保全するフォールバック設計
- レポート出力先を`output/reports/stocks/YYYYMMDD-HHMM-{code}-analysis/`に移行
  - タイムスタンプ付きディレクトリで過去分析結果を保持
  - `base_report.md`, `deep_research_report.md`, `chart.png` を同一ディレクトリに格納
- StockScreener.filter()に`include`パラメータとカラム最小化を追加
  - `include`パラメータでカラムグループ("scores", "fundamentals", "valuation", "all")を指定可能
  - デフォルトで常時5カラム(date, code, long_name, sector, market_cap)のみ返却、フィルタ使用項目は自動追加
  - 出力カラム名は全てsnake_case
  - market_capはyfinance_valuation優先のCOALESCE(フォールバック: calculated_fundamentals)
  - calculated_fundamentals/yfinance_valuationを常時JOINし基本情報を確保
  - scores系テーブル(hl_ratio, relative_strength)は必要時のみJOIN(パフォーマンス維持)
- StockScreenerに自己資本比率・ROA・ROE上限フィルターを追加
  - `ScreenerFilter`に`equity_ratio_min/max`, `roa_min/max`, `roe_max`フィールド追加
  - `StockScreener.filter()`でequity_ratio, roa, roe_maxによるフィルタリングが可能に
  - テストDBフィクスチャをconftest.pyに共通化（`create_screener_analysis_db()`, `create_screener_statements_db()`）
- yfinanceバリュエーション指標取得・スクリーニング機能
  - `src/market_pipeline/yfinance/valuation_fetcher.py`: `ValuationFetcher`クラス新規追加
    - yfinance APIからBS情報（現金等・有利子負債）・時価総額・PERをローリング取得（デフォルト150銘柄/日）
    - 優先順位ロジック: BS未取得(PER低い順) → 90日経過(PER低い順) → 更新日古い順
    - `net_cash_ratio`（純ネットキャッシュ比率）、`cash_neutral_per`（キャッシュニュートラルPER）を自動計算
    - データ保存先: `statements.db` → `yfinance_valuation`テーブル
  - `src/technical_tools/screener.py`: StockScreenerにバリュエーションフィルタ追加
    - `ScreenerFilter`に`net_cash_ratio_min/max`, `cash_neutral_per_min/max`フィールド追加
    - `yfinance_valuation`テーブルの遅延JOINによるフィルタリング
  - `src/market_pipeline/config/settings.py`: `YFinanceSettings`にバリュエーション設定追加
    - `valuation_batch_size`, `valuation_wait_seconds`, `valuation_max_workers`
  - `scripts/run_daily_analysis.py`: `yfinance_valuation`モジュール追加（`--modules yfinance_valuation`で単独実行可）
  - `tests/test_valuation_fetcher.py`: ValuationFetcherの単体テスト（18テスト）
  - `tests/test_stock_screener.py`: バリュエーションフィルタのテスト追加
  - `.env.example`: `YFINANCE_VALUATION_*`環境変数追加
---

## [0.8.0] - 2026-03-03

ニュース駆動型銘柄発見スキル（Phase 1〜3）と財務分析セクションの拡張。

### Added
- ニュース駆動型銘柄発見 Phase 3: 適時開示情報対応と銘柄ニュース調査スキル
  - `FilterKeywords` dataclass: タイトルベースのキーワードフィルタリング設定（`include`/`exclude`リスト）
  - `NewsSource.filter_keywords` フィールド追加: 適時開示等のフィルタリング対応
  - `config/news_sources.yaml`: `disclosure`カテゴリ追加（会社四季報適時開示ページ、16個のinclude / 3個のexcludeキーワード）
  - `.claude/skills/research-stock-news/SKILL.md`: 銘柄ニュース調査スキル新規作成
  - `tests/test_news_config.py`: `FilterKeywords`バリデーション・パーステスト追加
- `src/market_pipeline/news/config_parser.py`: ニュース巡回先設定パーサーモジュール
  - `NewsSource` frozenデータクラス: 巡回先サイト情報（name, auth, url, selector等）
  - `NewsConfig`データクラス: カテゴリ別ソース管理
  - `load_config()`: YAML設定ファイルの読み込みとバリデーション
  - `get_sources_by_category()`: カテゴリ別ソース取得
  - `auth`フィールドのバリデーション（`none`または`cdp`のみ許可）
- `config/news_sources.yaml`: ニュース巡回先設定ファイル（news, analysis, financialカテゴリ）
- `tests/test_news_config.py`: ニュース巡回先設定パーサーのテスト
- `pyproject.toml`: `pyyaml>=6.0`, `types-PyYAML>=6.0`, `nbformat>=5.10.4` 依存関係追加
---

## [0.7.0] - 2026-02-10

Slack 通知統合と戦略最適化・バックテスト・仮想ポートフォリオ機能群の追加。

### Added
- `src/market_pipeline/utils/slack_notifier.py`: Slack Incoming Webhook通知モジュール
  - `SlackNotifier`クラス: 成功・エラー・警告の3種類の通知送信
  - `JobContext`コンテキストマネージャ: `with`文によるジョブの自動通知
  - `JobResult`データクラス: ジョブ実行結果（メトリクス、警告、実行時間）を保持
  - リトライロジック（最大3回、1秒間隔）
  - 通知失敗がジョブの処理結果に影響しない設計
- `src/market_pipeline/utils/__init__.py`: SlackNotifier, JobContext, JobResultのエクスポート
- `src/market_pipeline/config/settings.py`: `SlackSettings`クラス追加
  - `SLACK_WEBHOOK_URL`, `SLACK_ERROR_WEBHOOK_URL`, `SLACK_ENABLED`, `SLACK_TIMEOUT_SECONDS`, `SLACK_MAX_RETRIES`環境変数サポート
- `tests/test_slack_notifier.py`: SlackNotifier/JobContext/JobResultのテスト
- `.env.example`: Slack通知設定セクション追加
- `src/technical_tools/optimizer.py`: 戦略パラメータ最適化エンジン
  - `StrategyOptimizer`クラス: Backtesterを利用したパラメータ最適化
  - `add_search_space()`: 探索パラメータの定義（MA期間、RSI閾値、エグジットルール等）
  - `add_constraint()`: パラメータ制約条件の追加
  - `run()`: グリッドサーチ/ランダムサーチによる最適化実行
  - 並列処理対応（ThreadPoolExecutor）
  - ウォークフォワード分析（過学習対策）
  - タイムアウト機能
  - ストリーミング出力（JSONL形式）
- `src/technical_tools/optimization_results.py`: 最適化結果分析クラス
  - `OptimizationResults`: 結果の保持・分析・可視化
  - `TrialResult`: 個別試行結果データクラス
  - `best()`: 最良パラメータの取得
  - `top()`: 上位N件をDataFrameで取得
  - `plot_heatmap()`: パラメータ空間のヒートマップ可視化
  - `save()`/`load()`: JSON/CSV形式での永続化
  - `load_streaming()`: JSONL形式からの読み込み
- `src/technical_tools/exceptions.py`: 最適化関連例外追加
  - `OptimizerError`, `InvalidSearchSpaceError`, `NoValidParametersError`, `OptimizationTimeoutError`
- テストファイル追加:
  - `tests/test_optimizer.py`: StrategyOptimizerクラステスト
  - `tests/test_optimization_results.py`: OptimizationResultsクラステスト
- `src/technical_tools/backtester.py`: シグナルベースバックテストエンジン
  - `Backtester`クラス: backtesting.pyをラップしたシンプルなAPI
  - `add_signal()`: プラグイン形式のシグナル追加
  - `add_exit_rule()`: エグジットルール追加（stop_loss, take_profit, max_holding_days, trailing_stop）
  - `run()`: 並列処理対応のバックテスト実行
  - `run_with_screener()`: StockScreener連携バックテスト
- `src/technical_tools/backtest_results.py`: バックテスト結果分析クラス
  - `BacktestResults`: 結果の保持・分析・可視化
  - `summary()`: パフォーマンス指標（勝率、シャープレシオ、最大DD等）
  - `plot()`: plotlyによる資産推移・ドローダウンチャート
  - `export()`: CSV/Excel/HTML出力
  - `by_symbol()`, `by_sector()`, `monthly_returns()`, `yearly_returns()`: 詳細分析
  - `Trade`データクラス: 個別取引情報
- `src/technical_tools/virtual_portfolio.py`: 仮想ポートフォリオ管理
  - `VirtualPortfolio`クラス: JSON永続化対応の仮想ポートフォリオ
  - `buy()`: 株数指定または金額指定での購入
  - `sell()`, `sell_all()`: 売却
  - `summary()`, `holdings()`, `performance()`: 状態確認
  - `plot()`: plotlyによるポートフォリオチャート
  - `buy_from_screener()`: StockScreener連携の一括購入
- `src/technical_tools/backtest_signals/`: バックテスト用シグナルモジュール
  - `BaseSignal`: シグナル抽象基底クラス
  - `SignalRegistry`: シグナルのプラグイン登録・取得
  - 対応シグナル: golden_cross, dead_cross, rsi_oversold, rsi_overbought, macd_cross, bollinger_breakout, bollinger_squeeze, volume_spike, volume_breakout
- `src/technical_tools/exceptions.py`: バックテスト・ポートフォリオ関連例外追加
  - `BacktestError`, `BacktestInsufficientDataError`, `InvalidSignalError`, `InvalidRuleError`, `PortfolioError`
- テストファイル追加:
  - `tests/test_backtester.py`: Backtesterクラステスト
  - `tests/test_backtest_results.py`: BacktestResultsクラステスト
  - `tests/test_backtest_signals.py`: バックテストシグナルテスト
  - `tests/test_virtual_portfolio.py`: VirtualPortfolioクラステスト
- `data/portfolios/`: VirtualPortfolio用JSONファイル格納ディレクトリ（.gitignore追加）
- `pyproject.toml`: `backtesting>=0.3.3` 依存関係追加

---

## 横断的な変更履歴 (Changed / Fixed / Documentation)

> 以下のエントリは複数バージョンに跨る変更を時系列順に記録したもの。バージョン区切り済みの「Added」セクションと併読すること。

### Changed
- 日次パイプラインをチェーン実行に変更（DB競合回避、[0.9.0]）
  - `run_daily_jquants.py` → `run_daily_analysis.py` → `integrated_analysis2.py` を順序実行
  - launchd は `daily-jquants`（18:00）のみ。`daily-analysis`（18:30）/ `integrated-analysis`（19:00）は廃止
  - `--no-chain` フラグで個別実行可能（両スクリプト対応）
- `JQuantsDataProcessor`: APIタイムアウト・並列数の最適化
  - `timeout_seconds`: 30秒 → 10秒（タイムアウト時は1回リトライ）
  - `max_concurrent_requests`: 3 → 10
  - `request_delay`: 0.1秒 → 0.05秒
  - `get_daily_quotes_async()`: `asyncio.TimeoutError` 個別ハンドリング + リトライ
  - 空データコードのバッチ内サマリーログ追加
- `JQuantsSettings`（settings.py）: デフォルト値を `max_concurrent_requests=10`, `request_delay=0.05`, `timeout_seconds=10` に変更
- `HistoricalPriceFetcher.map_columns()`: `auto_adjust=False`に変更し、生OHLCV + Adj Close比率による調整済み価格を正しく格納
  - 生OHLCV → Open/High/Low/Close/Volume
  - AdjustmentOpen/High/Low = 生値 × (Adj Close / Close)
  - AdjustmentClose = Adj Close
- `JQuantsDataProcessor.get_all_prices_for_past_5_years_to_db_optimized()`: `Dict[str, int]`を返却（total_listed, codes_to_update, codes_updated, records_inserted, codes_failed）
- `JQuantsDataProcessor.update_prices_to_db_optimized()`: 同上
- `JQuantsDataProcessor.get_listed_info_cached()`: MIN_EXPECTED_COMPANIES（100社）を下回るキャッシュ/API結果に対する検証ロジックを追加
- `run_daily_jquants.py`: ジョブ実績メトリクス（対象銘柄数、更新銘柄数、新規レコード数、失敗銘柄数）をSlack通知に追加
- StockScreener `pattern_window` パラメータ: `WindowInput`型を導入
  - int（累積）、tuple[int,int]（スライス (240,480)等）、str（"240-480", "w240_480", "pattern_w240_480"）に対応
  - list[WindowInput]によるマルチウィンドウANDフィルタ
  - マルチウィンドウ時のワイド形式ピボット出力（pattern_w240_480, score_w240_480 等）
- `PATTERN_LABELS` 定数をtechnical_toolsパッケージからエクスポート
- チャートパターン分類の「不明」ラベルを廃止
  - r < 0.3でもベストマッチのラベルをそのまま保存（スコアで信頼度を判断）
  - `MIN_CORRELATION_THRESHOLD`による上書きロジックを削除
  - `PATTERN_LABELS`から「不明」を除外
- StockScreenerの`pattern_labels`フィルタロジックを修正
  - `pattern_window="all"` + `pattern_labels`: 存在する全ウィンドウがマッチする銘柄のみ（AND、NaN無視）
  - `pattern_window=[list]` + `pattern_labels`: 指定全ウィンドウでAND条件
  - 単一ウィンドウ指定: 従来通りセル単位フィルタ
  - "all"モードの欠損ウィンドウ補完を`pd.NA`から`np.nan`に統一
- チャートパターン分類の中長期ウィンドウを期間スライスに変更
  - 累積ウィンドウ（直近N日）: 20/60/120/240日（変更なし）
  - スライスウィンドウ（期間指定）: (240,480)/(480,1200)/(1200,2400)/(2400,4800)日前
  - 全期間ウィンドウを廃止し、各期間が独立した分析結果を返す設計に変更
  - `WindowSpec = int | tuple[int, int]` 型定義を導入
  - `classify_window()` メソッドを新規追加（累積/スライス両対応）
  - `window_spec_to_db_value()` / `db_value_to_window_spec()` 変換関数を追加
  - スライスウィンドウのDB保存値: `start * 10000 + end`（例: 2400480）
  - 対数正規化（`np.log()` + MinMaxScaler）を導入し、急騰・急落による歪みを軽減
  - NaN対策: dropna + 50%最低データ閾値
  - 信頼度閾値: Pearson相関 r < 0.3 で「不明」ラベルを付与 → 後に廃止（ベストマッチを保持する設計に変更）
  - StockScreenerのピボットカラム名をスライス対応（例: `pattern_w240_480`）
- チャートパターン分類の長期ウィンドウを刷新
  - 960日ウィンドウを廃止し、480日（2年）ウィンドウに置き換え
  - 2400日（10年）・4800日（20年）ウィンドウを追加
  - 銘柄ごとの全期間（上場来）ウィンドウを動的に追加（データ長>4800日の場合）
  - データ読み込み範囲を1500日→6000日に拡大（20年分対応）
  - `settings.py`の`chart_long_windows`を`[480, 1200, 2400, 4800]`に更新
- `src/market_pipeline/jquants/data_processor.py`: J-Quantsデータ挿入時に`source='jquants'`を付与
- ソースコードディレクトリを `backend/` から `src/` にリネーム
  - `backend/market_pipeline/` → `src/market_pipeline/`
  - `backend/__init__.py` → `src/__init__.py`
  - pyproject.toml、テスト設定、mypy設定を `src/` に更新
  - 全Claude Codeスキルファイル（9ファイル）の `backend/` パス参照を `src/` に更新
  - コアドキュメント（CLAUDE.md, README.md, docs/core/）を `src/` パスに更新
- Backtester.run_with_screener()、VirtualPortfolio.buy_from_screener()のカラム参照を`Code`→`code`に修正（StockScreenerのsnake_case出力に追従）
- `.claude/skills/analyze-stock/SKILL.md`: レポートテンプレートに財務状況（2.2）・キャッシュフロー（2.3）・ネットキャッシュ分析（2.4）セクションを追加、四季報データ抽出JSを強化
- `.claude/skills/analyze-stock/SKILL.md`: レポート構成を8セクションに拡張（事業構造・セグメント分析、四季報ライバル比較テーブル、gemini CLIプロンプト強化）
  - 新セクション「2. 事業構造・セグメント分析」追加（セグメント別売上・利益構成、成長性・競争力、CAGR）
  - 四季報ライバル比較セクションからデータ抽出（`browser_run_code` JavaScript拡張）
  - gemini CLIプロンプトを5→8セクション構成に拡張（セグメント分析・プロダクト別競争力評価・セグメント別成長性追加）
  - 投資判断サマリーにセグメント分析・成長性の判断根拠を追加
- `/analyze-stock` チャートPNG画像生成機能
  - `TechnicalAnalyzer.plot_chart()` + `fig.write_image()` でローソク足+SMA+RSI+MACD+GC/DCシグナルのチャートPNG画像を自動生成
  - 保存先: `docs/reports/stocks/images/{code}-chart-YYYYMMDD-HHMM.png`（1200x800px）
  - `kaleido>=1.0.0` をオプショナル依存（`chart-export`）として追加
  - チャート生成失敗時はスキップしてテキストのみのレポートを生成（エラー耐性）
  - `.gitignore` に `docs/reports/stocks/images/*.png` を追加
- `.claude/skills/discover-stocks/SKILL.md`: 適時開示巡回ステップ追加（`--category disclosure`対応）
- `.claude/skills/analyze-stock/SKILL.md`: `/research-stock-news`相当のニュース情報を自動統合
- `src/market_pipeline/news/__init__.py`: `FilterKeywords`エクスポート追加
- `scripts/run_daily_jquants.py`: JobContext統合（レコード数・銘柄数・データ期間を通知）
- `scripts/run_daily_analysis.py`: JobContext統合（対象日・実行モジュールを通知）
- `scripts/run_weekly_tasks.py`: JobContext統合（財務データ・統合分析の完了状況を通知）
- `scripts/run_monthly_master.py`: JobContext統合（総銘柄数・アクティブ銘柄数を通知）
- `src/technical_tools/__init__.py`: StrategyOptimizer, OptimizationResults, TrialResult, 最適化例外クラスをエクスポート追加
- `src/technical_tools/__init__.py`: Backtester, BacktestResults, Trade, VirtualPortfolio, 新例外クラスをエクスポート追加
- パッケージバージョンを0.2.0に統一（pyproject.toml, `__init__.py`）

### Fixed
- `src/technical_tools/data_sources/jquants.py`: 株式分割を考慮した調整後価格（AdjustmentOpen/High/Low/Close/Volume）を使用するように修正
  - 以前は未調整価格（Open/High/Low/Close/Volume）を使用していたため、株式分割時にチャートにギャップが生じていた

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
- `src/market_reader/` パッケージ: pandas_datareader風のデータアクセスAPI（旧 `stock_reader/`）
  - `DataReader`クラス: コンストラクタで `db_path` と `strict` パラメータをサポート
  - `get_prices()`: 単一/複数銘柄対応、日付自動デフォルト（end=DB最新日、start=5年前）
  - カラム選択: "simple"（OHLCV + AdjustmentClose）、"full"（全16カラム）、カスタムリスト
  - 4/5桁コード自動正規化（出力は常に4桁）
  - カスタム例外クラス: `StockReaderError`, `StockNotFoundError`, `DatabaseConnectionError`, `InvalidDateRangeError`
  - ユーティリティ関数: `normalize_code()`, `to_5digit_code()`, `validate_date()`
  - PRAGMA最適化（WALモード、キャッシュ設定）
- `notebooks/` ディレクトリ: 分析・可視化用Jupyterノートブック
- `py.typed` マーカーファイル: PEP 561準拠の型ヒントサポート
  - `src/market_pipeline/py.typed`
  - `src/market_reader/py.typed`
- パフォーマンスベンチマークファイル（テストから分離）:
  - `tests/benchmark_integrated_analysis_optimization.py`
  - `tests/benchmark_jquants_performance.py`
  - `tests/benchmark_optimizations.py`

### Changed
- yfinanceからJ-Quantsデータ計算への切り替え (`refactor/yfinance-to-jquants`ブランチ)
- FundamentalsCalculator: J-Quantsデータを使用した財務指標計算への移行
- ドキュメント構造の再編成（`docs/refs/`、`docs/core/`）
- リポジトリ構造のリファクタリング:
  - `core/` → `src/market_pipeline/` へ移動
  - `stock_reader/` → `src/market_reader/` へ移動
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

本プロジェクトは [Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) と[セマンティックバージョニング](https://semver.org/lang/ja/) に準拠して履歴を記録する:

- MAJOR (1.0.0 移行時): 後方互換性のないAPI変更
- MINOR (0.x.0): 後方互換性のある機能追加
- PATCH (0.x.y): 後方互換性のあるバグ修正

リリース履歴:
- [0.7.0] - 2026-02-10: Slack通知 + Optimizer / Backtester / VirtualPortfolio
- [0.8.0] - 2026-03-03: ニュース駆動型銘柄発見スキル + 財務分析拡張
- [0.9.0] - 2026-04-08: yfinance 20年分過去日足 + Deep Research + cash-neutral-PER + `src/` リネーム
- [Unreleased]: 経営陣評価モジュール + ウォッチリストニュース配信（次バージョン [0.10.0] 予定）
