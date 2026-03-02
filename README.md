# Stock-Analysis (株式データ分析プロジェクト)

## 概要

このプロジェクトは、日本の株式市場に関するデータを収集、保存、分析するためのツール群です。J-Quants APIを利用して、日々の株価、財務諸表データ、銘柄マスターなどを自動で取得・更新し、SQLiteデータベースに格納します。

収集したデータは、様々な分析手法（ミネルヴィニ戦略、高値・安値比率、相対力、チャートパターン分類など）を用いて評価され、統合的な視点から銘柄選定や投資戦略の構築に活用できます。

## 主な機能

- **データ収集と管理:**
    - **日次株価取得 (J-Quants):** 平日の夜間にJ-Quants APIから最新の株価四本値（始値, 高値, 安値, 終値）および出来高を取得し、`data/jquants.db` に保存します。
    - **週次財務データ取得 (J-Quants Statements):** 週末にJ-Quants Statements APIから財務諸表データ（売上高、利益、EPS、BPS等）を取得し、PER・PBR・ROE等の財務指標を計算して `data/statements.db` に保存します。
    - **月次マスターデータ更新:** 毎月1回、最新の銘柄一覧（マスターデータ）を更新し、`data/master.db` に保存します。

- **株式分析:**
    - **ミネルヴィニ戦略:** マーク・ミネルヴィニの投資基準に基づき、銘柄のトレンドと強さを評価します。
    - **高値・安値比率 (HL Ratio):** 過去一定期間の高値と安値に対する現在の株価の位置を評価し、買われすぎ・売られすぎを判断します。
    - **相対力 (Relative Strength):** 市場全体や他の銘柄と比較した株価の相対的な強さを評価します。
    - **チャートパターン分類:** 機械学習を用いて、株価チャートの形状を自動的に分類し、特定のパターン（上昇、下落、もみ合いなど）を識別します。データの可用性に基づいて動的にウィンドウサイズを選択（1200日または960日の長期パターン分析を含む）し、高性能なバッチ処理により効率的に実行されます。
    - **統合分析:** 上記の各分析結果を組み合わせ、複合的なスコアや条件フィルタリングにより、多角的な視点から銘柄を評価します。

- **ニュース駆動型銘柄発見 (`/discover-stocks`):**
    - ニュースや分析記事から有望銘柄を自動抽出するClaude Codeスキルです。
    - Playwright MCPでニュースサイト（日経電子版、Reuters Japan）や分析サイト（トウシル、会社四季報オンライン）を巡回し、銘柄コード・推奨理由を抽出します。
    - 裏付け情報の収集とリスク分析を経て、候補銘柄レポートを `docs/reports/adhoc/` に出力します。
    - テーマ絞り込み（例: AI、半導体）やカテゴリ・期間指定に対応しています。
    - 巡回先設定は `config/news_sources.yaml` で管理されます。

- **銘柄ニュース調査 (`/research-stock-news`):**
    - 特定銘柄のニュース・適時開示・IR情報を包括的に調査するClaude Codeスキルです。
    - 四季報適時開示ページ、四季報銘柄ページのニュースタブ、WebSearchから情報を収集し、レポートを `docs/reports/stocks/` に出力します。

- **銘柄詳細分析 (`/analyze-stock`):**
    - 銘柄コードまたはPhase 1候補リストから、企業分析・財務分析・テクニカル分析を統合した投資判断レポートを生成します。
    - 会社四季報銘柄ページ（CDP経由）、企業IR情報、既存テクニカルツール（StockScreener, TechnicalAnalyzer, DataReader）を情報ソースとして活用します。
    - 5段階投資判断評価（強気/やや強気/中立/やや弱気/弱気）を含むレポートを `docs/reports/stocks/` に出力します。

## 分析機能の詳細

`backend/market_pipeline/analysis` ディレクトリには、以下の分析プログラムが含まれており、それぞれが特定の分析手法を実装しています。

- **`minervini.py`**: マーク・ミネルヴィニの株式スクリーニング戦略を実装しています。株価と移動平均線の関係、52週高値・安値からの乖離率などを基に、銘柄のトレンドと強さを評価します。計算結果は `data/analysis_results.db` の `minervini` テーブルに保存されます。
- **`high_low_ratio.py`**: 銘柄の高値・安値比率を計算するロジックです。過去52週間の高値と安値の範囲内で、現在の株価がどの位置にあるかを示します。結果は `data/analysis_results.db` の `hl_ratio` テーブルに保存されます。
- **`relative_strength.py`**: 相対力（Relative Strength Percentage: RSP）および相対力指数（Relative Strength Index: RSI）を計算するロジックです。銘柄の市場に対する相対的なパフォーマンスを評価します。結果は `data/analysis_results.db` の `relative_strength` テーブルに保存されます。
- **`chart_classification.py`**: 株価チャートのパターンを分類するロジックです。過去の株価データから特定の期間（20日、60日、120日、240日、および動的選択される960日または1200日）のチャート形状を抽出し、「上昇」「下落」「もみ合い」などのパターンに分類します。データの可用性に基づいて自動的に長期ウィンドウ（1200日 ≥ 960日）を選択し、バッチ処理による高性能な分析を提供します。結果は `data/analysis_results.db` の `classification_results` テーブルに保存されます。
- **`integrated_analysis.py`**: 上記の各分析プログラムによって生成された結果（`hl_ratio`, `minervini`, `relative_strength`, `classification_results`）を `data/analysis_results.db` から読み込み、それらを統合して複合的な評価を行うためのユーティリティ関数を提供します。これにより、複数の指標を横断的に評価し、より精度の高い銘柄選定を支援します。このスクリプト自体はデータを生成せず、既存のデータをクエリ・集計します。
- **`integrated_analysis2.py`**: `integrated_analysis.py` の機能を利用し、各分析結果と財務指標データを統合して、最終的な分析結果をExcelファイルとして `output` フォルダに出力します。

## ディレクトリ構成

```
.
├── backend/             # バックエンドパッケージ群
│   ├── market_pipeline/ # データ処理のコアロジック（旧core/）
│   │   ├── analysis/    # 各種分析ロジック
│   │   ├── config/      # 設定管理（Pydantic Settings）
│   │   ├── jquants/     # J-Quants API関連の処理（株価・財務諸表）
│   │   ├── master/      # 銘柄マスター関連の処理
│   │   ├── news/        # ニュース巡回設定（YAML設定パーサー）
│   │   ├── utils/       # ユーティリティ（キャッシュ、並列処理、Slack通知等）
│   │   └── yfinance/    # yfinance連携（レガシー）
│   ├── market_reader/   # pandas_datareader風のデータアクセスAPI（旧stock_reader/）
│   └── technical_tools/ # Jupyter Notebook用テクニカル分析ツール
├── data/                # データベースファイル（.sqlite, .db）を格納
├── logs/                # launchdジョブの実行ログ
├── output/              # 分析結果のExcelファイルやエラーログなどを格納
├── notebooks/           # Jupyter Notebook（分析・可視化用）
├── scripts/             # 定期実行用のスクリプト群
├── tests/               # テストコード
├── config/              # 設定ファイル
│   └── news_sources.yaml # ニュース巡回先設定
├── docs/                # ドキュメント
│   ├── core/            # コア設計ドキュメント
│   ├── reports/         # レポート出力
│   │   ├── adhoc/       # アドホック分析レポート（discover-stocks等）
│   │   └── stocks/      # 銘柄詳細分析レポート（analyze-stock出力）
│   │       └── images/  # チャートPNG画像
│   └── refs/            # 参考資料
├── pyproject.toml       # プロジェクトの依存関係定義
└── README.md            # このファイル
```

## データベース構造

このプロジェクトでは、主に以下のSQLiteデータベースファイルを使用します。

*   **`data/jquants.db`**:
    *   J-Quants APIから取得した日次株価データ（`daily_quotes`テーブル）が格納されます。
*   **`data/statements.db`**:
    *   J-Quants Statements APIから取得した財務諸表データと、計算済み財務指標が格納されます。
    *   主なテーブル:
        *   `financial_statements`: 生の財務諸表データ（売上高、利益、EPS、BPS、キャッシュフロー等）
        *   `calculated_fundamentals`: 計算済み財務指標（PER、PBR、ROE、ROA、配当利回り等）
*   **`data/master.db`**:
    *   銘柄マスターデータ（`stocks_master`テーブルなど）が格納されます。
*   **`data/analysis_results.db`**:
    *   各種分析プログラム（`minervini.py`, `high_low_ratio.py`, `relative_strength.py`, `chart_classification.py`）によって計算された結果が格納されます。
    *   主なテーブル:
        *   `minervini`: ミネルヴィニ戦略の評価結果
        *   `hl_ratio`: 高値・安値比率の計算結果
        *   `relative_strength`: 相対力（RSP, RSI）の計算結果
        *   `classification_results`: チャートパターン分類の結果
        *   `integrated_scores`: 統合スコアと順位（日次蓄積）

## セットアップ方法

1.  **リポジトリのクローン:**
    ```bash
    git clone <repository_url>
    cd Stock-Analysis
    ```

2.  **Python環境:**
    このプロジェクトは Python 3.10 以上を要求します。

3.  **依存ライブラリのインストール:**
    `uv` を使用して、必要なライブラリをインストールします。
    ```bash
    uv sync

    # チャートPNG出力が必要な場合（オプション）
    uv sync --extra chart-export
    ```

4.  **環境変数の設定:**
    J-Quants APIを利用するために、認証情報の設定が必要です。プロジェクトルートに `.env` ファイルを作成し、以下の内容を記述してください。
    ```
    EMAIL="your_jquants_email@example.com"
    PASSWORD="your_jquants_password"
    ```

## 使用方法

### 手動での実行

各スクリプトを直接実行することで、任意のタイミングでデータ取得や分析を実行できます。

- **J-Quants日次データ取得:**
    J-Quants APIから日次株価データを取得します。
    ```bash
    python scripts/run_daily_jquants.py
    ```

- **週次タスク (財務データ取得 & 統合分析):**
    J-Quants Statements APIから財務諸表データを取得し、財務指標を計算、統合分析を実行します。
    ```bash
    # 全タスク実行
    python scripts/run_weekly_tasks.py

    # 財務データ取得のみ
    python scripts/run_weekly_tasks.py --statements-only

    # 統合分析のみ
    python scripts/run_weekly_tasks.py --analysis-only
    ```

- **月次マスターデータ更新:**
    銘柄マスターデータを更新します。
    ```bash
    python scripts/run_monthly_master.py
    ```

- **日次分析フロー:**
    高値・安値比率、ミネルヴィニ戦略、相対力（RSP/RSI）の計算とデータベースへの保存、およびその日の分析サマリーのログ出力を実行します。
    ```bash
    python scripts/run_daily_analysis.py
    ```

- **統合分析:**
    `integrated_analysis2.py` を直接実行することで、統合分析を行い、結果をExcelファイルに出力します。
    ```bash
    python backend/market_pipeline/analysis/integrated_analysis2.py
    ```

- **チャートパターン分類:**
    チャートパターンの分類分析を実行します。複数の実行モードが利用可能です。
    ```bash
    # サンプル実行（基本的なパターン分析）
    python backend/market_pipeline/analysis/chart_classification.py --mode sample

    # アダプティブウィンドウのサンプル実行（1200/960日の動的選択をテスト）
    python backend/market_pipeline/analysis/chart_classification.py --mode sample-adaptive

    # 全銘柄での高性能分析（アダプティブウィンドウ付き）
    python backend/market_pipeline/analysis/chart_classification.py --mode full-optimized

    # バッチサイズの調整（メモリ使用量の制御）
    python backend/market_pipeline/analysis/chart_classification.py --mode full --batch-size 50
    ```

### launchdによる自動実行

macOS launchdを使用してデータ取得・更新・分析を自動化しています。スリープ復帰後にも自動実行されます。

**スケジュール:**

| スクリプト | 実行タイミング | 説明 |
|-----------|--------------|------|
| `run_daily_jquants.py` | 平日 18:00 | J-Quants APIから株価取得 |
| `run_daily_analysis.py` | 平日 18:30 | 日次分析実行 |
| `run_adhoc_integrated_analysis.py` | 平日 19:00 | アドホック統合分析 |
| `run_weekly_tasks.py` | 土曜 06:00 | 財務諸表取得 + 統合分析 |
| `run_monthly_master.py` | 毎月1日 20:30 | マスターデータ更新 |

**管理:** `~/.local/share/launchd/setup.sh --status`

### Slack通知

launchdスクリプトの実行結果をSlack Incoming Webhookで自動通知します。4つのlaunchdスクリプト全てに統合済みです。

**設定:** `.env`ファイルに以下を追加:

```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
# SLACK_ERROR_WEBHOOK_URL=           # オプション: エラー専用チャンネル
# SLACK_ENABLED=true                 # false で通知無効化
# SLACK_TIMEOUT_SECONDS=10
# SLACK_MAX_RETRIES=3
```

**特徴:**
- `SLACK_WEBHOOK_URL`未設定時は通知をスキップ（エラーなく動作）
- 通知失敗がジョブの処理結果に影響しない
- リトライロジック（最大3回、1秒間隔）
- 成功時はメトリクス（レコード数、銘柄数等）を含む通知を送信
- エラー時はトレースバック情報を含む通知を送信

## ニュース駆動型銘柄発見 (`/discover-stocks`)

ニュースや分析記事から有望銘柄を抽出するClaude Codeスキルです。Playwright MCPでサイトを巡回し、銘柄コード・推奨理由を抽出、裏付け情報収集とリスク分析を経てレポートを生成します。

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

**巡回先カテゴリ（`config/news_sources.yaml`で管理）:**
- `news`: ニュースサイト（日経電子版, Reuters Japan）
- `analysis`: 分析サイト（トウシル, 会社四季報オンライン）
- `disclosure`: 適時開示情報（会社四季報、`filter_keywords`によるフィルタリング）
- `financial`: 個別銘柄ページ（Phase 2用）

**認証方式:**
- `auth: cdp` — Chrome DevTools Protocol経由（要: `open -a 'Google Chrome' --args --remote-debugging-port=9222`）
- `auth: none` — Playwright MCPで直接アクセス

**構成ファイル:**
- `config/news_sources.yaml`: 巡回先サイト設定（カテゴリ別）
- `.claude/skills/discover-stocks/SKILL.md`: スキル定義
- `backend/market_pipeline/news/config_parser.py`: YAML設定パーサー
- `docs/reports/adhoc/`: レポート出力先

## 銘柄ニュース調査 (`/research-stock-news`)

特定銘柄のニュース・適時開示・IR情報を包括的に調査し、レポートを生成するClaude Codeスキルです。

```bash
# 銘柄コード指定
/research-stock-news 4443

# 複数銘柄
/research-stock-news 4443 7203

# 期間指定
/research-stock-news 4443 --from 2026-02-01 --to 2026-02-28
```

**情報ソース:**
- 四季報適時開示ページ（`auth: none`、`?qtext={code}`で銘柄絞り込み）
- 四季報銘柄ページのニュースタブ（CDP経由）
- WebSearchによる企業IR・一般ニュース

**構成ファイル:**
- `.claude/skills/research-stock-news/SKILL.md`: スキル定義
- `docs/reports/stocks/`: レポート出力先（`{code}-news.md`）

## 銘柄詳細分析 (`/analyze-stock`)

銘柄コードまたはPhase 1候補リストから、企業分析・財務分析・テクニカル分析を統合した投資判断レポートを生成します。

```bash
# 銘柄コード直接指定
/analyze-stock 7203

# 複数銘柄の一括分析
/analyze-stock 7203 9984

# Phase 1候補リストから全銘柄を分析
/analyze-stock --from-report docs/reports/adhoc/2026-02-28-candidates.md

# Phase 1候補リストから特定銘柄のみ分析
/analyze-stock --from-report docs/reports/adhoc/2026-02-28-candidates.md 7203 9984
```

**情報ソース:**
- 会社四季報銘柄ページ（CDP経由、フォールバック: WebSearch）
- 企業IR情報・業界動向（WebSearch）
- 既存テクニカルツール（StockScreener, TechnicalAnalyzer, DataReader）

**レポート内容:**
- 企業概要、財務分析（PER/PBR/ROE等）、財務状況（総資産/自己資本/有利子負債等）、キャッシュフロー、ネットキャッシュ分析（ネットキャッシュ/ネットキャッシュ比率/キャッシュニュートラルPER）、テクニカル分析（統合スコア/Minervini/RSP）
- 株価チャートPNG画像（ローソク足+SMA+RSI+MACD+GC/DCシグナル、直近2年、`docs/reports/stocks/images/{code}-chart.png`）
- 業界・競合分析、リスク要因
- 直近の適時開示・ニュース（`/research-stock-news`相当の情報を自動統合）
- 投資判断サマリー（5段階評価: 強気/やや強気/中立/やや弱気/弱気）

**チャート生成依存:** `kaleido`（オプショナル）。未インストール時はチャート生成をスキップし、テキストのみのレポートを生成する。

**構成ファイル:**
- `.claude/skills/analyze-stock/SKILL.md`: スキル定義
- `docs/reports/stocks/`: レポート出力先（`images/`サブディレクトリにチャートPNG画像）

## Market Reader パッケージ

pandas_datareader風のインターフェースでJ-Quantsの株価データにアクセスできます。

```python
from market_reader import DataReader

reader = DataReader()  # デフォルトDB設定を使用
# または明示的にパスとstrictモードを指定
reader = DataReader(db_path="data/jquants.db", strict=True)

# 単一銘柄の取得（Dateインデックス）
df = reader.get_prices("7203", start="2024-01-01", end="2024-12-31")

# 複数銘柄の取得（(Date, Code) MultiIndex DataFrame）
df = reader.get_prices(["7203", "9984"], start="2024-01-01", end="2024-12-31")

# カラム選択: "simple"（デフォルト）, "full", またはリスト
df = reader.get_prices("7203", columns=["Open", "Close"])
```

**機能:**
- 日付の自動デフォルト（end=DB内最新日、start=endの5年前）
- 4/5桁コードの正規化（出力は常に4桁）
- `strict=True`で例外発生、`strict=False`（デフォルト）で空DataFrameと警告
- 読み取りパフォーマンス最適化（WALモード、PRAGMA設定）

**例外クラス:**
- `StockReaderError`: 基底例外
- `StockNotFoundError`: 銘柄が見つからない（strict=True時）
- `DatabaseConnectionError`: DB接続エラー
- `InvalidDateRangeError`: 日付範囲エラー（strict=True時）

## Technical Tools パッケージ

Jupyter Notebook向けのテクニカル分析ツールで、日本株（J-Quants）と米国株（yfinance）の統一インターフェースを提供します。

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
- テクニカル指標計算（SMA, EMA, RSI, MACD, Bollinger Bands）
- ゴールデンクロス/デッドクロス自動検出
- plotlyによるインタラクティブチャート
- 既存分析結果（Minervini, RSP）との連携

**例外クラス:**
- `TechnicalToolsError`: 基底例外
- `DataSourceError`: データ取得エラー
- `TickerNotFoundError`: 銘柄が見つからない
- `InsufficientDataError`: データ不足

## Backtester（バックテスト）

シグナルベースのバックテストを実行し、投資戦略の有効性を評価します。

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

# スクリーナー連携バックテスト
results = bt.run_with_screener(
    screener_filter={"composite_score_min": 70, "hl_ratio_min": 80},
    start="2023-01-01",
    end="2024-12-31",
    exit_rules={"stop_loss": -0.10, "take_profit": 0.20}
)

# レポート出力
results.export("report.xlsx")  # Excel出力
results.export("report.csv")   # CSV出力
results.export("report.html")  # HTML出力
```

**対応シグナル:**
- `golden_cross`: ゴールデンクロス（短期MAが長期MAを上抜け）
- `dead_cross`: デッドクロス（短期MAが長期MAを下抜け）
- `rsi_oversold`: RSI売られすぎ（RSIがthreshold以下）
- `rsi_overbought`: RSI買われすぎ（RSIがthreshold以上）
- `macd_cross`: MACDクロス（MACD線がシグナル線を上抜け）
- `bollinger_breakout`: ボリンジャーバンドブレイクアウト
- `bollinger_squeeze`: ボリンジャースクイーズ
- `volume_spike`: 出来高急増
- `volume_breakout`: 出来高確認付きブレイクアウト

**対応エグジットルール:**
- `stop_loss`: 損切り（threshold: 負の値、例: -0.10）
- `take_profit`: 利確（threshold: 正の値、例: 0.20）
- `max_holding_days`: 最大保有日数
- `trailing_stop`: トレーリングストップ

## VirtualPortfolio（仮想ポートフォリオ）

仮想ポートフォリオを作成し、パフォーマンスを追跡します。

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

# スクリーナー結果から一括購入
vp.buy_from_screener(
    screener_filter={"composite_score_min": 80},
    amount_per_stock=100000,  # 各銘柄10万円
    max_stocks=10
)
```

**機能:**
- JSON永続化（data/portfolios/*.json）
- 平均取得単価の自動計算
- スクリーナー結果からの一括銘柄追加
- 現在価格はmarket_readerから自動取得
- 取引履歴の記録
- plotlyによるインタラクティブチャート

## StrategyOptimizer（戦略最適化）

投資戦略のパラメータを自動最適化し、最適な戦略を発見します。

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

## Stock Screener（銘柄スクリーニング）

統合分析結果をDBから取得し、柔軟にフィルタリングするツールです。

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
    roe_min=15.0
)

# ScreenerFilterオブジェクトを使用（パラメータの構造化）
config = ScreenerFilter(
    composite_score_min=70.0,
    market_cap_min=100_000_000_000,
    per_max=15.0,
)
results = screener.filter(config)

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
- テクニカル指標（HlRatio, RSI）でのフィルタリング
- 財務指標（時価総額、PER、PBR、ROE、配当利回り）でのフィルタリング
- チャートパターン（60日/120日など）でのフィルタリング
- 順位変動分析（rank_changes）
- 銘柄別時系列データ取得（history）
- TechnicalAnalyzerとのシームレスな連携

## 依存ライブラリ

このプロジェクトでは、以下の主要なライブラリを使用しています。

- pandas: データ操作と分析
- numpy: 数値計算
- sqlite3: SQLiteデータベース操作
- aiohttp: 非同期HTTPリクエスト（J-Quants API用）
- requests: HTTPリクエスト
- pydantic-settings: 設定管理
- backtesting: バックテストフレームワーク（technical_tools/backtester.pyで使用）
- matplotlib: グラフ描画（チャート分類で使用）
- plotly: インタラクティブチャート（technical_toolsで使用）
- yfinance: 米国株データ取得（technical_toolsで使用）
- japanize_matplotlib: matplotlibの日本語表示対応（チャート分類で使用）
- scipy: 科学技術計算（チャート分類で使用）
- scikit-learn: 機械学習（チャート分類で使用）
- pyyaml: YAML設定ファイルの読み込み
- kaleido: Plotlyチャートの静的画像エクスポート（オプション、`chart-export`エクストラ）
- pytest: テストフレームワーク
- openpyxl: Excelファイルの読み書き

## 計算される財務指標

`data/statements.db` の `calculated_fundamentals` テーブルには、以下の財務指標が計算・格納されます：

| 指標 | 説明 | 計算式 |
|------|------|--------|
| PER | 株価収益率 | 株価 / EPS |
| PBR | 株価純資産倍率 | 株価 / BPS |
| ROE | 自己資本利益率 | 純利益 / 自己資本 × 100 |
| ROA | 総資産利益率 | 純利益 / 総資産 × 100 |
| 配当利回り | | 年間配当 / 株価 × 100 |
| 時価総額 | | 株価 × 発行済株式数 |
| 営業利益率 | | 営業利益 / 売上高 × 100 |
| 純利益率 | | 純利益 / 売上高 × 100 |
| FCF | フリーキャッシュフロー | 営業CF + 投資CF |
