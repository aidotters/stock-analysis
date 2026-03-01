# API リファレンス

## 設定モジュール (`backend/market_pipeline/config/`)

### get_settings()

設定インスタンスを取得します（シングルトン）。

```python
from market_pipeline.config import get_settings

settings = get_settings()
```

**戻り値**: `Settings` - 設定オブジェクト

### Settings クラス

#### settings.paths

| 属性 | 型 | 説明 |
|-----|-----|------|
| `base_dir` | `Path` | プロジェクトルートディレクトリ |
| `data_dir` | `Path` | データディレクトリ (`data/`) |
| `logs_dir` | `Path` | ログディレクトリ (`logs/`) |
| `output_dir` | `Path` | 出力ディレクトリ (`output/`) |
| `jquants_db` | `Path` | J-Quants株価DB (`data/jquants.db`) |
| `analysis_db` | `Path` | 分析結果DB (`data/analysis_results.db`) |
| `yfinance_db` | `Path` | yfinance DB (`data/yfinance.db`) - レガシー |
| `master_db` | `Path` | マスターDB (`data/master.db`) |
| `statements_db` | `Path` | 財務諸表DB (`data/statements.db`) |

#### settings.jquants

| 属性 | 型 | デフォルト | 説明 |
|-----|-----|---------|------|
| `max_concurrent_requests` | `int` | 3 | 同時リクエスト数上限 |
| `batch_size` | `int` | 100 | バッチサイズ |
| `request_delay` | `float` | 0.1 | リクエスト間隔（秒） |
| `timeout_seconds` | `int` | 30 | リクエストタイムアウト（秒） |
| `cache_ttl_hours` | `int` | 24 | キャッシュ有効期間（時間） |

#### settings.yfinance

| 属性 | 型 | デフォルト | 説明 |
|-----|-----|---------|------|
| `max_workers` | `int` | 1 | 最大ワーカー数（1 = シーケンシャル推奨） |
| `rate_limit_delay` | `float` | 2.0 | リクエスト間隔（秒） |

#### settings.analysis

| 属性 | 型 | デフォルト | 説明 |
|-----|-----|---------|------|
| `sma_short` | `int` | 50 | 短期移動平均期間 |
| `sma_medium` | `int` | 150 | 中期移動平均期間 |
| `sma_long` | `int` | 200 | 長期移動平均期間 |
| `hl_ratio_weeks` | `int` | 52 | HL比率計算期間（週） |
| `rsp_period_days` | `int` | 500 | RSP計算期間（日） |
| `update_window_days` | `int` | 5 | 更新ウィンドウ期間（日） |
| `trading_days_per_year` | `int` | 260 | 年間営業日数 |
| `type6_threshold` | `float` | 1.3 | Type6判定閾値（52週安値の130%以上） |
| `type7_threshold` | `float` | 0.75 | Type7判定閾値（52週高値の75%以上） |
| `type8_rsi_threshold` | `int` | 70 | Type8判定RSI閾値 |
| `rsp_weight_q1_q3` | `float` | 0.2 | RSP計算のQ1-Q3ウェイト |
| `rsp_weight_q4` | `float` | 0.4 | RSP計算のQ4ウェイト |
| `composite_weight_hl` | `float` | 0.4 | 複合スコアのHL比率ウェイト |
| `composite_weight_rsi` | `float` | 0.4 | 複合スコアのRSIウェイト |
| `composite_weight_minervini` | `float` | 0.2 | 複合スコアのMinerviniウェイト |
| `high_rsi_threshold` | `float` | 70.0 | 高RSI閾値 |
| `strong_composite_threshold` | `float` | 70.0 | 強い複合スコア閾値 |
| `chart_windows` | `list` | [20, 60, 120, 240] | 短期チャートウィンドウ |
| `chart_long_windows` | `list` | [960, 1200] | 長期チャートウィンドウ |

#### settings.database

| 属性 | 型 | デフォルト | 説明 |
|-----|-----|---------|------|
| `journal_mode` | `str` | "WAL" | SQLiteジャーナルモード |
| `synchronous` | `str` | "NORMAL" | 同期モード |
| `cache_size` | `int` | 10000 | キャッシュサイズ（ページ） |
| `mmap_size` | `int` | 268435456 | メモリマップサイズ（256MB） |

#### settings.logging

| 属性 | 型 | デフォルト | 説明 |
|-----|-----|---------|------|
| `level` | `str` | `"INFO"` | ログレベル |
| `format` | `str` | `"%(asctime)s - %(name)s - %(levelname)s - %(message)s"` | ログフォーマット |
| `date_format` | `str` | `"%Y-%m-%d %H:%M:%S"` | 日付フォーマット |

**環境変数プレフィックス:** `LOG_`（例: `LOG_LEVEL=DEBUG`）

#### settings.slack

| 属性 | 型 | デフォルト | 説明 |
|-----|-----|---------|------|
| `webhook_url` | `str` | `""` | Slack Webhook URL |
| `error_webhook_url` | `str` | `""` | エラー専用Webhook URL（オプション） |
| `enabled` | `bool` | `True` | 通知の有効/無効 |
| `timeout_seconds` | `int` | `10` | HTTPタイムアウト（秒） |
| `max_retries` | `int` | `3` | リトライ回数 |

---

## ニュース巡回設定モジュール (`backend/market_pipeline/news/`)

ニュース巡回先サイトのYAML設定を読み込み・バリデーションするモジュール。`/discover-stocks`スキルで使用。

### load_config()

```python
from market_pipeline.news import load_config

config = load_config("config/news_sources.yaml")
```

**パラメータ:**

| パラメータ | 型 | 説明 |
|-----------|-----|------|
| `path` | `str \| Path` | YAML設定ファイルのパス |

**戻り値**: `NewsConfig` - 設定オブジェクト

**例外**: `FileNotFoundError`（ファイル不在）、`ValueError`（バリデーションエラー）

### get_sources_by_category()

```python
from market_pipeline.news import get_sources_by_category

sources = get_sources_by_category(config, "news")
```

**パラメータ:**

| パラメータ | 型 | 説明 |
|-----------|-----|------|
| `config` | `NewsConfig` | 設定オブジェクト |
| `category` | `str` | カテゴリ名（`news`, `analysis`, `financial`） |

**戻り値**: `list[NewsSource]` - ソースのリスト（未知カテゴリの場合は空リスト）

### NewsSource（データクラス）

frozenデータクラス。各巡回先サイトの情報を保持。

| 属性 | 型 | 説明 |
|-----|-----|------|
| `name` | `str` | サイト名 |
| `auth` | `str` | 認証方式（`"none"` または `"cdp"`） |
| `url` | `str \| None` | サイトURL（`url`または`url_template`のいずれか必須） |
| `selector` | `str \| None` | CSSセレクタ |
| `description` | `str \| None` | サイト説明 |
| `url_template` | `str \| None` | URLテンプレート（`{code}`プレースホルダー） |

### NewsConfig（データクラス）

| 属性 | 型 | 説明 |
|-----|-----|------|
| `sources` | `dict[str, list[NewsSource]]` | カテゴリ別ソースリスト |

| プロパティ | 型 | 説明 |
|-----------|-----|------|
| `categories` | `list[str]` | カテゴリ名リスト |
| `all_sources` | `list[NewsSource]` | 全ソースのフラットリスト |

---

## Market Reader パッケージ (`backend/market_reader/`)

pandas_datareader風のインターフェースでJ-Quantsの株価データにアクセスするパッケージ。

### DataReader

```python
from market_reader import DataReader

reader = DataReader()
df = reader.get_prices("7203", start="2024-01-01", end="2024-12-31")
```

#### コンストラクタ

```python
DataReader(db_path: str | Path | None = None, strict: bool = False)
```

**パラメータ**:
- `db_path`: jquants.dbのパス（省略時はsettings.paths.jquants_dbを使用）
- `strict`: エラー時の動作。True=例外発生、False（デフォルト）=空DataFrameと警告

**例外**:
- `DatabaseConnectionError`: データベースファイルが存在しない、またはアクセスできない場合

#### メソッド

##### `get_prices(code, start=None, end=None, columns="simple") -> pd.DataFrame`

株価データを取得。

**パラメータ**:
- `code` (`str | list[str]`): 銘柄コード（4桁または5桁、単一または複数）
- `start` (`str | None`): 開始日 (YYYY-MM-DD)。省略時はendの5年前
- `end` (`str | None`): 終了日 (YYYY-MM-DD)。省略時はDB内最新日
- `columns` (`str | list[str]`): 取得カラム。"simple"、"full"、またはカラム名リスト

**columnsオプション詳細**:

| オプション | 説明 |
|-----------|------|
| `"simple"` | デフォルト。基本的なOHLCVデータ |
| `"full"` | 全カラム（Date, Code以外） |
| `list[str]` | 任意のカラム名リスト |

**SIMPLE_COLUMNS** (6カラム):

| カラム名 | 説明 |
|---------|------|
| `Open` | 始値 |
| `High` | 高値 |
| `Low` | 安値 |
| `Close` | 終値 |
| `Volume` | 出来高 |
| `AdjustmentClose` | 調整後終値 |

**FULL_COLUMNS** (16カラム):

| カラム名 | 説明 |
|---------|------|
| `Date` | 日付（インデックスとして使用） |
| `Code` | 銘柄コード（インデックスとして使用） |
| `Open` | 始値 |
| `High` | 高値 |
| `Low` | 安値 |
| `Close` | 終値 |
| `UpperLimit` | ストップ高 |
| `LowerLimit` | ストップ安 |
| `Volume` | 出来高 |
| `TurnoverValue` | 売買代金 |
| `AdjustmentFactor` | 調整係数 |
| `AdjustmentOpen` | 調整後始値 |
| `AdjustmentHigh` | 調整後高値 |
| `AdjustmentLow` | 調整後安値 |
| `AdjustmentClose` | 調整後終値 |
| `AdjustmentVolume` | 調整後出来高 |

**戻り値**: `pd.DataFrame` - 日付インデックスの株価DataFrame。複数銘柄の場合はMultiIndex（Date, Code）

**例外**（strict=True時のみ）:
- `StockNotFoundError`: 銘柄が見つからない場合
- `InvalidDateRangeError`: start > end の場合

**例外**（常時）:
- `ValueError`: 無効なカラム名、無効な日付フォーマット

### 例外クラス

| クラス | 説明 |
|--------|------|
| `StockReaderError` | 基底例外クラス |
| `StockNotFoundError` | 銘柄が見つからない |
| `DatabaseConnectionError` | DB接続エラー |
| `InvalidDateRangeError` | 日付範囲エラー（start > end） |

### ユーティリティ関数 (`backend/market_reader/utils.py`)

##### `normalize_code(code: str) -> str`

銘柄コードを4桁形式に正規化。5桁コードで末尾が0の場合、4桁に変換。

```python
>>> normalize_code("72030")
'7203'
>>> normalize_code("7203")
'7203'
>>> normalize_code("72031")  # 末尾が0以外は変換しない
'72031'
```

##### `to_5digit_code(code: str) -> str`

4桁コードを5桁形式に変換（DBクエリ用）。

```python
>>> to_5digit_code("7203")
'72030'
```

##### `validate_date(date_str: str | None) -> datetime | None`

日付文字列をdatetimeに変換。形式はYYYY-MM-DD。

**例外**: `ValueError` - 無効な日付フォーマット

##### `get_default_end_date(conn: sqlite3.Connection) -> datetime`

データベース内の最新日付を取得。

##### `get_default_start_date(end_date: datetime) -> datetime`

デフォルト開始日（end_dateの5年前）を計算。

---

## Technical Tools パッケージ (`backend/technical_tools/`)

Jupyter Notebook用のテクニカル分析ツール。日本株(J-Quants)と米国株(yfinance)の統一インターフェースを提供。

### TechnicalAnalyzer

```python
from technical_tools import TechnicalAnalyzer

analyzer = TechnicalAnalyzer(source="jquants")
fig = analyzer.plot_chart("7203", show_sma=[25, 75], show_rsi=True)
fig.show()
```

#### コンストラクタ

```python
TechnicalAnalyzer(source: Literal["jquants", "yfinance"] = "jquants")
```

**パラメータ**:
- `source`: データソース（"jquants" または "yfinance"）

#### メソッド

##### `get_prices(ticker, start=None, end=None, **kwargs) -> pd.DataFrame`

株価データを取得（キャッシュ付き）。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `start` (`str | None`): 開始日 (YYYY-MM-DD)
- `end` (`str | None`): 終了日 (YYYY-MM-DD)
- `**kwargs`: データソース固有の追加引数
  - `period` (`str`): 期間指定（start/endの代替）。例: `"1y"`, `"6mo"`, `"3mo"`, `"1mo"`

**戻り値**: `pd.DataFrame` - OHLCV列を持つDataFrame

##### `calculate_indicators(ticker, indicators, start=None, end=None, **kwargs) -> pd.DataFrame`

複数のテクニカル指標を計算。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `indicators` (`list[str]`): 指標リスト（"sma", "ema", "rsi", "macd", "bb"）
- `start` (`str | None`): 開始日
- `end` (`str | None`): 終了日

**戻り値**: `pd.DataFrame` - 指標列が追加されたDataFrame

##### `add_sma(ticker, periods, start=None, end=None, **kwargs) -> pd.DataFrame`

単純移動平均を追加。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `periods` (`list[int]`): SMA期間リスト

**戻り値**: `pd.DataFrame` - `SMA_N`列が追加されたDataFrame

##### `add_ema(ticker, periods, start=None, end=None, **kwargs) -> pd.DataFrame`

指数移動平均を追加。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `periods` (`list[int]`): EMA期間リスト

**戻り値**: `pd.DataFrame` - `EMA_N`列が追加されたDataFrame

##### `add_rsi(ticker, period=14, start=None, end=None, **kwargs) -> pd.DataFrame`

RSIを追加。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `period` (`int`): RSI期間（デフォルト: 14）

**戻り値**: `pd.DataFrame` - `RSI_N`列が追加されたDataFrame

##### `add_macd(ticker, fast=12, slow=26, signal=9, start=None, end=None, **kwargs) -> pd.DataFrame`

MACDを追加。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `fast` (`int`): 短期EMA期間（デフォルト: 12）
- `slow` (`int`): 長期EMA期間（デフォルト: 26）
- `signal` (`int`): シグナル線期間（デフォルト: 9）

**戻り値**: `pd.DataFrame` - `MACD`, `MACD_Signal`, `MACD_Hist`列が追加されたDataFrame

##### `add_bollinger_bands(ticker, period=20, std=2.0, start=None, end=None, **kwargs) -> pd.DataFrame`

ボリンジャーバンドを追加。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `period` (`int`): 移動平均期間（デフォルト: 20）
- `std` (`float`): 標準偏差倍率（デフォルト: 2.0）

**戻り値**: `pd.DataFrame` - `BB_Upper`, `BB_Middle`, `BB_Lower`列が追加されたDataFrame

##### `detect_crosses(ticker, short=5, long=25, patterns=None, start=None, end=None, **kwargs) -> list[Signal]`

ゴールデンクロス・デッドクロスを検出。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `short` (`int`): 短期MA期間（patternsがNoneの場合使用）
- `long` (`int`): 長期MA期間（patternsがNoneの場合使用）
- `patterns` (`list[tuple[int, int]] | None`): 検出パターンリスト

**戻り値**: `list[Signal]` - 日付順にソートされたSignalオブジェクトのリスト

##### `plot_chart(ticker, show_sma=None, show_bb=False, show_rsi=False, show_macd=False, show_signals=False, signal_patterns=None, start=None, end=None, **kwargs) -> go.Figure`

インタラクティブチャートを生成。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `show_sma` (`list[int] | None`): 表示するSMA期間リスト
- `show_bb` (`bool`): ボリンジャーバンドを表示
- `show_rsi` (`bool`): RSIサブプロットを表示
- `show_macd` (`bool`): MACDサブプロットを表示
- `show_signals` (`bool`): クロスシグナルを表示
- `signal_patterns` (`list[tuple[int, int]] | None`): シグナル検出パターン

**戻り値**: `go.Figure` - Plotly Figureオブジェクト

##### `load_existing_analysis(ticker, db_path=None) -> dict[str, Any]`

既存の分析結果をデータベースから読み込み。

**パラメータ**:
- `ticker` (`str`): 銘柄コード
- `db_path` (`Path | str | None`): analysis_results.dbのパス（省略時は設定から取得）

**戻り値**: `dict` - `minervini`と`relative_strength`キーを持つ辞書

### Signal

シグナルデータクラス。

```python
from technical_tools import Signal
```

**属性**:
- `date` (`datetime`): シグナル発生日
- `signal_type` (`Literal["golden_cross", "dead_cross"]`): シグナル種別
- `price` (`float`): シグナル発生時の終値
- `short_period` (`int`): 短期MA期間
- `long_period` (`int`): 長期MA期間

### 例外クラス

| クラス | 説明 |
|--------|------|
| `TechnicalToolsError` | 基底例外クラス |
| `DataSourceError` | データ取得エラー |
| `TickerNotFoundError` | 銘柄が見つからない |
| `InsufficientDataError` | データ不足 |
| `BacktestError` | バックテスト関連の基底エラー |
| `BacktestInsufficientDataError` | バックテストデータ不足 |
| `InvalidSignalError` | 無効なシグナル指定 |
| `InvalidRuleError` | 無効なルール指定 |
| `PortfolioError` | ポートフォリオ関連エラー |
| `OptimizerError` | 最適化関連の基底エラー |
| `InvalidSearchSpaceError` | 探索空間の定義が不正 |
| `NoValidParametersError` | 有効なパラメータ組み合わせがない |
| `OptimizationTimeoutError` | 最適化タイムアウト |

### StockScreener

```python
from technical_tools import StockScreener, ScreenerFilter

screener = StockScreener()
results = screener.filter(composite_score_min=70.0, hl_ratio_min=80.0)
```

Jupyter Notebook用の銘柄スクリーニングツール。統合分析結果をDBから取得し、柔軟にフィルタリング。

#### コンストラクタ

```python
StockScreener(
    analysis_db_path: Path | None = None,
    statements_db_path: Path | None = None
)
```

**パラメータ**:
- `analysis_db_path`: analysis_results.dbのパス（省略時はsettings.paths.analysis_dbを使用）
- `statements_db_path`: statements.dbのパス（省略時はsettings.paths.statements_dbを使用）

#### メソッド

##### `filter(filter_config=None, *, date=None, composite_score_min=None, ...) -> pd.DataFrame`

複数条件で銘柄をフィルタリング。

**パラメータ**:
- `filter_config` (`ScreenerFilter | None`): ScreenerFilterオブジェクト（指定時は他のキーワード引数を無視）
- `date` (`str | None`): 分析日（省略時は最新日）
- `composite_score_min` (`float | None`): 最小composite_score
- `composite_score_max` (`float | None`): 最大composite_score
- `hl_ratio_min` (`float | None`): 最小HL比率
- `hl_ratio_max` (`float | None`): 最大HL比率
- `rsi_min` (`float | None`): 最小RSI
- `rsi_max` (`float | None`): 最大RSI
- `market_cap_min` (`float | None`): 最小時価総額
- `market_cap_max` (`float | None`): 最大時価総額
- `per_min` (`float | None`): 最小PER
- `per_max` (`float | None`): 最大PER
- `pbr_max` (`float | None`): 最大PBR
- `roe_min` (`float | None`): 最小ROE
- `dividend_yield_min` (`float | None`): 最小配当利回り
- `pattern_window` (`int | None`): チャートパターンウィンドウ（20, 60, 120, 240, 960, 1200）
- `pattern_labels` (`list[str] | None`): パターンラベルリスト（例: ["上昇", "急上昇"]）
- `sector` (`str | None`): セクターフィルター
- `limit` (`int`): 最大結果数（デフォルト: 100）

**戻り値**: `pd.DataFrame` - フィルタリング結果

##### `rank_changes(metric="composite_score", days=7, direction="up", min_change=1, limit=50) -> pd.DataFrame`

順位変動が大きい銘柄を取得。

**パラメータ**:
- `metric` (`str`): 順位指標（"composite_score", "hl_ratio", "rsp"）
- `days` (`int`): 比較日数
- `direction` (`str`): "up"（上昇）, "down"（下降）, "both"（両方）
- `min_change` (`int`): 最小順位変動幅
- `limit` (`int`): 最大結果数

**戻り値**: `pd.DataFrame` - Code, current_rank, past_rank, rank_change列を持つDataFrame

**例外**:
- `ValueError`: metricが無効な値の場合

##### `history(code, days=30) -> pd.DataFrame`

特定銘柄の時系列データを取得。

**パラメータ**:
- `code` (`str`): 銘柄コード
- `days` (`int`): 取得日数

**戻り値**: `pd.DataFrame` - Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank列を持つDataFrame

### ScreenerFilter

フィルター設定をグループ化するデータクラス。

```python
from technical_tools import ScreenerFilter

config = ScreenerFilter(
    composite_score_min=70.0,
    hl_ratio_min=80.0,
    market_cap_min=100_000_000_000,
    per_max=15.0,
)
results = screener.filter(config)
```

**属性**:

| カテゴリ | 属性 | 型 | デフォルト |
|---------|------|-----|---------|
| 日付 | `date` | `str \| None` | None |
| テクニカル | `composite_score_min` | `float \| None` | None |
| テクニカル | `composite_score_max` | `float \| None` | None |
| テクニカル | `hl_ratio_min` | `float \| None` | None |
| テクニカル | `hl_ratio_max` | `float \| None` | None |
| テクニカル | `rsi_min` | `float \| None` | None |
| テクニカル | `rsi_max` | `float \| None` | None |
| 財務 | `market_cap_min` | `float \| None` | None |
| 財務 | `market_cap_max` | `float \| None` | None |
| 財務 | `per_min` | `float \| None` | None |
| 財務 | `per_max` | `float \| None` | None |
| 財務 | `pbr_max` | `float \| None` | None |
| 財務 | `roe_min` | `float \| None` | None |
| 財務 | `dividend_yield_min` | `float \| None` | None |
| パターン | `pattern_window` | `int \| None` | None |
| パターン | `pattern_labels` | `list[str] \| None` | None |
| その他 | `sector` | `str \| None` | None |
| その他 | `limit` | `int` | 100 |

##### `to_dict() -> dict`

フィルター設定を辞書に変換（None値は除外）。

### 指標計算関数 (`backend/technical_tools/indicators.py`)

低レベル関数（DataFrame直接操作）:

##### `add_sma(df, periods) -> pd.DataFrame`

単純移動平均を追加。

##### `add_ema(df, periods) -> pd.DataFrame`

指数移動平均を追加。

##### `add_rsi(df, period=14) -> pd.DataFrame`

RSIを追加。

##### `add_macd(df, fast=12, slow=26, signal=9) -> pd.DataFrame`

MACDを追加。

##### `add_bollinger_bands(df, period=20, std=2.0) -> pd.DataFrame`

ボリンジャーバンドを追加。

##### `calculate_indicators(df, indicators, ...) -> pd.DataFrame`

複数指標を一括計算。

**パラメータ**:
- `df` (`pd.DataFrame`): 'Close'カラムを持つDataFrame
- `indicators` (`list[str]`): 計算する指標リスト（"sma", "ema", "rsi", "macd", "bb"）
- `sma_periods` (`list[int] | None`): SMA期間（デフォルト: [5, 25, 75]）
- `ema_periods` (`list[int] | None`): EMA期間（デフォルト: [12, 26]）
- `rsi_period` (`int`): RSI期間（デフォルト: 14）
- `macd_fast` (`int`): MACD短期EMA（デフォルト: 12）
- `macd_slow` (`int`): MACD長期EMA（デフォルト: 26）
- `macd_signal` (`int`): MACDシグナル線（デフォルト: 9）
- `bb_period` (`int`): ボリンジャーバンド期間（デフォルト: 20）
- `bb_std` (`float`): ボリンジャーバンド標準偏差倍率（デフォルト: 2.0）

**戻り値**: `pd.DataFrame` - 指定した指標カラムが追加されたDataFrame

### シグナル検出関数 (`backend/technical_tools/signals.py`)

##### `detect_crosses(df, short=5, long=25) -> list[Signal]`

単一パターンのクロス検出。

##### `detect_crosses_multiple(df, patterns=None) -> list[Signal]`

複数パターンのクロス検出。

### データソース (`backend/technical_tools/data_sources/`)

#### DataSource (抽象基底クラス)

```python
from technical_tools.data_sources.base import DataSource
```

すべてのデータソースが実装すべきインターフェース。

##### `get_prices(ticker, start=None, end=None, **kwargs) -> pd.DataFrame`

株価データを取得。戻り値はOpen, High, Low, Close, Volume列を持つDataFrame。

#### JQuantsSource

```python
from technical_tools.data_sources.jquants import JQuantsSource
```

market_readerパッケージ経由でJ-Quantsデータにアクセス。株式分割を考慮した調整後価格（AdjustmentOpen/High/Low/Close/Volume）を使用してOHLCVデータを返す。

**サポートするperiod値**: 1mo, 3mo, 6mo, 1y, 2y, 5y

#### YFinanceSource

```python
from technical_tools.data_sources.yfinance import YFinanceSource
```

yfinanceライブラリ経由で米国株・国際株にアクセス。

**サポートするティッカー形式**:
- 米国株: AAPL, MSFT等
- 日本株: 7203.T, 9984.T等

### Backtester

```python
from technical_tools import Backtester

bt = Backtester(cash=1_000_000)
bt.add_signal("golden_cross", short=5, long=25)
bt.add_exit_rule("stop_loss", threshold=-0.10)
results = bt.run(symbols=["7203"], start="2023-01-01", end="2023-12-31")
```

シグナルベースのバックテストを実行するクラス。backtesting.pyライブラリをラップ。

#### コンストラクタ

```python
Backtester(cash: float = 1_000_000, commission: float = 0.0)
```

**パラメータ**:
- `cash`: 初期資金（デフォルト: 1,000,000）
- `commission`: 取引手数料率（デフォルト: 0）

#### メソッド

##### `add_signal(signal_name, **params) -> Backtester`

トレーディングシグナルを追加。

**パラメータ**:
- `signal_name` (`str`): シグナル名

**対応シグナル**:

| シグナル名 | 説明 | パラメータ |
|-----------|------|-----------|
| `golden_cross` | ゴールデンクロス | `short=5, long=25` |
| `dead_cross` | デッドクロス | `short=5, long=25` |
| `rsi_oversold` | RSI売られすぎ | `threshold=30, period=14` |
| `rsi_overbought` | RSI買われすぎ | `threshold=70, period=14` |
| `macd_cross` | MACDクロス | `fast=12, slow=26, signal=9` |
| `bollinger_breakout` | ボリンジャーブレイクアウト | `period=20, std=2.0, direction="up"` |
| `bollinger_squeeze` | ボリンジャースクイーズ | `period=20, std=2.0, squeeze_threshold=0.1` |
| `volume_spike` | 出来高急増 | `period=20, multiplier=2.0` |
| `volume_breakout` | 出来高確認付きブレイクアウト | `price_period=20, volume_period=20, volume_multiplier=1.5` |

**戻り値**: `Backtester` - メソッドチェーン用

**例外**: `InvalidSignalError` - シグナル名が無効

##### `add_entry_rule(rule_name, **params) -> Backtester`

エントリールールを追加。

**パラメータ**:
- `rule_name` (`str`): ルール名

**対応ルール**:

| ルール名 | 説明 |
|---------|------|
| `next_day_open` | 翌営業日の寄付で買い |

##### `add_exit_rule(rule_name, **params) -> Backtester`

エグジットルールを追加。

**パラメータ**:
- `rule_name` (`str`): ルール名

**対応ルール**:

| ルール名 | 説明 | パラメータ |
|---------|------|-----------|
| `stop_loss` | 損切り | `threshold=-0.10`（負の値） |
| `take_profit` | 利確 | `threshold=0.20`（正の値） |
| `max_holding_days` | 最大保有日数 | `days=30` |
| `trailing_stop` | トレーリングストップ | `threshold=-0.05`（負の値） |

**戻り値**: `Backtester` - メソッドチェーン用

**例外**: `InvalidRuleError` - ルール名またはパラメータが無効

##### `run(symbols, start, end, max_workers=None) -> BacktestResults`

バックテストを実行。

**パラメータ**:
- `symbols` (`list[str]`): 銘柄コードリスト
- `start` (`str`): 開始日（YYYY-MM-DD）
- `end` (`str`): 終了日（YYYY-MM-DD）
- `max_workers` (`int | None`): 並列ワーカー数（デフォルト: min(len(symbols), 4)）

**戻り値**: `BacktestResults` - バックテスト結果

**例外**:
- `BacktestError` - シグナル未設定
- `BacktestInsufficientDataError` - データ不足

##### `run_with_screener(screener_filter, start, end, ...) -> BacktestResults`

スクリーナー結果を使用したバックテスト。

**パラメータ**:
- `screener_filter` (`ScreenerFilter | dict`): スクリーナーフィルター
- `start` (`str`): 開始日
- `end` (`str`): 終了日
- `entry_rule` (`str`): エントリールール（デフォルト: "next_day_open"）
- `exit_rules` (`dict[str, float] | None`): エグジットルール辞書
- `screener` (`StockScreener | None`): スクリーナーインスタンス
- `max_workers` (`int | None`): 並列ワーカー数

**戻り値**: `BacktestResults`

### BacktestResults

```python
results = bt.run(symbols=["7203"], start="2023-01-01", end="2023-12-31")
print(results.summary())
results.plot().show()
```

バックテスト結果を保持し、分析・可視化を提供するクラス。

#### メソッド

##### `summary() -> dict`

パフォーマンス指標のサマリーを取得。

**戻り値**: 以下のキーを持つ辞書

| キー | 説明 |
|-----|------|
| `total_trades` | 総取引回数 |
| `win_rate` | 勝率（0-1） |
| `avg_return` | 平均リターン |
| `max_return` | 最大リターン |
| `max_loss` | 最大損失 |
| `profit_factor` | プロフィットファクター |
| `max_drawdown` | 最大ドローダウン |
| `sharpe_ratio` | 年率シャープレシオ |
| `avg_holding_days` | 平均保有日数 |

##### `trades() -> pd.DataFrame`

取引一覧をDataFrameで取得。

**戻り値**: `pd.DataFrame` - symbol, entry_date, entry_price, exit_date, exit_price, shares, pnl, return_pct, holding_days, exit_reason

##### `plot() -> go.Figure`

資産推移とドローダウンのインタラクティブチャートを生成。

**戻り値**: `go.Figure` - Plotly Figure

##### `export(path, format=None) -> Path`

結果をファイルにエクスポート。

**パラメータ**:
- `path` (`str | Path`): 出力パス
- `format` (`str | None`): "csv", "excel", "html"（省略時は拡張子から推定）

**戻り値**: `Path` - エクスポートされたファイルパス

**Excel出力シート**:
- Summary: サマリー指標
- Trades: 取引一覧
- By Symbol: 銘柄別パフォーマンス
- Monthly Returns: 月次リターン

##### `by_symbol() -> pd.DataFrame`

銘柄別パフォーマンス。

**戻り値**: `pd.DataFrame` - symbol, trades, win_rate, avg_return, total_pnl

##### `by_sector(sector_map) -> pd.DataFrame`

セクター別パフォーマンス。

**パラメータ**:
- `sector_map` (`dict[str, str] | None`): 銘柄→セクターのマッピング

**戻り値**: `pd.DataFrame` - sector, trades, win_rate, avg_return, total_pnl

##### `monthly_returns() -> pd.DataFrame`

月次リターン分析。

**戻り値**: `pd.DataFrame` - year_month, trades, avg_return, total_pnl

##### `yearly_returns() -> pd.DataFrame`

年次リターン分析。

**戻り値**: `pd.DataFrame` - year, trades, win_rate, avg_return, total_pnl

### Trade

取引データクラス。

```python
from technical_tools import Trade
```

**属性**:
- `symbol` (`str`): 銘柄コード
- `entry_date` (`datetime`): エントリー日
- `entry_price` (`float`): エントリー価格
- `exit_date` (`datetime`): エグジット日
- `exit_price` (`float`): エグジット価格
- `shares` (`int`): 株数
- `pnl` (`float`): 損益
- `return_pct` (`float`): リターン率
- `holding_days` (`int`): 保有日数
- `exit_reason` (`str`): エグジット理由

### VirtualPortfolio

```python
from technical_tools import VirtualPortfolio

vp = VirtualPortfolio("my_strategy_2025")
vp.buy("7203", shares=100, price=2500)
print(vp.summary())
vp.plot().show()
```

仮想ポートフォリオを管理するクラス。データは`data/portfolios/`にJSON永続化。

#### コンストラクタ

```python
VirtualPortfolio(name: str, portfolio_dir: Path | None = None)
```

**パラメータ**:
- `name`: ポートフォリオ名（ファイル名に使用）
- `portfolio_dir`: 保存ディレクトリ（デフォルト: data/portfolios/）

#### メソッド

##### `buy(symbol, shares=None, amount=None, price=None) -> VirtualPortfolio`

銘柄を購入。

**パラメータ**:
- `symbol` (`str`): 銘柄コード
- `shares` (`int | None`): 購入株数
- `amount` (`float | None`): 投資金額（sharesがNoneの場合に使用）
- `price` (`float | None`): 購入価格（省略時は現在価格）

**戻り値**: `VirtualPortfolio` - メソッドチェーン用

**例外**: `PortfolioError` - shares/amountの両方がNone、または株数が0以下

##### `sell(symbol, shares, price=None) -> VirtualPortfolio`

銘柄を売却。

**パラメータ**:
- `symbol` (`str`): 銘柄コード
- `shares` (`int`): 売却株数
- `price` (`float | None`): 売却価格（省略時は現在価格）

**例外**: `PortfolioError` - 銘柄未保有または株数不足

##### `sell_all(symbol, price=None) -> VirtualPortfolio`

銘柄を全株売却。

##### `summary() -> dict`

ポートフォリオサマリーを取得。

**戻り値**: 以下のキーを持つ辞書

| キー | 説明 |
|-----|------|
| `total_investment` | 総投資額 |
| `current_value` | 現在評価額 |
| `total_pnl` | 総損益 |
| `return_pct` | リターン率 |

##### `holdings() -> pd.DataFrame`

保有銘柄一覧を取得。

**戻り値**: `pd.DataFrame` - symbol, shares, avg_price, current_price, pnl

##### `performance(days=30) -> pd.DataFrame`

パフォーマンス推移を取得。

**パラメータ**:
- `days` (`int`): 取得日数

**戻り値**: `pd.DataFrame` - date, portfolio_value

##### `plot() -> go.Figure`

ポートフォリオのインタラクティブチャートを生成。

**戻り値**: `go.Figure` - 保有比率（円グラフ）、損益（棒グラフ）、パフォーマンス推移

##### `buy_from_screener(screener_filter=None, amount_per_stock=100000, max_stocks=10, ...) -> VirtualPortfolio`

スクリーナー結果から一括購入。

**パラメータ**:
- `screener_filter` (`ScreenerFilter | dict | None`): フィルター設定
- `amount_per_stock` (`float`): 各銘柄への投資額
- `max_stocks` (`int`): 最大購入銘柄数
- `screener` (`StockScreener | None`): スクリーナーインスタンス
- `**filter_kwargs`: フィルターパラメータ（screener_filterがNoneの場合）

### StrategyOptimizer

```python
from technical_tools import StrategyOptimizer

optimizer = StrategyOptimizer(cash=1_000_000)
optimizer.add_search_space("ma_short", [5, 10, 20])
optimizer.add_search_space("ma_long", [50, 75, 100])
optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])
results = optimizer.run(
    symbols=["7203"],
    start="2023-01-01",
    end="2023-12-31",
    method="grid",
    metric="sharpe_ratio"
)
```

投資戦略のパラメータを自動最適化するクラス。Backtesterを内部で利用し、複数のパラメータ組み合わせを効率的に評価。

#### コンストラクタ

```python
StrategyOptimizer(cash: float = 1_000_000, commission: float = 0.0)
```

**パラメータ**:
- `cash`: 初期資金（デフォルト: 1,000,000）
- `commission`: 取引手数料率（デフォルト: 0）

#### メソッド

##### `add_search_space(name, values) -> StrategyOptimizer`

探索空間にパラメータを追加。

**パラメータ**:
- `name` (`str`): パラメータ名
- `values` (`list[Any]`): 探索する値のリスト

**対応パラメータ**:

| パラメータ名 | 説明 | 例 |
|-------------|------|-----|
| `ma_short` | 短期移動平均期間 | [5, 10, 20, 25] |
| `ma_long` | 長期移動平均期間 | [50, 75, 100, 200] |
| `rsi_threshold` | RSI閾値 | [20, 25, 30, 35] |
| `macd_fast` | MACD短期EMA | [8, 10, 12] |
| `macd_slow` | MACD長期EMA | [20, 24, 26] |
| `macd_signal` | MACDシグナル線 | [7, 9, 11] |
| `stop_loss` | 損切り閾値 | [-0.05, -0.10, -0.15] |
| `take_profit` | 利確閾値 | [0.10, 0.15, 0.20] |

**戻り値**: `StrategyOptimizer` - メソッドチェーン用

**例外**: `ValueError` - パラメータ名が空、または値リストが空の場合

##### `add_constraint(func) -> StrategyOptimizer`

パラメータ制約条件を追加。

**パラメータ**:
- `func` (`Callable[[dict[str, Any]], bool]`): パラメータ辞書を受け取りTrueなら有効とする関数

**戻り値**: `StrategyOptimizer` - メソッドチェーン用

##### `run(symbols, start, end, method="grid", n_trials=100, metric="sharpe_ratio", n_jobs=-1, validation=None, train_ratio=0.7, n_splits=5, timeout=None, streaming_output=None) -> OptimizationResults`

最適化を実行。

**パラメータ**:
- `symbols` (`list[str]`): 対象銘柄リスト
- `start` (`str`): 開始日（YYYY-MM-DD）
- `end` (`str`): 終了日（YYYY-MM-DD）
- `method` (`Literal["grid", "random"]`): 探索手法（デフォルト: "grid"）
- `n_trials` (`int`): ランダムサーチ時の試行回数（デフォルト: 100）
- `metric` (`str | dict[str, float]`): 最適化対象指標（デフォルト: "sharpe_ratio"）
- `n_jobs` (`int`): 並列数（-1で全コア使用、デフォルト: -1）
- `validation` (`Literal["none", "walk_forward"] | None`): 検証手法
- `train_ratio` (`float`): 訓練期間の割合（デフォルト: 0.7）
- `n_splits` (`int`): 分割数（デフォルト: 5）
- `timeout` (`float | None`): タイムアウト秒数
- `streaming_output` (`str | Path | None`): JSONL形式のストリーミング出力パス

**探索手法**:

| 手法 | 説明 |
|------|------|
| `grid` | 全パラメータ組み合わせを探索 |
| `random` | n_trials回のランダムサンプリング |

**評価指標**:

| 指標 | 説明 | 最適化方向 |
|------|------|----------|
| `total_return` | トータルリターン | 最大化 |
| `sharpe_ratio` | シャープレシオ | 最大化 |
| `max_drawdown` | 最大ドローダウン | 最小化 |
| `win_rate` | 勝率 | 最大化 |
| `profit_factor` | プロフィットファクター | 最大化 |

**複合評価（重み付け）**:

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

**戻り値**: `OptimizationResults`

**例外**:
- `InvalidSearchSpaceError`: 探索空間が未定義
- `NoValidParametersError`: 制約条件を満たすパラメータがない
- `OptimizationTimeoutError`: タイムアウト発生

### OptimizationResults

```python
results = optimizer.run(...)
best = results.best()
top_10 = results.top(10)
results.plot_heatmap("ma_short", "ma_long").show()
results.save("results.json")
```

最適化結果を保持し、分析・可視化を提供するクラス。

#### メソッド

##### `best() -> TrialResult | None`

最良の試行結果を取得。

**戻り値**: `TrialResult` - 最良の結果、またはNone

##### `top(n=10) -> pd.DataFrame`

上位N件をDataFrameで取得。

**パラメータ**:
- `n` (`int`): 取得件数（デフォルト: 10）

**戻り値**: `pd.DataFrame` - パラメータと評価指標を含むDataFrame

##### `plot_heatmap(x_param, y_param, metric="sharpe_ratio") -> go.Figure`

パラメータ空間のヒートマップを生成。

**パラメータ**:
- `x_param` (`str`): X軸パラメータ
- `y_param` (`str`): Y軸パラメータ
- `metric` (`str`): 可視化する指標（デフォルト: "sharpe_ratio"）

**戻り値**: `go.Figure` - Plotly Figure

**例外**: `ValueError` - パラメータが探索空間に存在しない

##### `save(path) -> Path`

結果をファイルに保存。

**パラメータ**:
- `path` (`str | Path`): 出力パス（.json または .csv）

**戻り値**: `Path` - 保存されたファイルパス

##### `load(path) -> OptimizationResults` (classmethod)

JSONファイルから結果を読み込み。

**パラメータ**:
- `path` (`str | Path`): 入力ファイルパス

**戻り値**: `OptimizationResults`

##### `load_streaming(path, metric="sharpe_ratio") -> OptimizationResults` (classmethod)

JSONLファイル（ストリーミング出力）から結果を読み込み。

**パラメータ**:
- `path` (`str | Path`): 入力JONLファイルパス
- `metric` (`str | dict[str, float]`): ソート用評価指標

**戻り値**: `OptimizationResults`

### TrialResult

1回の最適化試行結果を保持するデータクラス。

```python
from technical_tools import TrialResult
```

**属性**:
- `params` (`dict[str, Any]`): 使用したパラメータ
- `metrics` (`dict[str, float]`): 評価指標
- `oos_metrics` (`dict[str, float] | None`): アウトオブサンプル評価（ウォークフォワード時）
- `backtest_results` (`BacktestResults | None`): バックテスト結果（メモリ節約のためNoneの場合あり）

### 最適化関連例外

| クラス | 説明 |
|--------|------|
| `OptimizerError` | 最適化関連エラーの基底クラス |
| `InvalidSearchSpaceError` | 探索空間の定義が不正 |
| `NoValidParametersError` | 有効なパラメータ組み合わせがない |
| `OptimizationTimeoutError` | タイムアウト発生 |

`OptimizationTimeoutError`の属性:
- `timeout` (`float`): タイムアウト秒数
- `completed` (`int`): 完了した試行数
- `total` (`int`): 総試行数

### バックテストシグナル (`backend/technical_tools/backtest_signals/`)

#### SignalRegistry

シグナルクラスの登録・取得を管理。

```python
from technical_tools.backtest_signals import SignalRegistry

# シグナルクラスを取得
signal_cls = SignalRegistry.get("golden_cross")

# 利用可能なシグナル一覧
signal_names = SignalRegistry.list_signals()
```

#### BaseSignal

すべてのシグナルの基底クラス（抽象）。

```python
from technical_tools.backtest_signals import BaseSignal

class MySignal(BaseSignal):
    name = "my_signal"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        # シグナル検出ロジック
        return pd.Series(False, index=df.index)
```

---

## J-Quants モジュール (`backend/market_pipeline/jquants/`)

### JQuantsDataProcessor

日次株価データの取得と保存を担当。

```python
from market_pipeline.jquants.data_processor import JQuantsDataProcessor

processor = JQuantsDataProcessor()
await processor.fetch_and_store_daily_quotes(stock_codes, start_date, end_date)
```

#### メソッド

##### `async fetch_and_store_daily_quotes(stock_codes, start_date, end_date)`

複数銘柄の日次株価データを非同期で取得し、データベースに保存。

**パラメータ**:
- `stock_codes` (`list[str]`): 銘柄コードリスト
- `start_date` (`str`): 開始日 (YYYY-MM-DD)
- `end_date` (`str`): 終了日 (YYYY-MM-DD)

##### `get_refresh_token() -> str`

J-Quants APIのリフレッシュトークンを取得。

### JQuantsStatementsProcessor

財務諸表データの取得を担当。

```python
from market_pipeline.jquants.statements_processor import JQuantsStatementsProcessor

processor = JQuantsStatementsProcessor(
    max_concurrent_requests=3,
    batch_size=100,
    request_delay=0.1
)
processor.get_all_statements(db_path)
```

#### コンストラクタ

```python
JQuantsStatementsProcessor(
    max_concurrent_requests: int = 3,
    batch_size: int = 100,
    request_delay: float = 0.1
)
```

**パラメータ**:
- `max_concurrent_requests`: 同時リクエスト数上限
- `batch_size`: バッチサイズ
- `request_delay`: リクエスト間隔（秒）

#### メソッド

##### `get_all_statements(db_path: str)`

全上場銘柄の財務諸表データを取得しデータベースに保存。

**パラメータ**:
- `db_path` (`str`): statements.dbのパス

##### `async get_statements_async(session, code) -> Tuple[str, List[Dict]]`

単一銘柄の財務諸表を非同期で取得。

**パラメータ**:
- `session`: aiohttpセッション
- `code` (`str`): 銘柄コード（5桁）

**戻り値**: `Tuple[str, List[Dict]]` - (銘柄コード, 財務諸表リスト)

##### `get_database_stats(db_path: str) -> Dict[str, Any]`

データベース統計を取得。

**戻り値**: `Dict` - statement_record_count, statement_code_count, statement_date_range, fundamentals_count

### FundamentalsCalculator

財務指標の計算を担当。

```python
from market_pipeline.jquants.fundamentals_calculator import FundamentalsCalculator

calculator = FundamentalsCalculator(
    statements_db_path="data/statements.db",
    jquants_db_path="data/jquants.db"
)
processed = calculator.update_all_fundamentals(output_db_path)
```

#### コンストラクタ

```python
FundamentalsCalculator(
    statements_db_path: str,
    jquants_db_path: str,
    master_db_path: str = None
)
```

**パラメータ**:
- `statements_db_path`: statements.dbのパス（財務諸表テーブル）
- `jquants_db_path`: jquants.dbのパス（株価テーブル）
- `master_db_path`: master.dbのパス（省略時はjquants_db_pathから推測）

#### メソッド

##### `update_all_fundamentals(output_db_path: str) -> int`

全銘柄のファンダメンタルズを計算しデータベースに保存。

**パラメータ**:
- `output_db_path` (`str`): 出力データベースパス

**戻り値**: `int` - 処理した銘柄数

##### `calculate_per(price, eps) -> Optional[float]`

PER（株価収益率）を計算。

##### `calculate_pbr(price, bps) -> Optional[float]`

PBR（株価純資産倍率）を計算。

##### `calculate_roe(profit, equity) -> Optional[float]`

ROE（自己資本利益率）を計算。

##### `calculate_roa(profit, total_assets) -> Optional[float]`

ROA（総資産利益率）を計算。

##### `calculate_dividend_yield(dps, price) -> Optional[float]`

配当利回りを計算。

##### `calculate_market_cap(price, shares) -> Optional[float]`

時価総額を計算。

##### `calculate_operating_margin(operating_profit, net_sales) -> Optional[float]`

営業利益率を計算。

##### `calculate_profit_margin(profit, net_sales) -> Optional[float]`

純利益率を計算。

##### `calculate_free_cash_flow(cf_operating, cf_investing) -> Optional[float]`

フリーキャッシュフローを計算。

---

## 分析モジュール (`backend/market_pipeline/analysis/`)

### IntegratedScoresRepository

```python
from market_pipeline.analysis.integrated_scores_repository import IntegratedScoresRepository

repo = IntegratedScoresRepository()
count = repo.save_scores(df, "2026-02-04")
scores = repo.get_scores("2026-02-04")
```

`integrated_scores`テーブルのCRUD操作を担当するリポジトリクラス。

#### コンストラクタ

```python
IntegratedScoresRepository(db_path: Path | None = None)
```

**パラメータ**:
- `db_path`: analysis_results.dbのパス（省略時はsettings.paths.analysis_dbを使用）

#### メソッド

##### `save_scores(df, date) -> int`

スコアと順位をDBに保存（UPSERT処理）。

**パラメータ**:
- `df` (`pd.DataFrame`): Code, composite_score, HlRatio, RelativeStrengthPercentageカラムを持つDataFrame
- `date` (`str`): 分析日（YYYY-MM-DD形式）

**戻り値**: `int` - 保存件数

**順位計算**:
- `rank(method='min')`を使用（同順位は最小値を使用）
- NULL値は順位計算から除外

##### `get_scores(date=None) -> pd.DataFrame`

指定日のスコアを取得。

**パラメータ**:
- `date` (`str | None`): 分析日（省略時は最新日）

**戻り値**: `pd.DataFrame` - Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank, created_at列を持つDataFrame

##### `get_history(code, days=30) -> pd.DataFrame`

銘柄の時系列データを取得。

**パラメータ**:
- `code` (`str`): 銘柄コード
- `days` (`int`): 取得日数

**戻り値**: `pd.DataFrame` - 日付降順でソートされた履歴データ

##### `get_latest_date() -> str | None`

データベース内の最新日付を取得。

**戻り値**: `str | None` - 最新日付またはデータがない場合はNone

##### `get_rank_changes(metric="composite_score", days=7, direction="up", min_change=1, limit=50) -> pd.DataFrame`

順位変動が大きい銘柄を取得。

**パラメータ**:
- `metric` (`str`): 順位指標（"composite_score", "hl_ratio", "rsp"）
- `days` (`int`): 比較日数
- `direction` (`str`): "up"（上昇）, "down"（下降）, "both"（両方）
- `min_change` (`int`): 最小順位変動幅
- `limit` (`int`): 最大結果数

**戻り値**: `pd.DataFrame` - Code, current_rank, past_rank, rank_change列を持つDataFrame

---

### MinerviniConfig

ミネルヴィニ分析の設定クラス。

```python
from market_pipeline.analysis.minervini import MinerviniConfig

config = MinerviniConfig(base_dir=Path("/path/to/project"))
```

#### コンストラクタ

```python
MinerviniConfig(base_dir: Optional[Path] = None)
```

**パラメータ**:
- `base_dir`: プロジェクトルートディレクトリ（省略時はデフォルトパス）

**属性**:
- `base_dir`: プロジェクトルートディレクトリ
- `data_dir`: データディレクトリ (`base_dir/data`)
- `logs_dir`: ログディレクトリ (`base_dir/logs`)
- `output_dir`: 出力ディレクトリ (`base_dir/output`)
- `jquants_db_path`: J-Quants DB パス
- `results_db_path`: 分析結果DB パス
- `error_output_dir`: エラー出力ディレクトリ

### MinerviniAnalyzer

マーク・ミネルヴィニのトレンドスクリーニング戦略。ベクトル化演算による最適化実装。

```python
from market_pipeline.analysis.minervini import MinerviniAnalyzer, MinerviniConfig

config = MinerviniConfig()
analyzer = MinerviniAnalyzer(config)
results = analyzer.calculate_strategy_vectorized(df)
```

#### コンストラクタ

```python
MinerviniAnalyzer(config: MinerviniConfig)
```

**パラメータ**:
- `config`: MinerviniConfigインスタンス

#### メソッド

##### `calculate_strategy_vectorized(df: pd.DataFrame) -> pd.DataFrame`

ベクトル化演算によるミネルヴィニ条件の計算。

**パラメータ**:
- `df` (`pd.DataFrame`): DateインデックスとAdjustmentCloseカラムを持つDataFrame

**戻り値**: `pd.DataFrame` - 以下のカラムを含むDataFrame

| カラム | 説明 |
|--------|------|
| `Close` | 終値 |
| `Sma50` | 50日移動平均 |
| `Sma150` | 150日移動平均 |
| `Sma200` | 200日移動平均 |
| `Type_1` | 株価が150日・200日MAより上（1.0/0.0） |
| `Type_2` | 150日MAが200日MAより上（1.0/0.0） |
| `Type_3` | 200日MAが1ヶ月間上昇トレンド（1.0/0.0） |
| `Type_4` | 50日MAが150日・200日MAより上（1.0/0.0） |
| `Type_5` | 株価が50日MAより上（1.0/0.0） |
| `Type_6` | 株価が52週安値の130%以上（1.0/0.0） |
| `Type_7` | 株価が52週高値の75%以上（1.0/0.0） |
| `Type_8` | RSI >= 70（別途計算、初期値NaN） |

### MinerviniDatabase

ミネルヴィニ分析のデータベース操作クラス。

```python
from market_pipeline.analysis.minervini import MinerviniDatabase, MinerviniConfig

config = MinerviniConfig()
database = MinerviniDatabase(config)
database.init_database(source_db_path, dest_db_path, code_list)
```

#### コンストラクタ

```python
MinerviniDatabase(config: MinerviniConfig)
```

**パラメータ**:
- `config`: MinerviniConfigインスタンス

#### メソッド

##### `init_minervini_table(dest_db_path: str)`

Minerviniテーブルとインデックスを初期化。

##### `init_database(source_db_path: str, dest_db_path: str, code_list: List[str], n_workers: Optional[int] = None)`

全銘柄のMinerviniデータベースを並列処理で初期化。

##### `update_database(source_db_path: str, dest_db_path: str, code_list: List[str], calc_start_date: str, calc_end_date: str, period: int = 5)`

Minerviniデータベースを更新。

##### `update_type8(dest_db_path: str, date_list: List[str], period: int = -5)`

Type 8（相対力指数）をバッチ操作で更新。

### 後方互換関数

```python
from market_pipeline.analysis.minervini import init_minervini_db, update_minervini_db, update_type8_db

# 初期化（SQLite接続オブジェクト使用）
init_minervini_db(source_conn, dest_conn, code_list, n_workers)

# 更新
update_minervini_db(source_conn, dest_conn, code_list, calc_start_date, calc_end_date, period)

# Type 8更新
update_type8_db(conn, date_list, period)
```

### High-Low Ratio 関数 (`backend/market_pipeline/analysis/high_low_ratio.py`)

52週高値・安値位置比率の計算。関数ベースで実装。

```python
from market_pipeline.analysis.high_low_ratio import calc_hl_ratio_for_all, calc_hl_ratio_by_code

# 全銘柄のHL比率を計算
ratio_df = calc_hl_ratio_for_all(db_path, end_date, weeks=52)

# 特定銘柄のHL比率を計算
ratios, price_df = calc_hl_ratio_by_code("7203", db_path, end_date, weeks=52)
```

#### 関数

##### `init_hl_ratio_db(db_path=RESULTS_DB_PATH)`

HL比率テーブルを初期化。

**パラメータ**:
- `db_path`: 結果データベースのパス

##### `calc_hl_ratio_for_all(db_path=JQUANTS_DB_PATH, end_date=None, weeks=52, n_workers=None) -> pd.DataFrame`

全銘柄のHL比率を並列処理で計算。

**パラメータ**:
- `db_path`: jquants.dbのパス
- `end_date`: 計算終了日（省略時は当日）
- `weeks`: 計算期間（週数、デフォルト52週）
- `n_workers`: 並列ワーカー数（省略時は自動）

**戻り値**: `pd.DataFrame` - Code, HlRatio, MedianRatio, Date, Weeks

##### `calc_hl_ratio_by_code(code, db_path=JQUANTS_DB_PATH, end_date=None, weeks=52, save_to_db=True) -> Tuple[Dict, pd.DataFrame]`

特定銘柄のHL比率を計算。

**パラメータ**:
- `code` (`str`): 銘柄コード
- `db_path`: jquants.dbのパス
- `end_date`: 計算終了日
- `weeks`: 計算期間（週数）
- `save_to_db`: データベースに保存するか

**戻り値**: `Tuple[Dict, pd.DataFrame]` - ({'HlRatio': float, 'MedianRatio': float}, 価格DataFrame)

##### `calc_ratios_vectorized(df, weeks=52) -> pd.DataFrame`

ベクトル化されたHL比率計算（内部関数）。

**パラメータ**:
- `df`: Date, Code, High, Low, AdjustmentCloseカラムを持つDataFrame
- `weeks`: 計算期間（週数）

**戻り値**: `pd.DataFrame` - Code, HlRatio, MedianRatio

### Relative Strength 関数 (`backend/market_pipeline/analysis/relative_strength.py`)

相対力指標（RSP/RSI）の計算。関数ベースで実装。

```python
from market_pipeline.analysis.relative_strength import (
    init_rsp_db, update_rsp_db, update_rsi_db,
    relative_strength_percentage_vectorized
)

# RSPデータベースを初期化（全履歴計算）
processed, errors = init_rsp_db(db_path, result_db_path)

# RSPを更新（直近のみ）
processed, errors = update_rsp_db(db_path, result_db_path, calc_end_date="2024-01-01")

# RSIを更新（RSPからランキング計算）
errors = update_rsi_db(result_db_path)
```

#### 関数

##### `init_results_db(db_path)`

相対力結果テーブルを初期化。

##### `init_rsp_db(db_path=JQUANTS_DB_PATH, result_db_path=RESULTS_DB_PATH, n_workers=None) -> Tuple[int, int]`

全銘柄のRSPを初期計算してデータベースに保存。

**パラメータ**:
- `db_path`: jquants.dbのパス
- `result_db_path`: 結果データベースのパス
- `n_workers`: 並列ワーカー数

**戻り値**: `Tuple[int, int]` - (処理成功数, エラー数)

##### `update_rsp_db(db_path=JQUANTS_DB_PATH, result_db_path=RESULTS_DB_PATH, calc_start_date=None, calc_end_date=None, period=-5, n_workers=None) -> Tuple[int, int]`

直近のRSPを更新。

**パラメータ**:
- `db_path`: jquants.dbのパス
- `result_db_path`: 結果データベースのパス
- `calc_start_date`: 計算開始日
- `calc_end_date`: 計算終了日
- `period`: 更新期間（負数で直近N日）
- `n_workers`: 並列ワーカー数

**戻り値**: `Tuple[int, int]` - (処理成功数, エラー数)

##### `update_rsi_db(result_db_path=RESULTS_DB_PATH, date_list=None, period=-5) -> int`

RSI（相対力指数）を計算して更新。RSPを基にランキングを計算。

**パラメータ**:
- `result_db_path`: 結果データベースのパス
- `date_list`: 処理対象日リスト（省略時は自動取得）
- `period`: 更新期間

**戻り値**: `int` - エラー数

##### `relative_strength_percentage_vectorized(df, period=200) -> pd.DataFrame`

RSP（相対力パーセンテージ）をベクトル化計算。

**パラメータ**:
- `df`: Date インデックスと AdjustmentClose カラムを持つDataFrame
- `period`: 計算期間（日数）

**戻り値**: `pd.DataFrame` - RelativeStrengthPercentage カラムを追加したDataFrame

### OptimizedChartClassifier / ChartClassifier

MLベースのチャートパターン分類。テンプレートマッチングによるパターン検出。

```python
from market_pipeline.analysis.chart_classification import OptimizedChartClassifier, ChartClassifier

# OptimizedChartClassifier（推奨）
classifier = OptimizedChartClassifier(
    ticker="7203",
    window=60,
    price_data=price_series,  # オプション: 事前ロード済みデータ
    logger=logger
)
label, score, data_date = classifier.classify_latest()

# ChartClassifier（後方互換性ラッパー）
classifier = ChartClassifier(ticker="7203", window=60, db_path=db_path)
```

#### OptimizedChartClassifier コンストラクタ

```python
OptimizedChartClassifier(
    ticker: str,
    window: int,
    price_data: Optional[pd.Series] = None,
    logger: Optional[logging.Logger] = None
)
```

**パラメータ**:
- `ticker`: 銘柄コード
- `window`: 分析ウィンドウサイズ（20, 60, 120, 240, 960, 1200）
- `price_data`: 事前ロード済みの価格データ（省略時はDBから取得）
- `logger`: ロガーインスタンス

#### ChartClassifier コンストラクタ（後方互換）

```python
ChartClassifier(ticker: str, window: int, db_path: str = JQUANTS_DB_PATH)
```

**パラメータ**:
- `ticker`: 銘柄コード
- `window`: 分析ウィンドウサイズ
- `db_path`: jquants.dbのパス

#### メソッド

##### `classify_latest() -> Tuple[str, float, str]`

最新の価格データでチャートパターンを分類。

**戻り値**: `Tuple[str, float, str]` - (パターンラベル, 相関スコア, データ日付)

**パターンラベル**:
- `uptrend`: 上昇トレンド
- `downtrend`: 下降トレンド
- `double_bottom`: ダブルボトム
- `head_and_shoulders`: ヘッドアンドショルダーズ
- `その他のパターン`

##### `save_classification_plot(label, score, output_dir)`

分類結果のプロットを保存。

**パラメータ**:
- `label`: パターンラベル
- `score`: 相関スコア
- `output_dir`: 出力ディレクトリ

#### ヘルパー関数

##### `get_adaptive_windows(data_length: int) -> List[int]`

データ長に応じた適切なウィンドウサイズリストを返す。

##### `main_full_run_optimized()`

全銘柄の分類を実行するメイン関数（スクリプトから呼び出し）。

---

## マスターモジュール (`backend/market_pipeline/master/`)

### StockMasterDB

東証上場銘柄のマスター情報を管理するクラス。各データソース間の共通参照テーブルを提供。

```python
from market_pipeline.master.master_db import StockMasterDB

master_db = StockMasterDB(db_path="data/master.db")

# マスターデータを最新の東証情報で更新
master_db.update_master_data()

# 全銘柄を取得
stocks_df = master_db.get_all_stocks(active_only=True)

# 銘柄コードで検索
stock_info = master_db.get_stock_by_code("7203")

# 業種で検索
sector_stocks = master_db.get_stocks_by_sector("輸送用機器")

# 市場で検索
prime_stocks = master_db.get_stocks_by_market("プライム")

# 統計情報を取得
stats = master_db.get_statistics()
```

#### コンストラクタ

```python
StockMasterDB(db_path: str = None)
```

**パラメータ**:
- `db_path`: データベースファイルのパス（省略時は `data/master.db`）

#### メソッド

##### `update_master_data() -> bool`

東証上場銘柄一覧をダウンロードしてマスターデータを更新。

**戻り値**: `bool` - 更新成功時 True

##### `get_all_stocks(active_only: bool = True) -> pd.DataFrame`

全銘柄情報を取得。

**パラメータ**:
- `active_only`: アクティブな銘柄のみ取得するか

**戻り値**: `pd.DataFrame` - 銘柄情報（code, name, sector, market, market_product_category, yfinance_symbol, jquants_code, is_active）

##### `get_stock_by_code(code: str) -> Optional[Dict[str, Any]]`

銘柄コードから銘柄情報を取得。

**パラメータ**:
- `code`: 銘柄コード（4桁）

**戻り値**: `Dict[str, Any]` または `None`

##### `get_stocks_by_sector(sector: str, active_only: bool = True) -> pd.DataFrame`

業種で銘柄を取得。

**パラメータ**:
- `sector`: 業種名（33業種区分）
- `active_only`: アクティブな銘柄のみ取得するか

**戻り値**: `pd.DataFrame` - 該当銘柄情報

##### `get_stocks_by_market(market: str, active_only: bool = True) -> pd.DataFrame`

市場で銘柄を取得。

**パラメータ**:
- `market`: 市場名（プライム、スタンダード、グロース）
- `active_only`: アクティブな銘柄のみ取得するか

**戻り値**: `pd.DataFrame` - 該当銘柄情報

##### `get_statistics() -> Dict[str, Any]`

マスターデータの統計情報を取得。

**戻り値**: `Dict` - total_stocks, active_stocks, inactive_stocks, sector_distribution, market_distribution, last_updated

##### `download_tse_listed_stocks() -> Optional[str]`

東証上場銘柄一覧のExcelファイルをダウンロード（内部使用）。

##### `load_tse_stocks_from_excel(file_path: str) -> pd.DataFrame`

ExcelファイルからTSE上場銘柄データを読み込み（内部使用）。

---

## ユーティリティ (`backend/market_pipeline/utils/`)

### SlackNotifier

Slack Incoming Webhookによるジョブ結果通知。

```python
from market_pipeline.utils import SlackNotifier, JobContext, JobResult
```

#### SlackSettings (`backend/market_pipeline/config/settings.py`)

| 属性 | 型 | デフォルト | 環境変数 | 説明 |
|-----|-----|---------|---------|------|
| `webhook_url` | `str` | `""` | `SLACK_WEBHOOK_URL` | Webhook URL |
| `error_webhook_url` | `str` | `""` | `SLACK_ERROR_WEBHOOK_URL` | エラー専用Webhook URL（オプション） |
| `enabled` | `bool` | `True` | `SLACK_ENABLED` | 通知の有効/無効 |
| `timeout_seconds` | `int` | `10` | `SLACK_TIMEOUT_SECONDS` | HTTPタイムアウト（秒） |
| `max_retries` | `int` | `3` | `SLACK_MAX_RETRIES` | リトライ回数 |

**プロパティ**:
- `is_configured` (`bool`): `webhook_url`が非空かつ`enabled`がTrueの場合True

#### JobResult

ジョブ実行結果を格納するデータクラス。

```python
@dataclass
class JobResult:
    job_name: str
    success: bool = True
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metrics: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
```

**プロパティ**:
- `duration_formatted` (`str`): 実行時間を"X分Y秒"形式で返す。開始/終了時刻が未設定の場合は"不明"

#### SlackNotifier

Slack Webhook APIへの通知送信クラス。

##### コンストラクタ

```python
SlackNotifier()
```

設定は`get_settings().slack`から自動取得。

##### プロパティ

- `is_available` (`bool`): `webhook_url`が非空かつ`enabled`がTrueの場合True

##### メソッド

###### `send_success(job_result: JobResult) -> None`

成功通知を送信。メトリクスと警告を含む。

###### `send_error(job_result: JobResult) -> None`

エラー通知を送信。`error_webhook_url`が設定されている場合はそちらに送信。トレースバック情報を含む。

###### `send_warning(job_name: str, message: str, details: str = "") -> None`

警告通知を送信。

**共通動作**:
- `is_available`がFalseの場合、送信をスキップしDEBUGログを出力
- 送信失敗時は`max_retries`回リトライ（1秒間隔）
- 全リトライ失敗後はWARNINGログを出力し例外を発生させない

#### JobContext

コンテキストマネージャでジョブを囲み、成功/エラー通知を自動送信。

```python
with JobContext("ジョブ名") as job:
    job.add_metric("レコード数", "1,000")
    job.add_warning("一部データ欠損")
# 正常終了時は成功通知、例外時はエラー通知を自動送信
```

##### コンストラクタ

```python
JobContext(job_name: str)
```

##### メソッド

###### `add_metric(key: str, value: str) -> None`

通知に含めるメトリクスを追加。

###### `add_warning(message: str) -> None`

通知に含める警告を追加。

**動作**:
- `__enter__`: 開始時刻を記録し自身を返す
- `__exit__`: 終了時刻を記録し、例外の有無に応じて`send_success`/`send_error`を呼び出す
- 例外発生時はエラー通知後に例外を再送出
- 通知処理自体の例外はキャッチしてWARNINGログに記録（ジョブの結果に影響しない）

---

### ParallelProcessor

並列処理のラッパークラス。

```python
from market_pipeline.utils.parallel_processor import ParallelProcessor

processor = ParallelProcessor(n_workers=8)
results = processor.process(items, process_func)
```

#### コンストラクタ

```python
ParallelProcessor(n_workers: int = None, use_threads: bool = False)
```

**パラメータ**:
- `n_workers`: ワーカー数（デフォルト: CPUコア数）
- `use_threads`: ThreadPoolExecutorを使用（デフォルト: ProcessPoolExecutor）

#### メソッド

##### `process(items, func, batch_size=100) -> list`

アイテムを並列処理。

**パラメータ**:
- `items` (`list`): 処理対象リスト
- `func` (`callable`): 処理関数
- `batch_size` (`int`): バッチサイズ

**戻り値**: `list` - 処理結果

### BatchDatabaseProcessor

効率的なバッチデータベース操作のユーティリティクラス。

```python
from market_pipeline.utils.parallel_processor import BatchDatabaseProcessor

processor = BatchDatabaseProcessor(db_path, batch_size=1000)

# バッチ挿入
inserted = processor.batch_insert('table_name', records, on_conflict='REPLACE')

# バッチ取得
df = processor.batch_fetch(query, params=[], as_dataframe=True)

# インデックス作成
processor.create_indexes([
    {'name': 'idx_name', 'table': 'table_name', 'columns': ['col1', 'col2'], 'unique': False}
])
```

#### コンストラクタ

```python
BatchDatabaseProcessor(db_path: str, batch_size: int = 1000)
```

**パラメータ**:
- `db_path`: SQLiteデータベースのパス
- `batch_size`: バッチサイズ

#### メソッド

##### `batch_insert(table_name, data, on_conflict='REPLACE') -> int`

バッチ挿入を実行。

**パラメータ**:
- `table_name` (`str`): テーブル名
- `data` (`List[Dict]`): 行データのリスト
- `on_conflict` (`str`): 競合時の処理（'REPLACE' | 'IGNORE' | 'ABORT' | 'ROLLBACK' | 'FAIL'）

**戻り値**: `int` - 挿入された行数

##### `batch_fetch(query, params=None, as_dataframe=True) -> Union[pd.DataFrame, List[Tuple]]`

効率的なデータ取得。

**パラメータ**:
- `query` (`str`): SQLクエリ
- `params` (`List[Any]`): クエリパラメータ
- `as_dataframe` (`bool`): DataFrameで返すか

**戻り値**: `pd.DataFrame` または `List[Tuple]`

##### `create_indexes(index_definitions: List[Dict]) -> None`

データベースインデックスを作成。

### measure_performance

関数の実行時間を計測するデコレータ。

```python
from market_pipeline.utils.parallel_processor import measure_performance

@measure_performance
def my_function():
    # 処理
    pass
```

### CacheManager

メモリ・ディスクベースのキャッシュ管理クラス。

```python
from market_pipeline.utils.cache_manager import CacheManager, get_cache

# グローバルキャッシュインスタンスを取得（推奨）
cache = get_cache()

# または直接初期化
cache = CacheManager(
    cache_dir="/path/to/cache",
    max_memory_items=100,
    default_ttl_hours=24
)

# キャッシュに保存
cache.put("key", data, ttl_hours=24, use_disk=True)

# キャッシュから取得
data = cache.get("key", use_disk=True)

# 関数デコレータとして使用
@cache.cached_function(ttl_hours=6, use_disk=True)
def expensive_calculation(param):
    return result
```

#### コンストラクタ

```python
CacheManager(
    cache_dir: Optional[str] = None,
    max_memory_items: int = 100,
    default_ttl_hours: int = 24
)
```

**パラメータ**:
- `cache_dir`: ディスクキャッシュディレクトリ（省略時はシステムtempを使用）
- `max_memory_items`: メモリキャッシュの最大アイテム数
- `default_ttl_hours`: デフォルトの有効期限（時間）

#### メソッド

##### `put(key, data, ttl_hours=None, use_disk=True)`

データをキャッシュに保存。

##### `get(key, use_disk=True) -> Any | None`

キャッシュからデータを取得。期限切れの場合はNone。

##### `clear_memory()`

メモリキャッシュをクリア。

##### `clear_disk()`

ディスクキャッシュをクリア。

##### `clear_all()`

全キャッシュをクリア。

##### `get_stats() -> Dict`

キャッシュ統計を取得（memory_items, disk_items, disk_size_mb, cache_dir）。

##### `cleanup_expired()`

期限切れのキャッシュエントリを削除。

#### ヘルパー関数

##### `get_cache() -> CacheManager`

グローバルキャッシュインスタンスを取得。

##### `cache_dataframe(key, df, ttl_hours=24)`

pandas DataFrameをキャッシュに保存。

##### `get_cached_dataframe(key) -> Optional[pd.DataFrame]`

キャッシュからDataFrameを取得。

---

## スクリプト (`scripts/`)

### run_daily_jquants.py

```bash
python scripts/run_daily_jquants.py
```

J-Quants APIから日次株価データを取得し、`data/jquants.db`に保存。

### run_daily_analysis.py

```bash
python scripts/run_daily_analysis.py
python scripts/run_daily_analysis.py --modules hl_ratio rsp minervini
```

**オプション**:
- `--modules`: 実行する分析モジュールを指定

### run_weekly_tasks.py

```bash
python scripts/run_weekly_tasks.py
python scripts/run_weekly_tasks.py --statements-only
python scripts/run_weekly_tasks.py --analysis-only
```

**オプション**:
- `--statements-only`: 財務諸表取得のみ
- `--analysis-only`: 統合分析のみ

### run_monthly_master.py

```bash
python scripts/run_monthly_master.py
```

銘柄マスターデータを更新。

### run_adhoc_integrated_analysis.py

```bash
python scripts/run_adhoc_integrated_analysis.py
```

アドホック統合分析を実行。スケジュール外で手動実行する場合に使用。

### create_database_indexes.py

```bash
python scripts/create_database_indexes.py
```

SQLiteデータベースにインデックスを作成（初回セットアップ時）。
