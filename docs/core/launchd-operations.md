# launchd 運用ガイド

本プロジェクトではmacOSのlaunchdでジョブをスケジュール実行している。
2026-02-28にcronから移行した（スリープ復帰後に自動実行されるため）。

## ジョブ一覧

| ジョブ | plistラベル | スケジュール | スクリプト |
|--------|------------|-------------|-----------|
| 日次データ取得 | `com.tak.stock-analysis.daily-jquants` | 平日 18:00 | `scripts/run_daily_jquants.py` |
| 週次タスク | `com.tak.stock-analysis.weekly-tasks` | 土曜 06:00 | `scripts/run_weekly_tasks.py` |
| 月次マスタ更新 | `com.tak.stock-analysis.monthly-master` | 毎月1日 20:30 | `scripts/run_monthly_master.py` |
| ニュース配信(朝) | `com.tak.stock-analysis.news-delivery-morning` | 平日 08:00 | `scripts/run_news_delivery.py --slot morning` |
| ニュース配信(昼) | `com.tak.stock-analysis.news-delivery-noon` | 平日 12:30 | `scripts/run_news_delivery.py --slot noon` |
| ニュース配信(夜) | `com.tak.stock-analysis.news-delivery-evening` | 平日 19:30 | `scripts/run_news_delivery.py --slot evening` |

### ニュース配信ジョブの登録手順

`launchd/com.stock-analysis.news-delivery-{morning,noon,evening}.plist.template` をリポジトリに同梱。
スロットごとに次の手順で登録する（昼配信が不要であれば noon 分を省略可能）:

```bash
# 1. テンプレートを LaunchAgents へコピー (slot は morning / noon / evening)
SLOT=evening
cp launchd/com.stock-analysis.news-delivery-${SLOT}.plist.template \
   ~/Library/LaunchAgents/com.tak.stock-analysis.news-delivery-${SLOT}.plist

# 2. ジョブをロード
launchctl load ~/Library/LaunchAgents/com.tak.stock-analysis.news-delivery-${SLOT}.plist

# 3. 登録確認
launchctl list | grep news-delivery
```

各ジョブは平日のみ起動し、`RunAtLoad=false` のため再起動・スリープ復帰時には即時実行されない。
書き込み先は `data/news_delivery.db` のみで他ジョブとの競合は発生しない。

### スロットの考え方

- **朝 (08:00)**: 寄り付き前。前日夜〜早朝に出た適時開示・ニュースをまとめて把握
- **昼 (12:30)**: 前場引け後の昼休み。午前中の発表を午後の取引に活かす
- **夜 (19:30)**: 場中・引け後の発表をまとめて確認

3スロット同時に運用する必要はなく、用途に応じて取捨選択する。**当初は朝＋夜の2スロットで運用** し、以下のような状況で noon を `launchctl load` で追加するのが推奨パターン:

- **昼配信を追加すべき具体例:**
  - **デイトレ・短期スイング運用**: 午前のIRリリースを当日午後の判断に反映したい
  - **決算発表シーズン (4月下旬〜5月、10月下旬〜11月)**: ウォッチ銘柄の本決算・四半期決算が午前に集中する時期に午後の対応が遅れないようにする
  - **昼休みの相場確認を業務に組み込んでいる**: 日中の取引ができる環境で、12:30〜13:00 の前場引け後 30 分にニュースを確認したい
- **昼配信を追加しなくてよい具体例:**
  - 中長期ホールド主体（夜の確認だけで十分）
  - 平日昼間に Slack を見る習慣がない
  - Slack 通知数を減らしたい（朝＋夜の2スロットでも `lookback-days` を伸ばせば取りこぼしは少ない）

### 重複排除DB のクリーンアップ

`data/news_delivery.db` の `delivered_news` テーブルは時間経過で肥大化するため、定期的に古いレコードを削除する。`scripts/cleanup_news_db.py` を使用する:

```bash
# デフォルト (90日経過レコードを削除)
python scripts/cleanup_news_db.py

# 60日に変更
python scripts/cleanup_news_db.py --days 60
```

cron / launchd で月次実行する運用も可能。スキーマ詳細は `README.md` の "News Delivery Module" セクションを参照。

### 取得ソースの選択 (`--sources`)

`scripts/run_news_delivery.py` は `--sources` オプションで取得ソースを絞り込める:

```bash
# 全ソース (デフォルト): 四季報CDP + Google News RSS + TDnet RSS
python scripts/run_news_delivery.py --slot evening

# 適時開示のみ
python scripts/run_news_delivery.py --slot evening --sources disclosure

# 一般ニュースとTDnetのみ (四季報CDPを使わない=Playwright不要で軽量)
python scripts/run_news_delivery.py --slot morning --sources general_news,ir_release
```

ソース別の特徴:

| ソース | 取得元 | 特徴 |
|--------|--------|------|
| `disclosure` | 四季報オンライン (Playwright/CDP) | フィルタ済み適時開示。重要度ラベル `high` を付与 |
| `general_news` | Google News RSS | 銘柄名 + コードで検索。Yahoo!ファイナンス等のノイズは exclude フィルタで除外 |
| `ir_release` | yanoshin TDnet ラッパー (Atom) | TDnet 一次情報。四季報より早く反映される傾向 |
| `stock_news` | 四季報銘柄ページ (Playwright/CDP) | 「この銘柄の関連記事」セクション。ログミー / お宝銘柄日々発見術等の編集記事 |

### lookback 設定

`STOCK_NEWS_LOOKBACK_DAYS` または `--lookback-days` で取得対象期間を制御。デフォルトは 7 日。

```bash
# 直近1日のみ (短時間スロット間隔向け)
python scripts/run_news_delivery.py --slot morning --lookback-days 1

# 過去30日 (深掘り運用観察向け)
python scripts/run_news_delivery.py --slot evening --lookback-days 30
```

### レート制限

Google News RSS / TDnet RSS は内部で1分あたりのリクエスト上限 (`RateLimiter`) を持つ。到達した場合 `RateLimitError` が発生し、`DeliveryService` は **priority=high のウォッチ銘柄のみで自動再試行**する。`JobContext.warnings` および `metrics` に「レート制限到達」が記録され、Slack のジョブ通知で確認可能。

### チェーン実行フロー（日次）

DB競合を回避するため、subprocessで順次実行する設計:

```
launchd (18:00)
  └→ run_daily_jquants.py     # J-Quants APIから株価取得
       └→ run_daily_analysis.py    # 日次分析（Minervini, HL比率, RSP等）
            └→ integrated_analysis2.py  # 統合分析 → Slack通知
```

`--no-chain` フラグで個別実行も可能:
```bash
uv run scripts/run_daily_jquants.py --no-chain   # J-Quants取得のみ
uv run scripts/run_daily_analysis.py --no-chain   # 日次分析のみ（統合分析なし）
```

## plistファイル

### 配置場所

```
~/Library/LaunchAgents/com.tak.stock-analysis.*.plist
```

### plistの構造（テンプレート）

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tak.stock-analysis.ジョブ名</string>

    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/uv</string>
        <string>run</string>
        <string>scripts/対象スクリプト.py</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/Users/tak/Markets/Stocks/stock-analysis</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/Users/tak/.asdf/shims:/opt/homebrew/bin</string>
    </dict>

    <!-- スケジュール設定 -->
    <key>StartCalendarInterval</key>
    <array>
        <dict>
            <key>Weekday</key>    <!-- 0=日, 1=月, ..., 6=土 -->
            <integer>1</integer>
            <key>Hour</key>
            <integer>18</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <!-- 複数曜日は dict を繰り返す -->
    </array>

    <!-- 毎月N日の場合は Weekday の代わりに Day を使用 -->
    <!--
    <key>StartCalendarInterval</key>
    <dict>
        <key>Day</key>
        <integer>1</integer>
        <key>Hour</key>
        <integer>20</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>
    -->

    <key>StandardOutPath</key>
    <string>/Users/tak/Markets/Stocks/stock-analysis/logs/ジョブ名.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/tak/Markets/Stocks/stock-analysis/logs/ジョブ名.log</string>
</dict>
</plist>
```

### StartCalendarInterval の主要キー

| キー | 値 | 説明 |
|------|-----|------|
| `Weekday` | 0-6 | 曜日（0=日曜, 1=月曜, ..., 6=土曜） |
| `Day` | 1-31 | 月の日 |
| `Hour` | 0-23 | 時 |
| `Minute` | 0-59 | 分 |
| `Month` | 1-12 | 月 |

省略したキーはワイルドカード扱い（毎回マッチ）。

## 操作コマンド

### ジョブの登録・解除

```bash
# 登録（ロード）
launchctl load ~/Library/LaunchAgents/com.tak.stock-analysis.daily-jquants.plist

# 解除（アンロード）
launchctl unload ~/Library/LaunchAgents/com.tak.stock-analysis.daily-jquants.plist

# 再登録（設定変更後）
launchctl unload ~/Library/LaunchAgents/com.tak.stock-analysis.daily-jquants.plist
launchctl load ~/Library/LaunchAgents/com.tak.stock-analysis.daily-jquants.plist
```

> **注意**: `launchctl unload` はシステム再起動やログイン時に再ロードされることがある。
> 恒久的に無効化するにはplistファイル自体を削除するか、`Disabled`キーを追加する。

### ジョブの即時実行

```bash
# kickstart で即時起動
launchctl kickstart gui/$(id -u)/com.tak.stock-analysis.daily-jquants
```

### 状態確認

```bash
# ロード済みジョブ一覧
launchctl list | grep stock-analysis

# 個別ジョブの詳細（exitステータス等）
launchctl list com.tak.stock-analysis.daily-jquants
```

`launchctl list` の出力:
```
PID    Status    Label
-      0         com.tak.stock-analysis.daily-jquants
```
- **PID**: `-` なら待機中、数値なら実行中
- **Status**: 最後の終了コード（0=成功）

### ログの確認

```bash
# リアルタイム監視
tail -f logs/daily_jquants.log
tail -f logs/weekly_tasks.log
tail -f logs/monthly_master.log

# 直近のエラー確認
grep -i error logs/daily_jquants.log | tail -20

# launchd自体のエラー（plist構文エラー等）
log show --predicate 'subsystem == "com.apple.xpc.launchd"' --last 1h | grep stock-analysis
```

## トラブルシューティング

### ジョブが実行されない

1. **ロード状態を確認**
   ```bash
   launchctl list | grep stock-analysis
   ```
   表示されなければ `launchctl load` で登録する。

2. **plistの構文検証**
   ```bash
   plutil -lint ~/Library/LaunchAgents/com.tak.stock-analysis.daily-jquants.plist
   ```

3. **PATHの問題**: launchdはログインシェルのPATHを継承しない。
   plistの `EnvironmentVariables` に必要なパスを明示する。

4. **スリープ中に予定時刻を過ぎた場合**: launchdは復帰後に1回だけ実行する（cronとの主な違い）。

### ジョブが重複実行される

過去の事例（2026-04-14）: チェーン実行に移行後も個別ジョブのplistが残っていた。

- 不要なplistは `launchctl unload` だけでなく**ファイルを削除**する
- `unload` だけでは再起動時に再ロードされる場合がある

### 終了コードが0以外

```bash
# 終了コード確認
launchctl list com.tak.stock-analysis.daily-jquants

# ログで原因確認
tail -50 logs/daily_jquants.log
```

スクリプトはエラー時に `sys.exit(1)` で終了し、Slack通知も送信される。

## ジョブの追加手順

1. 実行スクリプトを `scripts/` に作成
2. plistファイルを作成（上記テンプレートを参考）
3. 構文チェック: `plutil -lint <plist>`
4. 登録: `launchctl load ~/Library/LaunchAgents/<plist>`
5. 動作確認: `launchctl kickstart gui/$(id -u)/<label>`
6. ログ確認: `tail -f logs/<ジョブ名>.log`

## セットアップスクリプト

初期セットアップや一括操作用のスクリプトが `~/.local/share/launchd/` にある:

```bash
cd ~/.local/share/launchd
./setup.sh            # 全ジョブの登録
./setup.sh --status   # 全ジョブの状態確認
```

## 参考

- plistファイル: `~/Library/LaunchAgents/com.tak.stock-analysis.*.plist`
- ログ: `logs/`（プロジェクトルート直下）
- crontab移行前バックアップ: `~/.local/share/launchd/crontab_backup.txt`
- Apple公式ドキュメント: `man launchd.plist`

## 四季報の有料記事を取得する場合の初回ログイン手順 (オプション)

`stock_news` / `disclosure` ソース (`CdpDisclosureFetcher` / `ShikihoStockNewsFetcher`) は四季報オンラインを Playwright で巡回するが、**「この銘柄の関連記事」一覧 / 適時開示一覧は無料で取得可能**であり、本手順は実施しなくても基本的な配信は動作する。

四季報オンラインの**有料記事本文**（東洋経済オンライン会員限定記事等）を将来的に取得対象に加える場合に限り、専用Chromiumプロファイルへの初回ログインが必要。

```bash
# 1. 専用プロファイルを headed (GUI) で起動。無料記事配信時と同じプロファイルを使う。
python -c "
from playwright.sync_api import sync_playwright
from pathlib import Path
with sync_playwright() as pw:
    ctx = pw.chromium.launch_persistent_context(
        user_data_dir=str(Path.home()/'.stock-news/chrome-profile'),
        headless=False,
    )
    page = ctx.new_page()
    page.goto('https://shikiho.toyokeizai.net/')
    input('Chromium ウィンドウでログイン → Enter で終了: ')
    ctx.close()
"

# 2. ウィンドウが開いたら、画面右上「ログイン」から会員アカウントでサインインする
#    (有料プラン契約済みの会員アカウント)。Cookie/セッションがプロファイルに永続化される。

# 3. ログイン状態の確認: ヘッダーに自分のアカウント名が表示されること

# 4. ターミナルで Enter キーを押して Python スクリプトを終了

# 5. 以降の launchd ジョブは headless=True でも同プロファイルから Cookie を引き継ぐ
```

ログインが切れた場合（数ヶ月単位）、上記を再実行する。launchd ジョブの実行ログ
(`logs/news_delivery_*.log`) で「ログイン要求」関連の警告が出始めたら再ログインのサイン。

このログイン状態はオプションであり、未ログインでも `stock_news` fetcher は記事一覧（タイトル・URL・日時）を取得して配信する。本文閲覧時に四季報側でログイン誘導が出るのは Slack 通知から記事を開いた閲覧者の責任範囲となる。
