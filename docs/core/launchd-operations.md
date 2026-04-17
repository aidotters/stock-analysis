# launchd 運用ガイド

本プロジェクトではmacOSのlaunchdでジョブをスケジュール実行している。
2026-02-28にcronから移行した（スリープ復帰後に自動実行されるため）。

## ジョブ一覧

| ジョブ | plistラベル | スケジュール | スクリプト |
|--------|------------|-------------|-----------|
| 日次データ取得 | `com.tak.stock-analysis.daily-jquants` | 平日 18:00 | `scripts/run_daily_jquants.py` |
| 週次タスク | `com.tak.stock-analysis.weekly-tasks` | 土曜 06:00 | `scripts/run_weekly_tasks.py` |
| 月次マスタ更新 | `com.tak.stock-analysis.monthly-master` | 毎月1日 20:30 | `scripts/run_monthly_master.py` |

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
