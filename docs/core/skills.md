# Claude Code スキルガイド

本プロジェクトで利用可能な Claude Code スキルの索引と、投資分析スキルの詳細な使い方をまとめる（CLAUDE.md から移設）。

**構成ファイル:** 各スキルの定義は `.claude/skills/<skill-name>/SKILL.md`

## スキル一覧

| カテゴリ | スキル | 用途 |
|---------|--------|------|
| 投資分析 | `/discover-stocks` | ニュースドリブン銘柄発見（巡回・抽出・裏付け・リスク分析） |
| 投資分析 | `/analyze-stock` | 銘柄詳細分析（企業・財務・テクニカル統合レポート） |
| 投資分析 | `/research-stock-news` | 特定銘柄のニュース・適時開示・IR情報の包括調査 |
| 投資分析 | `/research-executives` | 経営陣6軸スコアリング（独立した executive_report.md 生成） |
| 投資分析 | `/watch` | ウォッチリストCRUD（add / list / update / remove） |
| 投資分析 | `/sync-notion` | 投資分析レポートを Notion 親ページ配下にページ投入（同銘柄再投入は自動アーカイブ） |
| ドキュメント作成 | `/architecture-design` | アーキテクチャ設計書の作成 |
| ドキュメント作成 | `/functional-design` | 機能設計書の作成 |
| ドキュメント作成 | `/development-guidelines` | 開発ガイドラインの作成 |
| ドキュメント作成 | `/repository-structure` | リポジトリ構造定義書の作成 |
| ドキュメント作成 | `/prd-writing` | PRD（製品要件定義書）の作成 |
| ドキュメント作成 | `/glossary-creation` | 用語集の作成 |
| 開発フロー | `/brainstorm` | アイデア壁打ち → docs/ideas/ に保存 |
| 開発フロー | `/plan-feature` | 機能の計画ドキュメント作成 |
| 開発フロー | `/implement-feature` | 計画に基づく機能実装 |
| 開発フロー | `/initial-setup` | プロジェクト初期セットアップ |
| 品質管理 | `/steering` | 作業計画・タスクリスト管理（実装フローの全体管理） |
| 品質管理 | `/validation` | コード品質検証と受け入れテスト |
| 品質管理 | `/validate-code` | コード品質・設計整合性検証 |
| 品質管理 | `/acceptance-test` | 受け入れ条件の検証 |
| 品質管理 | `/review-docs` | ドキュメント品質レビュー |
| 品質管理 | `/update-docs` | 実装済みコードとドキュメントの同期 |
| 品質管理 | `/gen-all-docs` | 全ドキュメント一括生成 |

**表記ルール:** ドキュメント本文ではスキル名は単純表記（例: `/watch`）に統一する。引数や使用例は別途コードブロックや本文で記載する（`/watch add 7203 --tag holding` のように本文中で説明形式は混在させない）。

## News Discovery Skill (`/discover-stocks`)

ニュースや分析記事から有望銘柄を抽出するスキル。Playwright MCPでサイトを巡回し、銘柄コード・推奨理由を抽出、裏付け情報収集とリスク分析を経てレポートを生成する。

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
- `general_news`: 一般ニュース（Google News RSS、銘柄名+コードで検索）
- `ir_release`: TDnet 適時開示（yanoshin Atom）
- `stock_news`: 四季報銘柄ページの「この銘柄の関連記事」（CDP経由、ログイン不要）

**認証方式:**
- `auth: cdp` — Chrome DevTools Protocol経由（要: `open -a 'Google Chrome' --args --remote-debugging-port=9222`）
- `auth: none` — Playwright MCPで直接アクセス

## Stock News Research Skill (`/research-stock-news`)

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

## Stock Analysis Skill (`/analyze-stock`)

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

# 経営陣評価セクションを既存レポートに追加
/analyze-stock 7203 --with-executive-research
```

**構成ファイル:**
- `.claude/skills/analyze-stock/SKILL.md`: スキル定義
- `config/news_sources.yaml`: `stock_news` / `disclosure` カテゴリの銘柄ページ設定
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
1. 企業概要
2. 事業構造・セグメント分析（セグメント別売上・利益構成、成長性・競争力、CAGR）
3. 財務分析（PER/PBR/ROE等、財務状況、キャッシュフロー、ネットキャッシュ分析、業績推移）
4. テクニカル分析（統合スコア/Minervini/RSP、株価チャートPNG）
5. 業界・競合分析（業界動向、四季報ライバル比較テーブル、SWOT分析）
6. 直近の適時開示・ニュース（`/research-stock-news`相当の情報を自動統合）
7. リスク要因
8. 投資判断サマリー（5段階評価、セグメント分析・成長性を含む判断根拠）

**チャート生成依存:** `kaleido`（オプショナル）。未インストール時はチャート生成をスキップし、テキストのみのレポートを生成する。

## Executive Research Skill (`/research-executives`)

特定銘柄の法定役員（取締役・監査役・執行役）について、外部発信を Claude LLM で6軸スコアリング（ビジョン一貫性・実行力・市場認識・リスク開示誠実性・コミュニケーション能力・**成長志向**）し、独立した `executive_report.md` を生成する。バックエンドは `src/market_pipeline/executives/`（詳細は `docs/core/api-reference.md` 参照）。

```bash
/research-executives 7203
/research-executives 7203 9984
/research-executives 7203 --include-directors
/research-executives 6758 --include-executive-officers
/research-executives 7203 --persons "佐藤恒治,豊田章男"
/research-executives 7203 --force-refresh

# 期間指定（既定: 過去3年対象／直近1年ハイライト）
python scripts/run_research_executives.py build-report 7203 --lookback-days 1095 --highlight-days 365
```

**構成ファイル:**
- `.claude/skills/research-executives/SKILL.md`: スキル定義
- `scripts/run_research_executives.py`: CLIエントリ（list-executives / build-report サブコマンド）
- 出力先: `output/reports/stocks/YYYYMMDD-HHMM-{code}-analysis/executive_report.md`

**レポート構成（4セクション）:**
1. 役員サマリー（表形式、総合スコア＋軸ハイライト）
2. 役員別評価（EDINET XBRL 略歴＋6軸スコア＋各軸rationale）
3. タイムライン（発信日降順、対象は過去3年、直近1年は🆕＋太字でハイライト、発信日不明は末尾に `—` でまとめ表示）
4. 主要発信引用集（直近1年を優先、不足時は1〜3年の新しい順にフォールバック、各役員最大5件）

**期間・キーワード定数:** `src/market_pipeline/executives/__init__.py` に `LOOKBACK_DAYS_TOTAL=1095`（過去3年）、`HIGHLIGHT_DAYS_RECENT=365`（直近1年）。検索キーワードは `SEARCH_KEYWORDS`（10語: インタビュー／講演／対談／コラム／ブログ／記事／寄稿／note／メッセージ／登壇）。

**スコープ（Phase 0 PoC で確定）:**
- 対象は **法定役員のみ**（取締役・監査役・執行役）
- 執行役員（社内職位）専任者は XBRL 構造化データに含まれないため対象外
- 取締役兼任の執行役員は役職文字列に兼任情報が含まれるため自然に取得される

**月次バッチ:** `scripts/run_executive_master_update.py`（`--codes` / `--limit` / `--dry-run` オプション）。`documents.json` の `docID` を `executives.edinet_source_doc_id` と比較し、一致すれば DL・パース・upsert を全てスキップ（`status=unchanged`、Slack メトリクス「スキップ（有報未更新）」に集計）。

**環境変数:** `.env` に `EDINET_API_KEY` を設定（`.env.example` 参照）

## Watchlist Skill (`/watch`)

`scripts/watchlist.py` を呼ぶラッパー。サブコマンド: `add` / `list` / `update` / `remove`。

```bash
python scripts/watchlist.py add 7203 --tag holding --priority high --note "押し目検討"
python scripts/watchlist.py list
python scripts/watchlist.py list --tag holding
python scripts/watchlist.py update 7203 --priority mid
python scripts/watchlist.py remove 7203
```

ウォッチリスト（`data/watchlists/*.json`）はニュース配信モジュール（`src/market_pipeline/news_delivery/`、`docs/core/api-reference.md` 参照）が購読する。

## Notion Export Skill (`/sync-notion`)

`/analyze-stock` が生成する `output/reports/stocks/YYYYMMDD-HHMM-{code}-analysis/` 配下のレポート(`base_report.md` + `deep_research_report.md` + `chart.png`)を Notion 親ページ配下に 1 銘柄=1 ページの階層型構造で投入する。CLI・終了コード・前提条件の詳細は `docs/core/api-reference.md` の `sync_notion.py` / Notion 投入モジュールを参照。

```bash
python scripts/sync_notion.py 7804                # 最新タイムスタンプのディレクトリを自動選択
python scripts/sync_notion.py 7804 --dry-run      # API を呼ばず blocks JSON を標準出力
python scripts/sync_notion.py --report-dir output/reports/stocks/20260413-2244-7804-analysis
python scripts/sync_notion.py 7804 --skip-deep-research
```

**前提条件:**
- `.env` に `NOTION_PARENT_PAGE_ID`（投入先の親ページID）と `NOTION_API_TOKEN`（Internal Integration Token）を設定
- 親ページの「Connections」から Integration に編集権限を付与
- **セキュリティ注意:** 投資分析レポートは個人運用前提のため、Notion 親ページの共有設定は **ワークスペース内限定** を推奨（Web 公開・外部ゲスト招待を避ける）

**特徴:**
- Markdown → Notion blocks 変換（インライン bold/italic/code/strikethrough/link 対応、表セル内も保持）
- 2000 文字超の段落と 100 blocks 超のページ投入を自動分割
- 同銘柄の再投入時は既存ページを自動アーカイブ。3 件以上見つかった場合は安全のため停止
- `chart.png` は Notion File Upload API でアップロードして image block に埋め込む
- Deep Research セクションは `toggle` block 内にネスト
