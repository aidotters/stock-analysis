# 開発ガイドライン

このドキュメントは、既存コードベースから抽出された開発パターンと規約をまとめたものです。

## コーディング規約

### 言語・バージョン

- **Python 3.10+**
- 型ヒント使用推奨

### フォーマット・リント

```bash
# コードフォーマット
black .

# リント
ruff check .

# 型チェック
mypy .
```

### 設定ファイル

`pyproject.toml`で以下を設定:

```toml
[tool.black]
line-length = 88

[tool.ruff]
line-length = 88

[tool.mypy]
python_version = "3.10"
```

## プロジェクト構造

詳細なディレクトリツリーとファイル説明は [repo-structure.md](repo-structure.md) を参照。

## 設定管理パターン

### Pydantic Settingsの使用

```python
from market_pipeline.config import get_settings

settings = get_settings()

# パス設定
db_path = settings.paths.jquants_db

# API設定
max_requests = settings.jquants.max_concurrent_requests

# 分析設定
sma_period = settings.analysis.sma_short
```

### 環境変数

`.env`ファイルで環境変数を設定:

```bash
# .envファイル
EMAIL=your_email@example.com
PASSWORD=your_password
JQUANTS_MAX_CONCURRENT_REQUESTS=3
```

## データベース操作パターン

### コンテキストマネージャーの使用

```python
import sqlite3
from contextlib import contextmanager

@contextmanager
def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    try:
        # PRAGMA設定
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        yield conn
    finally:
        conn.close()

# 使用例
with get_connection(settings.paths.jquants_db) as conn:
    df = pd.read_sql_query("SELECT * FROM daily_quotes", conn)
```

### バッチ挿入

```python
# 非効率: 1件ずつ挿入
for record in records:
    cursor.execute("INSERT INTO table VALUES (?, ?)", record)
    conn.commit()

# 効率的: バッチ挿入
cursor.executemany("INSERT INTO table VALUES (?, ?)", records)
conn.commit()
```

### インデックス

```python
# scripts/create_database_indexes.py で作成
CREATE INDEX idx_daily_quotes_code ON daily_quotes (Code);
CREATE INDEX idx_daily_quotes_date ON daily_quotes (Date);
```

## 並列処理パターン

### ProcessPoolExecutor

```python
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

def process_stock(code):
    # 銘柄ごとの処理
    return result

def process_all_stocks(stock_codes):
    n_workers = multiprocessing.cpu_count()

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        results = list(executor.map(process_stock, stock_codes))

    return results
```

### parallel_processor.pyの使用

```python
from market_pipeline.utils.parallel_processor import ParallelProcessor

processor = ParallelProcessor(n_workers=8)
results = processor.process(stock_codes, analyze_stock, batch_size=100)
```

## 非同期処理パターン

### aiohttp + asyncio

```python
import asyncio
import aiohttp

class AsyncDataFetcher:
    def __init__(self, max_concurrent=3):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_single(self, session, url):
        async with self.semaphore:
            async with session.get(url) as response:
                return await response.json()

    async def fetch_all(self, urls):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_single(session, url) for url in urls]
            return await asyncio.gather(*tasks)
```

## ベクトル化計算パターン

### Pandas/NumPyの活用

```python
import pandas as pd
import numpy as np

# 非効率: ループ処理
def calculate_returns_slow(prices):
    returns = []
    for i in range(1, len(prices)):
        ret = (prices[i] - prices[i-1]) / prices[i-1]
        returns.append(ret)
    return returns

# 効率的: ベクトル化
def calculate_returns_fast(prices):
    return prices.pct_change().dropna()

# 移動平均
def calculate_sma(prices, window):
    return prices.rolling(window=window).mean()
```

## テストパターン

### pytest使用

```python
# tests/conftest.py
import pytest
import sqlite3
import tempfile

@pytest.fixture
def mock_database():
    """テスト用インメモリデータベース"""
    conn = sqlite3.connect(":memory:")
    # テーブル作成
    conn.execute("""
        CREATE TABLE daily_quotes (
            Code TEXT,
            Date TEXT,
            Close REAL
        )
    """)
    yield conn
    conn.close()

@pytest.fixture
def sample_stock_codes():
    return ['1001', '1002', '1003']
```

### テスト実行

```bash
# 全テスト実行
pytest

# 特定ファイル
pytest tests/test_minervini.py

# 詳細出力
pytest -v

# カバレッジ付き
pytest --cov=backend
```

## エラーハンドリング

### 個別エラーの分離

```python
def process_stocks_with_error_handling(stock_codes):
    results = []
    errors = []

    for code in stock_codes:
        try:
            result = process_stock(code)
            results.append(result)
        except Exception as e:
            # 個別エラーで全体を止めない
            errors.append((code, str(e)))
            continue

    return results, errors
```

### ログ出力

```python
import logging

logger = logging.getLogger(__name__)

def process_with_logging():
    logger.info("処理開始")
    try:
        # 処理
        logger.info("処理完了")
    except Exception as e:
        logger.error(f"エラー発生: {e}")
        raise
```

## キャッシュパターン

### LRUキャッシュ

```python
from functools import lru_cache

@lru_cache(maxsize=1000)
def expensive_calculation(param):
    # 重い計算
    return result
```

### クラスレベルキャッシュ

```python
class ChartClassifier:
    _template_cache = {}  # クラス変数でキャッシュ共有

    def __init__(self, window):
        if window not in self._template_cache:
            self._template_cache[window] = self._create_templates(window)
        self.templates = self._template_cache[window]
```

## コミットメッセージ規約

Conventional Commits形式を推奨:

```
<type>(<scope>): <subject>

[optional body]

[optional footer]
```

**Type:**
- `feat`: 新機能
- `fix`: バグ修正
- `refactor`: リファクタリング
- `docs`: ドキュメント
- `test`: テスト
- `perf`: パフォーマンス改善
- `chore`: その他

**例:**
```
feat(analysis): Minervini分析にRSI条件を追加

新しいRSI条件を追加し、より精度の高いスクリーニングを実現

Refs: #123
```

## 依存関係管理

### uv使用（推奨）

```bash
# パッケージ追加
uv pip install pandas

# 依存関係ロック
uv pip compile pyproject.toml -o requirements.txt

# インストール
uv pip sync requirements.txt
```

### pyproject.toml

```toml
[project]
name = "stock-analysis"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "aiohttp>=3.8.0",
    # ...
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "black>=23.0.0",
    "ruff>=0.1.0",
    "mypy>=1.0.0",
]
```

## パフォーマンス最適化チェックリスト

- [ ] データベースインデックスを作成したか
- [ ] バッチ処理を使用しているか
- [ ] ベクトル化計算を使用しているか
- [ ] 並列処理が可能か検討したか
- [ ] 不要なAPI呼び出しをキャッシュしているか
- [ ] WALモードを有効にしているか

## 新機能追加時のチェックリスト

1. [ ] `backend/market_pipeline/` に適切なモジュールを作成
2. [ ] 設定が必要な場合は `backend/market_pipeline/config/settings.py` に追加
3. [ ] テストを `tests/` に作成
4. [ ] 必要に応じて `scripts/` に実行スクリプトを追加
5. [ ] `CLAUDE.md` を更新
6. [ ] `docs/core/api-reference.md` にAPI仕様を追加
7. [ ] `docs/core/repo-structure.md` にファイル構成を追加
8. [ ] リント・型チェックを実行
