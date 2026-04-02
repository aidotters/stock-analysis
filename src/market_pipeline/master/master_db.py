"""
株式銘柄マスターデータベース管理モジュール

東証上場銘柄のマスター情報を管理し、各データソース間の共通参照テーブルを提供します。
"""

import os
import sqlite3
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from typing import Optional, Dict, Any
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StockMasterDB:
    def __init__(self, db_path: Optional[str] = None):
        """
        銘柄マスターデータベースの初期化

        Args:
            db_path (str): データベースファイルのパス。指定しない場合は設定のmaster_dbパス
        """
        if db_path is None:
            from market_pipeline.config import get_settings

            settings = get_settings()
            os.makedirs(str(settings.paths.data_dir), exist_ok=True)
            db_path = str(settings.paths.master_db)

        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """データベースとテーブルを初期化"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stocks_master (
                    code TEXT PRIMARY KEY,              -- 銘柄コード（4桁）
                    name TEXT NOT NULL,                 -- 銘柄名
                    sector TEXT,                        -- 業種（33業種区分）
                    market TEXT,                        -- 上場市場
                    market_product_category TEXT,       -- 市場・商品区分
                    yfinance_symbol TEXT,               -- yfinance用シンボル（code + ".T"）
                    jquants_code TEXT,                  -- J-Quants用コード
                    is_active BOOLEAN DEFAULT 1,        -- 上場廃止フラグ
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # インデックスの作成
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_sector ON stocks_master(sector);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_market ON stocks_master(market);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_active ON stocks_master(is_active);"
            )

            conn.commit()

        logger.info(f"Master database initialized at {self.db_path}")

    def download_tse_listed_stocks(self) -> Optional[str]:
        """
        東証上場銘柄一覧のExcelファイルをダウンロード

        Returns:
            str: ダウンロードしたファイルのパス、失敗時はNone
        """
        url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            excel_link = soup.find(
                "a",
                href=lambda href: bool(href and (".xls" in href or ".xlsx" in href)),
            )

            if isinstance(excel_link, Tag):
                excel_href = excel_link.get("href")
                if not excel_href:
                    logger.error("Excel link found but href is empty")
                    return None
                excel_url = urljoin(url, str(excel_href))
                excel_response = requests.get(excel_url)
                excel_response.raise_for_status()

                file_extension = ".xlsx" if excel_url.endswith(".xlsx") else ".xls"
                temp_dir = os.path.join(os.path.dirname(self.db_path), "temp")
                os.makedirs(temp_dir, exist_ok=True)
                file_path = os.path.join(temp_dir, f"tse_listed_stocks{file_extension}")

                with open(file_path, "wb") as f:
                    f.write(excel_response.content)

                logger.info(f"TSE listed stocks downloaded to {file_path}")
                return file_path
            else:
                logger.error("Could not find Excel file link")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading TSE listed stocks: {e}")
            return None

    def load_tse_stocks_from_excel(self, file_path: str) -> pd.DataFrame:
        """
        ExcelファイルからTSE上場銘柄データを読み込み

        Args:
            file_path (str): Excelファイルのパス

        Returns:
            pd.DataFrame: 銘柄データ
        """
        try:
            df = pd.read_excel(file_path, header=0)

            # ETFを除外
            df = df[~df["市場・商品区分"].str.contains("ETF")]

            # 必要な列のみ選択・リネーム
            df = df[["コード", "銘柄名", "市場・商品区分", "33業種区分"]].copy()
            df.rename(
                columns={
                    "コード": "code",
                    "銘柄名": "name",
                    "市場・商品区分": "market_product_category",
                    "33業種区分": "sector",
                },
                inplace=True,
            )

            # データ型変換
            df["code"] = df["code"].astype(str)

            # 上場市場の抽出（市場・商品区分から）
            df["market"] = df["market_product_category"].str.extract(
                r"(プライム|スタンダード|グロース)"
            )
            df["market"] = df["market"].fillna("その他")

            # 各種シンボルの生成
            df["yfinance_symbol"] = df["code"] + ".T"
            df["jquants_code"] = df["code"] + "0"
            df["is_active"] = True

            logger.info(f"Loaded {len(df)} stocks from Excel file")
            return df

        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            return pd.DataFrame()

    def update_master_data(self) -> bool:
        """
        マスターデータを最新の東証上場銘柄情報で更新

        Returns:
            bool: 更新成功時True
        """
        try:
            # 東証上場銘柄リストをダウンロード
            file_path = self.download_tse_listed_stocks()
            if not file_path:
                return False

            # Excelファイルからデータを読み込み
            df = self.load_tse_stocks_from_excel(file_path)
            if df.empty:
                return False

            # データベースに保存
            with sqlite3.connect(self.db_path) as conn:
                current_time = datetime.now().isoformat()

                # 既存データを非アクティブに設定
                conn.execute(
                    "UPDATE stocks_master SET is_active = 0, updated_at = ?",
                    (current_time,),
                )

                # 新しいデータを挿入または更新
                for _, row in df.iterrows():
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO stocks_master (
                            code, name, sector, market, market_product_category,
                            yfinance_symbol, jquants_code, is_active, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            row["code"],
                            row["name"],
                            row["sector"],
                            row["market"],
                            row["market_product_category"],
                            row["yfinance_symbol"],
                            row["jquants_code"],
                            True,
                            current_time,
                        ),
                    )

                conn.commit()

            # 一時ファイルを削除
            if os.path.exists(file_path):
                os.remove(file_path)

            logger.info(f"Master data updated successfully with {len(df)} stocks")
            return True

        except Exception as e:
            logger.error(f"Error updating master data: {e}")
            return False

    def get_all_stocks(self, active_only: bool = True) -> pd.DataFrame:
        """
        全銘柄情報を取得

        Args:
            active_only (bool): アクティブな銘柄のみ取得するか

        Returns:
            pd.DataFrame: 銘柄情報
        """
        query = "SELECT * FROM stocks_master"
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY code"

        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn)

    def get_stock_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """
        銘柄コードから銘柄情報を取得

        Args:
            code (str): 銘柄コード

        Returns:
            Dict[str, Any]: 銘柄情報、存在しない場合はNone
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stocks_master WHERE code = ?", (code,))
            row = cursor.fetchone()

            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None

    def get_stocks_by_sector(
        self, sector: str, active_only: bool = True
    ) -> pd.DataFrame:
        """
        業種で銘柄を取得

        Args:
            sector (str): 業種名
            active_only (bool): アクティブな銘柄のみ取得するか

        Returns:
            pd.DataFrame: 該当銘柄情報
        """
        query = "SELECT * FROM stocks_master WHERE sector = ?"
        params = [sector]

        if active_only:
            query += " AND is_active = 1"

        query += " ORDER BY code"

        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=tuple(params))

    def get_stocks_by_market(
        self, market: str, active_only: bool = True
    ) -> pd.DataFrame:
        """
        市場で銘柄を取得

        Args:
            market (str): 市場名（プライム、スタンダード、グロース等）
            active_only (bool): アクティブな銘柄のみ取得するか

        Returns:
            pd.DataFrame: 該当銘柄情報
        """
        query = "SELECT * FROM stocks_master WHERE market = ?"
        params = [market]

        if active_only:
            query += " AND is_active = 1"

        query += " ORDER BY code"

        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=tuple(params))

    def get_statistics(self) -> Dict[str, Any]:
        """
        マスターデータの統計情報を取得

        Returns:
            Dict[str, Any]: 統計情報
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 全銘柄数
            cursor.execute("SELECT COUNT(*) FROM stocks_master")
            total_count = cursor.fetchone()[0]

            # アクティブ銘柄数
            cursor.execute("SELECT COUNT(*) FROM stocks_master WHERE is_active = 1")
            active_count = cursor.fetchone()[0]

            # 業種別集計
            cursor.execute("""
                SELECT sector, COUNT(*) as count 
                FROM stocks_master 
                WHERE is_active = 1 
                GROUP BY sector 
                ORDER BY count DESC
            """)
            sector_stats = cursor.fetchall()

            # 市場別集計
            cursor.execute("""
                SELECT market, COUNT(*) as count 
                FROM stocks_master 
                WHERE is_active = 1 
                GROUP BY market 
                ORDER BY count DESC
            """)
            market_stats = cursor.fetchall()

            # 最終更新日
            cursor.execute("SELECT MAX(updated_at) FROM stocks_master")
            last_updated = cursor.fetchone()[0]

            return {
                "total_stocks": total_count,
                "active_stocks": active_count,
                "inactive_stocks": total_count - active_count,
                "sector_distribution": dict(sector_stats),
                "market_distribution": dict(market_stats),
                "last_updated": last_updated,
            }


def main():
    """メイン処理：マスターデータベースの作成・更新"""
    try:
        master_db = StockMasterDB()

        print("銘柄マスターデータベースを更新しています...")
        if master_db.update_master_data():
            print("マスターデータの更新が完了しました。")

            # 統計情報の表示
            stats = master_db.get_statistics()
            print("\n=== 統計情報 ===")
            print(f"総銘柄数: {stats['total_stocks']}")
            print(f"アクティブ銘柄数: {stats['active_stocks']}")
            print(f"非アクティブ銘柄数: {stats['inactive_stocks']}")
            print(f"最終更新: {stats['last_updated']}")

            print("\n=== 市場別分布 ===")
            for market, count in stats["market_distribution"].items():
                print(f"{market}: {count}銘柄")

            print("\n=== 業種別分布（上位10業種） ===")
            sorted_sectors = sorted(
                stats["sector_distribution"].items(), key=lambda x: x[1], reverse=True
            )
            for sector, count in sorted_sectors[:10]:
                print(f"{sector}: {count}銘柄")

            # サンプルデータの表示
            print("\n=== サンプルデータ ===")
            sample_stocks = master_db.get_all_stocks().head()
            print(
                sample_stocks[["code", "name", "sector", "market"]].to_string(
                    index=False
                )
            )

        else:
            print("マスターデータの更新に失敗しました。")

    except Exception as e:
        print(f"エラーが発生しました: {e}")


if __name__ == "__main__":
    main()
