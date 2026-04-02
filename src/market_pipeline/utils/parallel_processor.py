"""
Parallel processing utility for efficient stock analysis computations.
Provides reusable framework for parallel execution of stock-level calculations.
"""

import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import List, Callable, Any, Dict, Optional, Tuple, Union, cast
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import sqlite3


logger = logging.getLogger(__name__)


class ParallelProcessor:
    """
    A utility class for parallel processing of stock data.
    Supports both process-based and thread-based parallelism.
    """

    def __init__(
        self,
        n_workers: Optional[int] = None,
        use_threads: bool = False,
        batch_size: int = 100,
        show_progress: bool = True,
    ):
        """
        Initialize parallel processor.

        Args:
            n_workers: Number of workers. If None, uses CPU count.
            use_threads: Use ThreadPoolExecutor instead of ProcessPoolExecutor.
            batch_size: Size of batches for processing.
            show_progress: Show progress bar during processing.
        """
        self.n_workers = n_workers or mp.cpu_count()
        self.use_threads = use_threads
        self.batch_size = batch_size
        self.show_progress = show_progress

    def process_stocks_batch(
        self, stock_codes: List[str], process_func: Callable, *args, **kwargs
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        Process a list of stock codes in parallel batches.

        Args:
            stock_codes: List of stock codes to process.
            process_func: Function to apply to each stock.
            *args: Additional arguments for process_func.
            **kwargs: Additional keyword arguments for process_func.

        Returns:
            Dictionary mapping stock codes to results.
        """
        results = {}
        errors = {}

        # Split stocks into batches
        batches = [
            stock_codes[i : i + self.batch_size]
            for i in range(0, len(stock_codes), self.batch_size)
        ]

        # Choose executor type
        Executor = ThreadPoolExecutor if self.use_threads else ProcessPoolExecutor

        with Executor(max_workers=self.n_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(
                    self._process_batch, batch, process_func, *args, **kwargs
                ): batch
                for batch in batches
            }

            # Process completed futures
            iterator = as_completed(future_to_batch)
            if self.show_progress:
                iterator = tqdm(iterator, total=len(batches), desc="Processing batches")

            for future in iterator:
                batch = future_to_batch[future]
                try:
                    batch_results, batch_errors = future.result()
                    results.update(batch_results)
                    errors.update(batch_errors)
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
                    for code in batch:
                        errors[code] = str(e)

        if errors:
            logger.warning(f"Processing completed with {len(errors)} errors")

        return results, errors

    def _process_batch(
        self, batch: List[str], process_func: Callable, *args, **kwargs
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        Process a single batch of stocks.

        Args:
            batch: List of stock codes in the batch.
            process_func: Function to apply to each stock.
            *args: Additional arguments for process_func.
            **kwargs: Additional keyword arguments for process_func.

        Returns:
            Tuple of (results dict, errors dict).
        """
        results = {}
        errors = {}

        for code in batch:
            try:
                result = process_func(code, *args, **kwargs)
                results[code] = result
            except Exception as e:
                logger.error(f"Error processing stock {code}: {e}")
                errors[code] = str(e)

        return results, errors

    @staticmethod
    def parallelize_dataframe(
        df: pd.DataFrame, func: Callable, n_workers: Optional[int] = None, axis: int = 0
    ) -> pd.DataFrame:
        """
        Parallelize operations on a pandas DataFrame.

        Args:
            df: DataFrame to process.
            func: Function to apply to DataFrame chunks.
            n_workers: Number of workers.
            axis: Axis to split on (0 for rows, 1 for columns).

        Returns:
            Processed DataFrame.
        """
        n_workers = n_workers or mp.cpu_count()

        if axis == 0:
            # Split by rows
            df_chunks = np.array_split(df, n_workers)
        else:
            # Split by columns
            df_chunks = [df.iloc[:, i::n_workers] for i in range(n_workers)]

        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            results: List[pd.DataFrame] = list(executor.map(func, df_chunks))

        if axis == 0:
            return cast(pd.DataFrame, pd.concat(results, axis=0, ignore_index=True))
        else:
            return cast(pd.DataFrame, pd.concat(results, axis=1))


class BatchDatabaseProcessor:
    """
    Utility for efficient batch database operations.
    """

    def __init__(self, db_path: str, batch_size: int = 1000):
        """
        Initialize batch database processor.

        Args:
            db_path: Path to SQLite database.
            batch_size: Size of batches for database operations.
        """
        self.db_path = db_path
        self.batch_size = batch_size

    def batch_insert(
        self, table_name: str, data: List[Dict[str, Any]], on_conflict: str = "REPLACE"
    ) -> int:
        """
        Perform batch insert into database.

        Args:
            table_name: Name of the table.
            data: List of dictionaries containing row data.
            on_conflict: SQL clause for handling conflicts.

        Returns:
            Number of rows inserted.
        """
        if not data:
            return 0

        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("BEGIN TRANSACTION")

        try:
            # Get column names from first record
            columns = list(data[0].keys())
            placeholders = ",".join(["?" for _ in columns])
            columns_str = ",".join(columns)

            query = f"INSERT OR {on_conflict} INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # Process in batches
            total_inserted = 0
            for i in range(0, len(data), self.batch_size):
                batch = data[i : i + self.batch_size]
                values = [tuple(row[col] for col in columns) for row in batch]
                conn.executemany(query, values)
                total_inserted += len(batch)

            conn.commit()
            return total_inserted

        except Exception as e:
            conn.rollback()
            logger.error(f"Error in batch insert: {e}")
            raise
        finally:
            conn.close()

    def batch_fetch(
        self, query: str, params: Optional[List[Any]] = None, as_dataframe: bool = True
    ) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Fetch data from database efficiently.

        Args:
            query: SQL query to execute.
            params: Query parameters.
            as_dataframe: Return as DataFrame instead of list.

        Returns:
            Query results as DataFrame or list of tuples.
        """
        conn = sqlite3.connect(self.db_path)

        try:
            if as_dataframe:
                return pd.read_sql_query(query, conn, params=params)
            else:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
        finally:
            conn.close()

    def create_indexes(self, index_definitions: List[Dict[str, Any]]) -> None:
        """
        Create database indexes for performance.

        Args:
            index_definitions: List of index definitions with keys:
                - name: Index name
                - table: Table name
                - columns: List of column names
                - unique: Whether index should be unique
        """
        conn = sqlite3.connect(self.db_path)

        try:
            for idx_def in index_definitions:
                unique = "UNIQUE" if idx_def.get("unique", False) else ""
                columns = ",".join(idx_def["columns"])
                query = f"CREATE {unique} INDEX IF NOT EXISTS {idx_def['name']} ON {idx_def['table']} ({columns})"
                conn.execute(query)
                logger.info(f"Created index {idx_def['name']} on {idx_def['table']}")

            conn.commit()
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            raise
        finally:
            conn.close()


def measure_performance(func: Callable) -> Callable:
    """
    Decorator to measure function execution time.

    Args:
        func: Function to measure.

    Returns:
        Wrapped function that logs execution time.
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"{func.__name__} completed in {duration:.2f} seconds")
        return result

    return wrapper
