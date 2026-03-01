import sqlite3
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List

from market_pipeline.analysis.high_low_ratio import (
    calc_hl_ratio_for_all,
    init_hl_ratio_db,
)
from market_pipeline.analysis.minervini import (
    update_minervini_db,
    update_type8_db,
    init_minervini_db,
)
from market_pipeline.analysis.relative_strength import (
    update_rsp_db,
    update_rsi_db,
    init_rsp_db,
)
from market_pipeline.analysis.integrated_analysis import create_analysis_summary
from market_pipeline.analysis.chart_classification import (
    main_full_run as run_chart_classification_full,
)
from market_pipeline.analysis.integrated_analysis2 import (
    main as run_integrated_analysis2,
)
from market_pipeline.utils.parallel_processor import measure_performance
from market_pipeline.utils.slack_notifier import JobContext


class DatabaseManager:
    """Database connection manager for analysis workflow."""

    def __init__(self, jquants_db_path: str, results_db_path: str):
        self.jquants_db_path = jquants_db_path
        self.results_db_path = results_db_path
        self.jquants_conn = None
        self.results_conn = None

    def __enter__(self):
        """Enter context manager and open connections."""
        self.jquants_conn = sqlite3.connect(self.jquants_db_path)
        self.results_conn = sqlite3.connect(self.results_db_path)
        # Enable optimizations
        self.jquants_conn.execute("PRAGMA journal_mode=WAL")
        self.jquants_conn.execute("PRAGMA synchronous=NORMAL")
        self.results_conn.execute("PRAGMA journal_mode=WAL")
        self.results_conn.execute("PRAGMA synchronous=NORMAL")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close connections."""
        if self.jquants_conn:
            self.jquants_conn.close()
        if self.results_conn:
            self.results_conn.close()


class DailyAnalysisConfig:
    """Configuration for daily analysis workflow."""

    def __init__(self):
        # Analysis periods
        self.rsp_period_days = 500  # Days to look back for RSP calculation (need 260+ business days for Minervini)
        self.update_window_days = 5  # Days to update in recent period
        self.hl_ratio_weeks = 52  # Weeks for high-low ratio calculation

        # Database paths - using project structure
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(project_root, "data")
        self.jquants_db_path = os.path.join(data_dir, "jquants.db")
        self.results_db_path = os.path.join(data_dir, "analysis_results.db")

        # Performance settings
        self.n_workers = None  # Auto-detect CPU count
        self.batch_size = 100  # Process 100 stocks per batch

    def setup_logger(self) -> logging.Logger:
        """Setup and return a logger instance."""
        # Create logs directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logs_dir = os.path.join(project_root, "logs")
        os.makedirs(logs_dir, exist_ok=True)

        # Setup logging
        log_filename = os.path.join(
            logs_dir, f"daily_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_filename, encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )

        logger = logging.getLogger(__name__)
        logger.info(f"Daily analysis logging initialized. Log file: {log_filename}")
        return logger

    def get_database_manager(self) -> DatabaseManager:
        """Get a database manager instance."""
        return DatabaseManager(self.jquants_db_path, self.results_db_path)


@measure_performance
def run_daily_analysis(
    target_date: Optional[str] = None, modules: Optional[List[str]] = None
) -> bool:
    """
    Run the optimized daily analysis workflow.

    Args:
        target_date: Optional target date in 'YYYY-MM-DD' format.
                    If None, uses the latest date from jquants.db
        modules: Optional list of modules to run. If None, runs all modules.
                Available modules: ['rsp', 'rsi', 'minervini', 'type8', 'hl_ratio', 'summary', 'chart_classification', 'integrated_scores']

    Returns:
        bool: True if all analysis steps completed successfully, False otherwise
    """
    analysis_config = DailyAnalysisConfig()
    logger = analysis_config.setup_logger()
    if modules is None:
        modules = [
            "rsp",
            "rsi",
            "minervini",
            "type8",
            "hl_ratio",
            "summary",
            "chart_classification",
            "integrated_scores",
        ]

    logger.info(f"Starting daily analysis workflow. Modules to run: {modules}")

    success = True
    calc_end_date_str = ""

    try:
        with JobContext("日次分析") as job:
            job.add_metric("実行モジュール", ", ".join(modules))

            with analysis_config.get_database_manager() as db_manager:
                # Get all stock codes from jquants.db
                cursor = db_manager.jquants_conn.cursor()
                cursor.execute("SELECT DISTINCT Code FROM daily_quotes")
                code_list = [row[0] for row in cursor.fetchall()]
                logger.info(f"Found {len(code_list)} stock codes for analysis.")

                # Define date range for updates
                if target_date:
                    # Use specified target date
                    try:
                        end_date = datetime.strptime(target_date, "%Y-%m-%d")
                        logger.info(f"Using specified target date: {target_date}")
                    except ValueError:
                        logger.error(
                            f"Invalid date format: {target_date}. Expected YYYY-MM-DD"
                        )
                        raise RuntimeError(
                            f"Invalid date format: {target_date}. Expected YYYY-MM-DD"
                        )
                else:
                    # Get latest date from jquants database
                    try:
                        cursor = db_manager.jquants_conn.cursor()
                        cursor.execute("SELECT MAX(Date) FROM daily_quotes")
                        latest_date_str = cursor.fetchone()[0]
                        if latest_date_str:
                            end_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
                            logger.info(
                                f"Using latest date from database: {latest_date_str}"
                            )
                        else:
                            logger.error("No data found in jquants database")
                            raise RuntimeError("No data found in jquants database")
                    except Exception as e:
                        logger.error(f"Error getting latest date from database: {e}")
                        raise RuntimeError(
                            f"Error getting latest date from database: {e}"
                        ) from e

                calc_end_date_str = end_date.strftime("%Y-%m-%d")
                calc_start_date_str = (
                    end_date - timedelta(days=analysis_config.rsp_period_days)
                ).strftime("%Y-%m-%d")

                # --- Analysis Steps ---

                # 1. Relative Strength Percentage (RSP) Update
                if "rsp" in modules:
                    logger.info("Running Relative Strength Percentage (RSP) update...")
                    try:
                        cursor = db_manager.results_conn.cursor()
                        cursor.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name='relative_strength'"
                        )
                        table_exists = cursor.fetchone() is not None

                        if not table_exists:
                            logger.info(
                                "relative_strength table not found. Initializing with full data using parallel processing..."
                            )
                            processed, errors = init_rsp_db(
                                db_path=analysis_config.jquants_db_path,
                                result_db_path=analysis_config.results_db_path,
                                n_workers=analysis_config.n_workers,
                            )
                            logger.info(
                                "relative_strength table initialization completed."
                            )
                        else:
                            logger.info(
                                "relative_strength table found. Updating recent data using parallel processing..."
                            )
                            processed, errors = update_rsp_db(
                                db_path=analysis_config.jquants_db_path,
                                result_db_path=analysis_config.results_db_path,
                                calc_start_date=calc_start_date_str,
                                calc_end_date=calc_end_date_str,
                                period=-analysis_config.update_window_days,
                                n_workers=analysis_config.n_workers,
                            )

                        if errors > 0:
                            logger.warning(
                                f"RSP update completed with {errors} errors. Processed {processed} stocks."
                            )
                            success = False
                        else:
                            logger.info(
                                f"RSP update completed successfully. Processed {processed} stocks."
                            )

                    except Exception as e:
                        logger.error(f"Error in RSP update: {e}", exc_info=True)
                        success = False

                # 2. Relative Strength Index (RSI) Update
                if "rsi" in modules:
                    logger.info("Running Relative Strength Index (RSI) update...")
                    try:
                        date_list_for_rsi = [
                            (end_date - timedelta(days=i)).strftime("%Y-%m-%d")
                            for i in range(analysis_config.update_window_days)
                        ]
                        errors = update_rsi_db(
                            result_db_path=analysis_config.results_db_path,
                            date_list=date_list_for_rsi,
                            period=-analysis_config.update_window_days,
                        )

                        if errors > 0:
                            logger.warning(
                                f"RSI update completed with {errors} errors."
                            )
                            success = False
                        else:
                            logger.info("RSI update completed successfully.")

                    except Exception as e:
                        logger.error(f"Error in RSI update: {e}", exc_info=True)
                        success = False

                # 3. Minervini Analysis Update
                if "minervini" in modules:
                    logger.info("Running Minervini analysis update...")
                    try:
                        cursor = db_manager.results_conn.cursor()
                        cursor.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name='minervini'"
                        )
                        table_exists = cursor.fetchone() is not None

                        if not table_exists:
                            logger.info(
                                "'minervini' table not found. Initializing with full data using parallel processing..."
                            )
                            init_minervini_db(
                                db_manager.jquants_conn,
                                db_manager.results_conn,
                                code_list,
                                n_workers=analysis_config.n_workers,
                            )
                            logger.info("'minervini' table initialization completed.")
                        else:
                            logger.info(
                                "'minervini' table found. Updating recent data using parallel processing..."
                            )
                            update_minervini_db(
                                db_manager.jquants_conn,  # source_conn
                                db_manager.results_conn,  # dest_conn
                                code_list,
                                calc_start_date_str,
                                calc_end_date_str,
                                period=1,  # Process only the latest date
                            )
                        logger.info("Minervini analysis update completed successfully.")
                    except Exception as e:
                        logger.error(f"Error in Minervini update: {e}", exc_info=True)
                        success = False

                # 4. Minervini Type 8 Update
                if "type8" in modules:
                    logger.info("Running Minervini Type 8 update...")
                    try:
                        # Check how many stocks have relative strength data for the target date
                        cursor = db_manager.results_conn.cursor()
                        cursor.execute(
                            "SELECT COUNT(*) FROM relative_strength WHERE Date = ? OR Date = ? OR Date LIKE ?",
                            (
                                calc_end_date_str,
                                calc_end_date_str.split()[0]
                                if " " in calc_end_date_str
                                else calc_end_date_str,
                                calc_end_date_str + "%",
                            ),
                        )
                        stock_count = cursor.fetchone()[0]
                        logger.info(
                            f"Found {stock_count} stocks with relative strength data for {calc_end_date_str}"
                        )

                        update_type8_db(
                            db_manager.results_conn,
                            [calc_end_date_str],  # Only update for the latest date
                            period=-1,
                        )
                        logger.info("Minervini Type 8 update completed successfully.")
                    except Exception as e:
                        logger.error(
                            f"Error in Minervini Type 8 update: {e}", exc_info=True
                        )
                        success = False

                # 5. High-Low Ratio Calculation
                if "hl_ratio" in modules:
                    logger.info("Running High-Low Ratio calculation...")
                    try:
                        # Ensure hl_ratio table exists with indexes
                        cursor = db_manager.results_conn.cursor()
                        cursor.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name='hl_ratio'"
                        )
                        table_exists = cursor.fetchone() is not None
                        if not table_exists:
                            logger.info(
                                "'hl_ratio' table not found. Initializing with indexes..."
                            )
                            init_hl_ratio_db(db_path=analysis_config.results_db_path)
                            logger.info("'hl_ratio' table initialized.")

                        # Calculate HL Ratio using optimized function
                        result_df = calc_hl_ratio_for_all(
                            db_path=analysis_config.jquants_db_path,
                            end_date=calc_end_date_str,
                            weeks=analysis_config.hl_ratio_weeks,
                            n_workers=analysis_config.n_workers,
                        )

                        if result_df is not None and not result_df.empty:
                            logger.info(
                                f"High-Low Ratio calculation completed for {len(result_df)} stocks."
                            )
                        else:
                            logger.warning(
                                "High-Low Ratio calculation returned no results."
                            )
                            success = False
                    except Exception as e:
                        logger.error(
                            f"Error in High-Low Ratio calculation: {e}", exc_info=True
                        )
                        success = False

                # 6. Create Analysis Summary
                if "summary" in modules:
                    logger.info("Creating daily analysis summary...")
                    try:
                        summary = create_analysis_summary(
                            date=calc_end_date_str,
                            db_path=analysis_config.results_db_path,
                        )
                        if summary:
                            logger.info(
                                f"Daily Analysis Summary for {calc_end_date_str}:"
                            )
                            for key, value in summary.items():
                                logger.info(f"  {key}: {value}")
                        else:
                            logger.warning(
                                f"No summary data generated for {calc_end_date_str}."
                            )
                            success = False
                        logger.info("Analysis summary creation completed.")
                    except Exception as e:
                        logger.error(
                            f"Error creating analysis summary: {e}", exc_info=True
                        )
                        success = False

                # 7. Chart Classification
                if "chart_classification" in modules:
                    logger.info("Running chart classification...")
                    try:
                        run_chart_classification_full()
                        logger.info("Chart classification completed.")
                    except Exception as e:
                        logger.error(
                            f"Error in chart classification: {e}", exc_info=True
                        )
                        success = False

                # 8. Integrated Scores (save to DB)
                if "integrated_scores" in modules:
                    logger.info(
                        "Running integrated scores analysis and saving to DB..."
                    )
                    try:
                        run_integrated_analysis2(target_date=calc_end_date_str)
                        logger.info("Integrated scores saved to database successfully.")
                    except Exception as e:
                        logger.error(f"Error in integrated scores: {e}", exc_info=True)
                        success = False

            status_msg = (
                "Daily analysis workflow finished successfully."
                if success
                else "Daily analysis workflow completed with errors."
            )
            logger.info(status_msg)

            if calc_end_date_str:
                job.add_metric("対象日", calc_end_date_str)
            if not success:
                job.add_warning("一部モジュールでエラーが発生しました")

    except Exception as e:
        logger.error(
            f"An error occurred during daily analysis workflow: {e}", exc_info=True
        )
        success = False

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run optimized daily stock analysis")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYY-MM-DD format (default: latest from database)",
    )
    parser.add_argument(
        "--modules",
        type=str,
        nargs="+",
        choices=[
            "rsp",
            "rsi",
            "minervini",
            "type8",
            "hl_ratio",
            "summary",
            "chart_classification",
            "integrated_scores",
        ],
        help="Analysis modules to run (default: all modules)",
    )
    args = parser.parse_args()

    run_daily_analysis(target_date=args.date, modules=args.modules)
