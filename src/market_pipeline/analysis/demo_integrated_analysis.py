#!/usr/bin/env python3
"""
Demonstration script for the integrated analysis functionality.
This script shows how to use the new database integration to perform
comprehensive stock analysis across all indicators.
"""

import sys
import os

# Add the analysis directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from integrated_analysis import (
    get_comprehensive_analysis,
    get_multi_date_analysis,
    get_top_stocks_by_criteria,
    get_stocks_meeting_criteria,
    create_analysis_summary,
    check_database_coverage,
    setup_logging,
)


def demo_database_coverage():
    """Demonstrate database coverage checking"""
    print("=== Database Coverage Analysis ===")
    try:
        coverage = check_database_coverage()

        print(f"HL Ratio Records: {coverage.get('hl_ratio_records', 0)}")
        print(f"Minervini Records: {coverage.get('minervini_records', 0)}")
        print(
            f"Relative Strength Records: {coverage.get('relative_strength_records', 0)}"
        )
        print(
            f"Unique Dates - HL: {coverage.get('hl_ratio_dates', 0)}, Minervini: {coverage.get('minervini_dates', 0)}, RS: {coverage.get('relative_strength_dates', 0)}"
        )
        print(
            f"Unique Codes - HL: {coverage.get('hl_ratio_codes', 0)}, Minervini: {coverage.get('minervini_codes', 0)}, RS: {coverage.get('relative_strength_codes', 0)}"
        )

    except Exception as e:
        print(f"Error checking coverage: {e}")
        print("This is expected if the database doesn't exist yet.")


def demo_comprehensive_analysis(sample_date="2023-12-01"):
    """Demonstrate comprehensive analysis for a specific date"""
    print(f"\n=== Comprehensive Analysis for {sample_date} ===")
    try:
        # Get comprehensive analysis for all stocks
        df = get_comprehensive_analysis(sample_date)

        if df.empty:
            print(f"No data found for {sample_date}")
            return

        print(f"Found data for {len(df)} stocks")
        print("\nTop 5 stocks by composite score:")
        top_columns = [
            "code",
            "hl_ratio",
            "relative_strength_index",
            "minervini_score",
            "composite_score",
        ]
        if all(col in df.columns for col in top_columns):
            print(df[top_columns].head().to_string(index=False))
        else:
            print("Some expected columns are missing from the analysis")
            print(f"Available columns: {list(df.columns)}")

        # Demonstrate single stock analysis
        if len(df) > 0:
            sample_code = df["code"].iloc[0]
            print(f"\n=== Single Stock Analysis for {sample_code} ===")
            single_stock = get_comprehensive_analysis(sample_date, code=sample_code)
            if not single_stock.empty:
                print(single_stock[top_columns].to_string(index=False))

    except Exception as e:
        print(f"Error in comprehensive analysis: {e}")


def demo_top_stocks_ranking(sample_date="2023-12-01"):
    """Demonstrate different ranking methods"""
    print(f"\n=== Top Stocks Ranking for {sample_date} ===")

    criteria_list = ["hl_ratio", "rsi", "minervini", "composite"]

    for criteria in criteria_list:
        try:
            print(f"\n--- Top 3 stocks by {criteria.upper()} ---")
            top_stocks = get_top_stocks_by_criteria(
                sample_date, criteria=criteria, limit=3
            )

            if top_stocks.empty:
                print(f"No data available for {criteria} ranking")
                continue

            if criteria == "hl_ratio":
                columns = ["code", "hl_ratio"]
            elif criteria == "rsi":
                columns = ["code", "relative_strength_index"]
            elif criteria == "minervini":
                columns = ["code", "minervini_score"]
            else:  # composite
                columns = ["code", "composite_score"]

            # Only show columns that exist
            available_columns = [col for col in columns if col in top_stocks.columns]
            if available_columns:
                print(top_stocks[available_columns].to_string(index=False))
            else:
                print(
                    f"Required columns not found. Available: {list(top_stocks.columns)}"
                )

        except Exception as e:
            print(f"Error ranking by {criteria}: {e}")


def demo_filtering_stocks(sample_date="2023-12-01"):
    """Demonstrate filtering stocks by criteria"""
    print(f"\n=== Filtering Stocks by Criteria for {sample_date} ===")

    # Try different filter levels
    filter_sets = [
        {
            "hl_ratio_min": 70.0,
            "rsi_min": 60.0,
            "minervini_min": 3,
            "desc": "Moderate criteria",
        },
        {
            "hl_ratio_min": 80.0,
            "rsi_min": 70.0,
            "minervini_min": 5,
            "desc": "Strict criteria",
        },
        {
            "hl_ratio_min": 90.0,
            "rsi_min": 80.0,
            "minervini_min": 7,
            "desc": "Very strict criteria",
        },
    ]

    for filter_set in filter_sets:
        try:
            desc = filter_set.pop("desc")
            print(f"\n--- {desc} ---")
            print(
                f"HL Ratio >= {filter_set['hl_ratio_min']}, RSI >= {filter_set['rsi_min']}, Minervini >= {filter_set['minervini_min']}"
            )

            filtered_stocks = get_stocks_meeting_criteria(sample_date, **filter_set)

            if filtered_stocks.empty:
                print("No stocks meet these criteria")
            else:
                print(f"Found {len(filtered_stocks)} stocks meeting criteria:")
                display_columns = [
                    "code",
                    "hl_ratio",
                    "relative_strength_index",
                    "minervini_score",
                    "composite_score",
                ]
                available_columns = [
                    col for col in display_columns if col in filtered_stocks.columns
                ]
                print(filtered_stocks[available_columns].head().to_string(index=False))

        except Exception as e:
            print(f"Error filtering stocks: {e}")


def demo_multi_date_analysis():
    """Demonstrate multi-date time series analysis"""
    print("\n=== Multi-Date Time Series Analysis ===")

    try:
        # Try to find a date range with data
        # You can modify these dates based on your actual data
        end_date = "2023-12-01"
        start_date = "2023-11-20"
        sample_code = "1001"  # Modify based on your data

        print(f"Analyzing {sample_code} from {start_date} to {end_date}")

        time_series = get_multi_date_analysis(start_date, end_date, sample_code)

        if time_series.empty:
            print(f"No time series data found for {sample_code}")
            return

        print(f"Found {len(time_series)} data points")
        display_columns = [
            "date",
            "hl_ratio",
            "relative_strength_index",
            "minervini_score",
        ]
        available_columns = [
            col for col in display_columns if col in time_series.columns
        ]

        if available_columns:
            print(time_series[available_columns].to_string(index=False))
        else:
            print(f"Expected columns not found. Available: {list(time_series.columns)}")

    except Exception as e:
        print(f"Error in multi-date analysis: {e}")


def demo_summary_statistics(sample_date="2023-12-01"):
    """Demonstrate summary statistics"""
    print(f"\n=== Summary Statistics for {sample_date} ===")

    try:
        summary = create_analysis_summary(sample_date)

        if not summary:
            print(f"No summary data available for {sample_date}")
            return

        print(f"Total Stocks Analyzed: {summary.get('total_stocks', 0)}")
        print(f"Average HL Ratio: {summary.get('avg_hl_ratio', 0):.2f}")
        print(f"Average RSI: {summary.get('avg_rsi', 0):.2f}")
        print(f"Average Minervini Score: {summary.get('avg_minervini_score', 0):.2f}")
        print(f"Average Composite Score: {summary.get('avg_composite_score', 0):.2f}")
        print(f"Stocks with HL Ratio >= 80: {summary.get('high_hl_ratio_count', 0)}")
        print(f"Stocks with RSI >= 70: {summary.get('high_rsi_count', 0)}")
        print(
            f"Stocks with Minervini Score >= 5: {summary.get('strong_minervini_count', 0)}"
        )
        print(
            f"Stocks with Composite Score >= 70: {summary.get('strong_composite_count', 0)}"
        )

    except Exception as e:
        print(f"Error creating summary: {e}")


def main():
    """Main demonstration function"""
    print("Integrated Analysis Demonstration")
    print("=" * 50)

    # Set up logging
    logger = setup_logging()
    logger.info("Starting integrated analysis demonstration")

    # Run all demonstrations
    demo_database_coverage()
    demo_comprehensive_analysis()
    demo_top_stocks_ranking()
    demo_filtering_stocks()
    demo_multi_date_analysis()
    demo_summary_statistics()

    print("\n" + "=" * 50)
    print("Demonstration completed!")
    print("\nNote: If you see 'No data found' messages, it means the database")
    print("hasn't been populated with analysis results yet. Run the individual")
    print("analysis scripts (high_low_ratio.py, minervini.py, relative_strength.py)")
    print("first to populate the database with data.")


if __name__ == "__main__":
    main()
