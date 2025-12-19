"""
Main orchestrator script for the data pipeline.
Loads data, preprocesses it, aggregates to monthly OHLC,
adds indicators and writes per-ticker output files.
"""

from load_data import load_stock_data
from preprocess import preprocess_data
from ohlc_monthly import calculate_monthly_ohlc
from indicators import add_indicators
from partition import partition_and_export


def run_pipeline():
    print("Starting pipeline...")

    try:
        print("Loading data...")
        df = load_stock_data()

        print("Preprocessing...")
        df = preprocess_data(df)

        print("Building monthly OHLC data...")
        df = calculate_monthly_ohlc(df)

        print("Computing indicators...")
        df = add_indicators(df)

        print("Exporting results...")
        partition_and_export(df)

    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        return

    print("Done.")


if __name__ == "__main__":
    run_pipeline()
