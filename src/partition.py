import pandas as pd
from pathlib import Path


OUTPUT_DIR = Path(__file__).resolve().parents[1] / "processed"


def _validate_months(df: pd.DataFrame, expected_rows: int) -> None:
    """
    Validates monthly completeness for a single ticker.
    Ensures the number of rows matches expectation and that
    months follow a continuous period range.
    """

    months = pd.PeriodIndex(df["month"], freq="M")

    if len(months) != expected_rows:
        raise ValueError(
            f"Ticker {df['ticker'].iloc[0]} has {len(months)} rows; "
            f"{expected_rows} expected."
        )

    # build expected range based on first month
    expected_range = pd.period_range(
        start=months.min(), periods=expected_rows, freq="M"
    )

    if not months.equals(expected_range):
        raise ValueError(
            f"Ticker {df['ticker'].iloc[0]} month sequence is not continuous."
        )


def partition_and_export(
    df: pd.DataFrame,
    output_dir: Path = OUTPUT_DIR,
    expected_rows: int = 24,
) -> None:
    """
    Write one ticker file to processed/ folder.
    Output: result_<SYMBOL>.csv as per instructions.
    """

    output_dir.mkdir(exist_ok=True)

    for ticker, subset in df.groupby("ticker", sort=False):

        subset = subset.sort_values("month")

        _validate_months(subset, expected_rows)

        output_path = output_dir / f"result_{ticker}.csv"

        subset.to_csv(output_path, index=False)

        print(f"Exported {ticker}: {output_path.name}")


if __name__ == "__main__":
    from load_data import load_stock_data
    from preprocess import preprocess_data
    from ohlc_monthly import calculate_monthly_ohlc
    from indicators import add_indicators

    raw = load_stock_data()
    clean = preprocess_data(raw)
    monthly = calculate_monthly_ohlc(clean)
    enriched = add_indicators(monthly)

    partition_and_export(enriched)