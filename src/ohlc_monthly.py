import pandas as pd


def calculate_monthly_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate daily stock prices into monthly OHLC values per ticker.

    Returns a dataframe with:
        ticker | month | open | high | low | close
    """

    # ensure chronological grouping and indexing for resampling
    df = df.sort_values(["ticker", "date"]).set_index("date")

    monthly_frames = []

    for ticker, data in df.groupby("ticker"):

        monthly = data.resample("ME").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
            }
        )

        monthly["ticker"] = ticker
        monthly["month"] = monthly.index.to_period("M")

        monthly = monthly[
            ["ticker", "month", "open", "high", "low", "close"]
        ]

        monthly_frames.append(monthly)

    # combine all per-ticker monthly frames into one result set
    final_df = (
        pd.concat(monthly_frames)
        .sort_values(["ticker", "month"])
        .reset_index(drop=True)
    )

    return final_df


if __name__ == "__main__":
    from load_data import load_stock_data
    from preprocess import preprocess_data

    raw = load_stock_data()
    clean = preprocess_data(raw)

    result = calculate_monthly_ohlc(clean)
    print(result.head())