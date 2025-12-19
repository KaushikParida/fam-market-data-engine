import pandas as pd


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add SMA and EMA indicators to monthly OHLC data.
    Indicators are calculated per ticker on monthly closing prices.
    """

    frames = []

    for ticker, data in df.groupby("ticker", sort=False):

        data = data.sort_values("month").copy()

        # simple moving averages(SMA) on monthly close prices
        data["sma10"] = data["close"].rolling(window=10).mean()
        data["sma20"] = data["close"].rolling(window=20).mean()

        # exponential moving averages(EMA) on monthly close prices
        data["ema10"] = data["close"].ewm(span=10, adjust=False).mean()
        data["ema20"] = data["close"].ewm(span=20, adjust=False).mean()

        frames.append(data)

    result = (
        pd.concat(frames)
        .sort_values(["ticker", "month"])
        .reset_index(drop=True)
    )

    return result


if __name__ == "__main__":
    from load_data import load_stock_data
    from preprocess import preprocess_data
    from ohlc_monthly import calculate_monthly_ohlc

    raw = load_stock_data()
    clean = preprocess_data(raw)
    monthly = calculate_monthly_ohlc(clean)

    df = add_indicators(monthly)
    print(df.head())