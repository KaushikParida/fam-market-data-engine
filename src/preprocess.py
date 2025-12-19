import pandas as pd

# tickers expected in the input dataset
REQUIRED_TICKERS = [
    "AAPL",
    "AMD",
    "AMZN",
    "AVGO",
    "CSCO",
    "MSFT",
    "NFLX",
    "PEP",
    "TMUS",
    "TSLA",
]


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter dataset to required symbols, remove duplicates,
    enforce ordering, and validate ticker completeness.
    """

    # keeping only required symbols
    df = df[df["ticker"].isin(REQUIRED_TICKERS)].copy()

    if df.empty:
        raise ValueError("Dataset contains no required tickers.")

    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)

    if removed > 0:
        print(f"{removed} duplicate rows removed.")

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    actual_symbols = df["ticker"].unique().tolist()
    missing = [sym for sym in REQUIRED_TICKERS if sym not in actual_symbols]

    if missing:
        raise ValueError(f"Ticker(s) missing from dataset: {missing}")

    return df


if __name__ == "__main__":
    from load_data import load_stock_data

    raw = load_stock_data()
    cleaned = preprocess_data(raw)
    print(cleaned.head())