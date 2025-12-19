import pandas as pd
from pathlib import Path

INPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "stock_data.csv"

# expected input schema for validation
EXPECTED_COLUMNS = [
    "date",
    "volume",
    "open",
    "high",
    "low",
    "close",
    "adjclose",
    "ticker"
]


def load_stock_data(input_path: Path = INPUT_PATH) -> pd.DataFrame:
    """
    Read the input stock dataset into a pandas DataFrame, validate schema,
    parse dates and enforce basic ordering/typing expectations.
    """

    input_path = Path(input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found at: {input_path}")

    df = pd.read_csv(input_path)

    if set(df.columns) != set(EXPECTED_COLUMNS):
        raise ValueError(
            "Input file has unexpected schema. "
            f"Expected columns: {EXPECTED_COLUMNS} "
            f"Received columns: {list(df.columns)}"
        )

    df = df[EXPECTED_COLUMNS]
    # convert date strings to datetime
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

    # ensure dataset chronological order
    df = df.sort_values("date").reset_index(drop=True)

    # enforce numeric dtype on price/volume fields
    numeric_cols = ["volume", "open", "high", "low", "close", "adjclose"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    if df.isnull().values.any():
        print("Note: Null values detected; they will propagate in downstream calculations.")

    return df


if __name__ == "__main__":
    data = load_stock_data()
    print(data.head())
