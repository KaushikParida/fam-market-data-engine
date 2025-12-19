# FAM Data Engineering Assignment – Monthly Stock Aggregation

This repository contains my solution to the FAM Data Engineering assignment. The goal was to convert two years of daily stock data into monthly financial summaries and compute technical indicators on top of it.

I approached the problem by breaking it down into independent processing stages that can be tested, read, and maintained easily.

---

## Problem Understanding & Approach

The dataset included daily OHLC price data for the following tickers:

AAPL, AMD, AMZN, AVGO, CSCO, MSFT, NFLX, PEP, TMUS, TSLA

The expected outcome was:

- Monthly OHLC values for each ticker  
- Indicators based on monthly closing price:
  - SMA10, SMA20
  - EMA10, EMA20  
- One CSV file per ticker  
- Exactly 24 monthly rows per file  

To achieve this, I designed a small processing framework using pure Pandas (no third-party TA libraries). Each step is handled in a separate script to reflect actual engineering project structure.

---

## What the Pipeline Does

**Daily → Monthly Aggregation**

For every ticker, I resampled daily data into monthly frequency as follows:

- Open = price of first day of the month  
- Close = price of last day  
- High = maximum value in the month  
- Low = minimum value in the month  

This matches standard OHLC logic in finance.

**Technical Indicators**

Once monthly close values were generated, I calculated:

- SMA10 & SMA20 using a rolling window
- EMA10 & EMA20 using exponential weighted functions

Indicator calculations begin only when enough months exist, so early blanks are expected and correct.

**Output Generation**

The result for each ticker is exported into: processed/result_<SYMBOL>.csv

Each file contains 24 monthly rows covering Jan 2018 to Dec 2019.

---

## How the Code Is Structured
src/
load_data.py → reads CSV + schema validation
preprocess.py → filtering + ordering + deduplication
ohlc_monthly.py → monthly aggregation logic
indicators.py → SMA/EMA calculations
partition.py → per-ticker output export
main.py → executes the entire pipeline

This modular layout allowed me to test each transformation individually before chaining them together.

---

## How to Run

From project root:

```bash
python src/main.py
```
This runs the full pipeline end-to-end and writes all final CSV files.

---

## Notes & Assumptions

This solution follows the instruction of computing all technical indicators strictly on monthly closing prices. I did not apply forward filling, backfilling, or interpolation anywhere in the workflow. The early SMA/EMA values appear blank because the rolling windows do not have enough monthly data points in the initial rows, which is mathematically correct behaviour.

The dataset naturally contained a complete 24-month range for each ticker (Jan 2018 → Dec 2019), so no padding or synthetic month construction was required. The entire pipeline is implemented using Python and Pandas only, as was asked, without relying on external technical analysis libraries.

---

## Closing Statement

This assignment gave me a good opportunity to demonstrate core data engineering fundamentals — including data ingestion, schema validation, time-series resampling, rolling indicator calculations, pipeline modularity, and dataset partitioning into clean outputs.

My approach to this project reflects how I handle real data problems: breaking work into clear steps, validating assumptions early, and ensuring each transformation remains reproducible and easy to review.

Thank you for taking the time to evaluate my submission.

— Kaushik Parida
