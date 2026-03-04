"""
EODHD API Client
Fetches end-of-day historical stock market data from EODHD and returns clean DataFrames.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from eodhd import APIClient


load_dotenv()


def _get_client() -> APIClient:
    """Initialize the EODHD API client with the key from .env."""
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise SystemExit(
            "Missing EODHD_API_KEY. Set it in your .env file or restart the program.\n"
            "Get a free key at: https://eodhd.com/register"
        )
    return APIClient(api_key)


def fetch_eod_data(
    ticker: str,
    start_date: str = "2020-01-01",
    end_date: str = "2025-12-31",
    period: str = "d",
) -> pd.DataFrame:
    """
    Fetch end-of-day OHLCV data for a single ticker.

    Args:
        ticker:     Stock symbol (e.g. 'AAPL', 'MSFT.US', 'BTC-USD.CC')
        start_date: Start date in YYYY-MM-DD format
        end_date:   End date in YYYY-MM-DD format
        period:     'd' = daily, 'w' = weekly, 'm' = monthly

    Returns:
        DataFrame with columns: date, open, high, low, close, adjusted_close, volume
    """
    client = _get_client()

    resp = client.get_eod_historical_stock_market_data(
        symbol=ticker,
        period=period,
        from_date=start_date,
        to_date=end_date,
        order="a",
    )

    # EODHD sometimes appends a 'warning' key to the last record — strip it
    clean_data = [{k: v for k, v in row.items() if k != "warning"} for row in resp]

    df = pd.DataFrame(clean_data)

    if df.empty:
        print(f"  ⚠ No data returned for {ticker} ({start_date} → {end_date})")
        return df

    # Standardize column names
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Ensure date column is proper datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    # Ensure numeric types
    numeric_cols = ["open", "high", "low", "close", "adjusted_close", "volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  ✓ Fetched {len(df)} records for {ticker}")
    return df


def fetch_multiple(
    tickers: list[str],
    start_date: str = "2020-01-01",
    end_date: str = "2025-12-31",
) -> dict[str, pd.DataFrame]:
    """Fetch EOD data for multiple tickers. Returns dict of ticker → DataFrame."""
    results = {}
    for ticker in tickers:
        try:
            results[ticker] = fetch_eod_data(ticker, start_date, end_date)
        except Exception as e:
            print(f"  ✗ Failed to fetch {ticker}: {e}")
    return results
