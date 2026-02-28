"""
Synthetic Financial Data Generator
Creates realistic OHLCV data using Geometric Brownian Motion (GBM).
Supports both sequential (Pandas) and parallel (PyArrow + ProcessPoolExecutor) modes.
"""

import time
import numpy as np
import pandas as pd
import pyarrow as pa
from concurrent.futures import ProcessPoolExecutor, as_completed


# ---------------------------------------------------------------------------
# Core GBM Generator
# ---------------------------------------------------------------------------

def generate_ohlcv(
    start_date: str,
    end_date: str,
    start_price: float = 100.0,
    volatility: float = 0.02,
    drift: float = 0.0003,
) -> pd.DataFrame:
    """
    Generate synthetic OHLCV data using Geometric Brownian Motion.

    Args:
        start_date:  Start date string (YYYY-MM-DD)
        end_date:    End date string (YYYY-MM-DD)
        start_price: Initial price level
        volatility:  Daily volatility (std dev of returns)
        drift:       Daily drift (mean of returns)

    Returns:
        DataFrame with columns: date, open, high, low, close, adjusted_close, volume
    """
    dates = pd.bdate_range(start=start_date, end=end_date)
    n_days = len(dates)

    if n_days == 0:
        return pd.DataFrame()

    # Geometric Brownian Motion: S(t) = S(0) * exp(cumsum(returns))
    returns = np.random.normal(drift, volatility, n_days)
    price_path = start_price * np.exp(np.cumsum(returns))

    # Generate OHLC spread around the price path
    open_prices = price_path * np.random.normal(1.0, 0.005, n_days)
    close_prices = price_path * np.random.normal(1.0, 0.005, n_days)

    high_prices = np.maximum(open_prices, close_prices) * np.random.normal(1.01, 0.002, n_days)
    low_prices = np.minimum(open_prices, close_prices) * np.random.normal(0.99, 0.002, n_days)

    # Ensure high >= everything, low <= everything
    high_prices = np.maximum.reduce([open_prices, close_prices, high_prices])
    low_prices = np.minimum.reduce([open_prices, close_prices, low_prices])

    # Lognormal volume distribution
    volumes = np.random.lognormal(mean=14, sigma=1.5, size=n_days).astype(int)

    df = pd.DataFrame({
        "date": dates.date,
        "open": np.round(open_prices, 4),
        "high": np.round(high_prices, 4),
        "low": np.round(low_prices, 4),
        "close": np.round(close_prices, 4),
        "adjusted_close": np.round(close_prices, 4),
        "volume": volumes,
    })

    return df


# ---------------------------------------------------------------------------
# Sequential Generation (year-by-year batches with latency tracking)
# ---------------------------------------------------------------------------

def generate_sequential(
    start_year: int = 2015,
    end_year: int = 2025,
    start_price: float = 100.0,
    volatility: float = 0.02,
) -> tuple[pd.DataFrame, dict]:
    """
    Generate synthetic data sequentially, one year at a time.
    Returns the full DataFrame and a dict of latency metrics.
    """
    creation_times = []
    all_chunks = []
    current_price = start_price

    for year in range(start_year, end_year + 1):
        t0 = time.perf_counter()
        chunk = generate_ohlcv(
            f"{year}-01-01", f"{year}-12-31",
            start_price=current_price,
            volatility=volatility,
        )
        t1 = time.perf_counter()

        if chunk.empty:
            continue

        creation_times.append(t1 - t0)
        current_price = chunk["close"].iloc[-1]
        all_chunks.append(chunk)

    df = pd.concat(all_chunks, ignore_index=True)

    metrics = {
        "mode": "sequential (pandas)",
        "batches": len(creation_times),
        "total_rows": len(df),
        "creation_p50_ms": np.percentile(creation_times, 50) * 1000,
        "creation_p99_ms": np.percentile(creation_times, 99) * 1000,
    }

    return df, metrics


# ---------------------------------------------------------------------------
# Parallel Generation (PyArrow + ProcessPoolExecutor)
# ---------------------------------------------------------------------------

def _generate_chunk_arrow(dates_chunk, price_chunk):
    """
    Worker function for parallel generation.
    Runs in a separate process — true parallelism, no GIL contention.
    Returns a PyArrow Table for zero-copy DuckDB ingestion.
    """
    t0 = time.perf_counter()
    n_days = len(dates_chunk)

    if n_days == 0:
        return pa.table({}), 0.0

    open_prices = price_chunk * np.random.normal(1.0, 0.005, n_days)
    close_prices = price_chunk * np.random.normal(1.0, 0.005, n_days)

    high_prices = np.maximum(open_prices, close_prices) * np.random.normal(1.01, 0.002, n_days)
    low_prices = np.minimum(open_prices, close_prices) * np.random.normal(0.99, 0.002, n_days)

    high_prices = np.maximum.reduce([open_prices, close_prices, high_prices])
    low_prices = np.minimum.reduce([open_prices, close_prices, low_prices])

    volumes = np.random.lognormal(mean=14, sigma=1.5, size=n_days).astype(np.int64)

    # PyArrow Table — DuckDB ingests this zero-copy (no conversion needed)
    arrow_table = pa.table({
        "date": dates_chunk.date,
        "open": np.round(open_prices, 4),
        "high": np.round(high_prices, 4),
        "low": np.round(low_prices, 4),
        "close": np.round(close_prices, 4),
        "adjusted_close": np.round(close_prices, 4),
        "volume": volumes,
    })

    t1 = time.perf_counter()
    return arrow_table, t1 - t0


def generate_parallel(
    start_year: int = 2015,
    end_year: int = 2025,
    start_price: float = 100.0,
    volatility: float = 0.02,
    workers: int = 8,
) -> tuple[list[pa.Table], dict]:
    """
    Generate synthetic data in parallel using ProcessPoolExecutor + PyArrow.

    Pre-generates the full price path vectorized (breaks sequential dependency),
    then fans out year-chunks to worker processes for OHLCV generation.

    Returns a list of PyArrow Tables and latency metrics.
    """
    # Pre-generate the full price path (vectorized — virtually instantaneous)
    dates = pd.bdate_range(start=f"{start_year}-01-01", end=f"{end_year}-12-31")
    n_days = len(dates)

    returns = np.random.normal(0, volatility, n_days)
    price_path = start_price * np.exp(np.cumsum(returns))

    # Slice into per-year chunks
    chunks = []
    for year in range(start_year, end_year + 1):
        mask = dates.year == year
        if mask.sum() > 0:
            chunks.append((dates[mask], price_path[mask]))

    creation_times = []
    arrow_tables = []

    master_t0 = time.perf_counter()

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_generate_chunk_arrow, d, p): d
            for d, p in chunks
        }

        for future in as_completed(futures):
            try:
                table, c_time = future.result()
                creation_times.append(c_time)
                if table.num_rows > 0:
                    arrow_tables.append(table)
            except Exception as e:
                print(f"  ✗ Batch generation failed: {e}")

    master_t1 = time.perf_counter()

    metrics = {
        "mode": "parallel (pyarrow + multiprocessing)",
        "batches": len(creation_times),
        "total_rows": sum(t.num_rows for t in arrow_tables),
        "workers": workers,
        "creation_p50_ms": np.percentile(creation_times, 50) * 1000,
        "creation_p99_ms": np.percentile(creation_times, 99) * 1000,
        "total_elapsed_s": master_t1 - master_t0,
    }

    return arrow_tables, metrics
