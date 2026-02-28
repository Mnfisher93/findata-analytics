"""
SQL Analytics Engine
All quantitative analysis runs as SQL against DuckDB using window functions, CTEs, and views.
Each function takes a DuckDB connection and table name, and returns a pandas DataFrame.
"""

import duckdb
import pandas as pd


# ---------------------------------------------------------------------------
# Daily Returns
# ---------------------------------------------------------------------------

def daily_returns(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Calculate daily percentage returns using LAG() window function."""
    return con.execute(f"""
        SELECT
            date,
            close,
            LAG(close) OVER (ORDER BY date) AS prev_close,
            ROUND(
                (close / LAG(close) OVER (ORDER BY date) - 1) * 100,
                4
            ) AS daily_return_pct
        FROM {table}
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# Moving Averages (50-day and 200-day)
# ---------------------------------------------------------------------------

def moving_averages(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Calculate 50-day and 200-day simple moving averages using windowed AVG."""
    return con.execute(f"""
        SELECT
            date,
            close,
            ROUND(
                AVG(close) OVER (
                    ORDER BY date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ), 2
            ) AS ma_50,
            ROUND(
                AVG(close) OVER (
                    ORDER BY date
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ), 2
            ) AS ma_200
        FROM {table}
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# Bollinger Bands (±2σ around 50-day MA)
# ---------------------------------------------------------------------------

def bollinger_bands(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Calculate Bollinger Bands using windowed STDDEV around the 50-day MA."""
    return con.execute(f"""
        WITH ma_data AS (
            SELECT
                date,
                close,
                AVG(close) OVER (
                    ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS ma_50,
                STDDEV_POP(close) OVER (
                    ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS std_50,
                AVG(close) OVER (
                    ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS ma_200
            FROM {table}
        )
        SELECT
            date,
            ROUND(close, 2) AS close,
            ROUND(ma_50, 2) AS ma_50,
            ROUND(ma_200, 2) AS ma_200,
            ROUND(ma_50 + 2 * std_50, 2) AS bb_upper,
            ROUND(ma_50 - 2 * std_50, 2) AS bb_lower
        FROM ma_data
        WHERE ma_50 IS NOT NULL
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# Golden Cross / Death Cross Detection
# ---------------------------------------------------------------------------

def cross_signals(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """
    Detect Golden Cross (MA50 crosses above MA200) and
    Death Cross (MA50 crosses below MA200) dates.
    """
    return con.execute(f"""
        WITH ma_data AS (
            SELECT
                date,
                close,
                AVG(close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)  AS ma_50,
                AVG(close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma_200
            FROM {table}
        ),
        with_lag AS (
            SELECT
                date,
                close,
                ma_50,
                ma_200,
                LAG(ma_50)  OVER (ORDER BY date) AS prev_ma_50,
                LAG(ma_200) OVER (ORDER BY date) AS prev_ma_200
            FROM ma_data
            WHERE ma_50 IS NOT NULL AND ma_200 IS NOT NULL
        )
        SELECT
            date,
            ROUND(close, 2) AS close,
            ROUND(ma_50, 2) AS ma_50,
            ROUND(ma_200, 2) AS ma_200,
            CASE
                WHEN prev_ma_50 < prev_ma_200 AND ma_50 >= ma_200 THEN 'GOLDEN_CROSS'
                WHEN prev_ma_50 >= prev_ma_200 AND ma_50 < ma_200 THEN 'DEATH_CROSS'
            END AS signal
        FROM with_lag
        WHERE (prev_ma_50 < prev_ma_200 AND ma_50 >= ma_200)
           OR (prev_ma_50 >= prev_ma_200 AND ma_50 < ma_200)
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# 30-Day Rolling Volatility (Annualized)
# ---------------------------------------------------------------------------

def rolling_volatility(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Calculate 30-day rolling annualized volatility using STDDEV_POP."""
    return con.execute(f"""
        WITH daily_ret AS (
            SELECT
                date,
                (close / LAG(close) OVER (ORDER BY date) - 1) AS ret
            FROM {table}
        )
        SELECT
            date,
            ROUND(
                STDDEV_POP(ret) OVER (
                    ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) * SQRT(252),
                4
            ) AS ann_volatility
        FROM daily_ret
        WHERE ret IS NOT NULL
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# Maximum Drawdown
# ---------------------------------------------------------------------------

def max_drawdown(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Calculate drawdown from all-time high using running MAX."""
    return con.execute(f"""
        SELECT
            date,
            close,
            MAX(close) OVER (
                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_peak,
            ROUND(
                (close / MAX(close) OVER (
                    ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) - 1) * 100,
                4
            ) AS drawdown_pct
        FROM {table}
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# Monthly Returns (CTEs + LAG)
# ---------------------------------------------------------------------------

def monthly_returns(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Aggregate month-over-month returns using CTEs and window functions."""
    return con.execute(f"""
        WITH monthly_bounds AS (
            SELECT
                YEAR(date)  AS yr,
                MONTH(date) AS mo,
                MIN(date)   AS first_date,
                MAX(date)   AS last_date
            FROM {table}
            GROUP BY YEAR(date), MONTH(date)
        ),
        monthly_prices AS (
            SELECT
                mb.yr,
                mb.mo,
                s1.close AS first_close,
                s2.close AS last_close
            FROM monthly_bounds mb
            JOIN {table} s1 ON s1.date = mb.first_date
            JOIN {table} s2 ON s2.date = mb.last_date
        ),
        monthly_with_prev AS (
            SELECT
                yr, mo, first_close, last_close,
                LAG(last_close) OVER (ORDER BY yr, mo) AS prev_month_close
            FROM monthly_prices
        )
        SELECT
            yr, mo,
            ROUND(
                (last_close - prev_month_close) * 100 / prev_month_close, 2
            ) AS monthly_return_pct
        FROM monthly_with_prev
        WHERE prev_month_close IS NOT NULL
        ORDER BY yr, mo
    """).fetchdf()


# ---------------------------------------------------------------------------
# Yearly Returns
# ---------------------------------------------------------------------------

def yearly_returns(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Aggregate year-over-year returns using CTEs and window functions."""
    return con.execute(f"""
        WITH yearly_bounds AS (
            SELECT
                YEAR(date)    AS yr,
                MIN(date)     AS first_date,
                MAX(date)     AS last_date
            FROM {table}
            GROUP BY YEAR(date)
        ),
        yearly_prices AS (
            SELECT
                yb.yr,
                s1.close AS first_close,
                s2.close AS last_close
            FROM yearly_bounds yb
            JOIN {table} s1 ON s1.date = yb.first_date
            JOIN {table} s2 ON s2.date = yb.last_date
        ),
        yearly_with_prev AS (
            SELECT
                yr, first_close, last_close,
                LAG(last_close) OVER (ORDER BY yr) AS prev_year_close
            FROM yearly_prices
        )
        SELECT
            yr,
            ROUND(
                (last_close - prev_year_close) * 100 / prev_year_close, 2
            ) AS yearly_return_pct
        FROM yearly_with_prev
        WHERE prev_year_close IS NOT NULL
        ORDER BY yr
    """).fetchdf()


# ---------------------------------------------------------------------------
# MA Crossover Backtest (Strategy Equity Curve)
# ---------------------------------------------------------------------------

def backtest_ma_crossover(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """
    Backtest a 50/200-day MA crossover strategy.
    Long when MA50 > MA200, flat (cash) otherwise.
    Returns equity curve starting from $10,000.
    """
    return con.execute(f"""
        WITH signals AS (
            SELECT
                date,
                close,
                CASE
                    WHEN AVG(close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
                       > AVG(close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
                    THEN 1
                    ELSE 0
                END AS long_flag
            FROM {table}
        ),
        with_returns AS (
            SELECT
                date,
                close,
                long_flag,
                (close / LAG(close) OVER (ORDER BY date) - 1) AS daily_ret
            FROM signals
        )
        SELECT
            date,
            close,
            long_flag,
            ROUND(
                EXP(
                    SUM(LN(1 + CASE WHEN long_flag = 1 THEN daily_ret ELSE 0 END))
                    OVER (ORDER BY date)
                ) * 10000,
                2
            ) AS strategy_equity
        FROM with_returns
        WHERE daily_ret IS NOT NULL
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# Buy-and-Hold Benchmark
# ---------------------------------------------------------------------------

def buy_and_hold(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Buy-and-hold equity curve starting from $10,000."""
    return con.execute(f"""
        WITH daily_ret AS (
            SELECT
                date,
                close,
                (close / LAG(close) OVER (ORDER BY date) - 1) AS daily_ret
            FROM {table}
        )
        SELECT
            date,
            close,
            ROUND(
                EXP(
                    SUM(LN(1 + daily_ret)) OVER (ORDER BY date)
                ) * 10000,
                2
            ) AS bnh_equity
        FROM daily_ret
        WHERE daily_ret IS NOT NULL
        ORDER BY date
    """).fetchdf()


# ---------------------------------------------------------------------------
# Summary Statistics
# ---------------------------------------------------------------------------

def summary_stats(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Quick summary statistics for the dataset."""
    return con.execute(f"""
        SELECT
            COUNT(*)                    AS total_days,
            MIN(date)                   AS start_date,
            MAX(date)                   AS end_date,
            ROUND(MIN(close), 2)        AS min_close,
            ROUND(MAX(close), 2)        AS max_close,
            ROUND(AVG(close), 2)        AS avg_close,
            ROUND(AVG(volume), 0)       AS avg_volume
        FROM {table}
    """).fetchdf()
