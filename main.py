"""
findata-analytics — Financial Data Pipeline & Quantitative Analytics

CLI entry point with three modes:
  --fetch TICKER    Pull real EOD data from EODHD API into DuckDB
  --synthetic       Generate synthetic OHLCV data via Geometric Brownian Motion
  --analyze TABLE   Run full analytics suite and generate charts

Usage:
  uv run python main.py --synthetic --analyze synthetic_data
  uv run python main.py --fetch AAPL --analyze aapl_eod
"""

import argparse
import sys
import time

from data_pipeline import eodhd_client, synthetic, ingest
from analytics import queries
import visualize


def cmd_fetch(args):
    """Fetch real market data from EODHD API and store in DuckDB."""
    print(f"\n{'='*60}")
    print(f"  EODHD Data Fetch: {args.fetch}")
    print(f"{'='*60}\n")

    ticker = args.fetch
    table_name = f"{ticker.lower().replace('.', '_').replace('-', '_')}_eod"

    print(f"  Fetching {ticker} EOD data...")
    df = eodhd_client.fetch_eod_data(
        ticker,
        start_date=args.start or "2020-01-01",
        end_date=args.end or "2025-12-31",
    )

    if df.empty:
        print("  No data returned. Check your ticker or API key.")
        return None

    con = ingest.get_connection()
    count = ingest.ingest_dataframe(con, table_name, df)
    print(f"  ✓ Loaded {count} rows into DuckDB table '{table_name}'\n")

    # Show summary
    stats = queries.summary_stats(con, table_name)
    print("  Summary:")
    print(f"    Period:  {stats['start_date'].iloc[0]} → {stats['end_date'].iloc[0]}")
    print(f"    Days:    {stats['total_days'].iloc[0]}")
    print(f"    Price:   ${stats['min_close'].iloc[0]} – ${stats['max_close'].iloc[0]} (avg ${stats['avg_close'].iloc[0]})")
    print()

    con.close()
    return table_name


def cmd_synthetic(args):
    """Generate synthetic financial data and store in DuckDB."""
    print(f"\n{'='*60}")
    print(f"  Synthetic Data Generation")
    print(f"{'='*60}\n")

    table_name = "synthetic_data"

    # --- Sequential (Pandas) ---
    print("  Mode 1: Sequential (Pandas)")
    df, seq_metrics = synthetic.generate_sequential(
        start_year=2015, end_year=2025, start_price=100.0
    )
    print(f"    Rows:         {seq_metrics['total_rows']}")
    print(f"    Batches:      {seq_metrics['batches']}")
    print(f"    Creation p50: {seq_metrics['creation_p50_ms']:.3f} ms")
    print(f"    Creation p99: {seq_metrics['creation_p99_ms']:.3f} ms")

    con = ingest.get_connection()
    count = ingest.ingest_dataframe(con, table_name, df)
    print(f"    ✓ Loaded {count} rows into '{table_name}'\n")

    # --- Parallel (PyArrow) ---
    print("  Mode 2: Parallel (PyArrow + ProcessPoolExecutor)")
    tables, par_metrics = synthetic.generate_parallel(
        start_year=2015, end_year=2025, start_price=100.0, workers=8
    )
    print(f"    Rows:         {par_metrics['total_rows']}")
    print(f"    Workers:      {par_metrics['workers']}")
    print(f"    Creation p50: {par_metrics['creation_p50_ms']:.3f} ms")
    print(f"    Creation p99: {par_metrics['creation_p99_ms']:.3f} ms")
    print(f"    Total time:   {par_metrics['total_elapsed_s']:.3f} s")

    parallel_table = "synthetic_data_parallel"
    count = ingest.ingest_arrow_tables(con, parallel_table, tables)
    print(f"    ✓ Loaded {count} rows into '{parallel_table}' (zero-copy)\n")

    con.close()
    return table_name


def cmd_analyze(args, table_name: str):
    """Run the full analytics suite on a DuckDB table and generate charts."""
    print(f"\n{'='*60}")
    print(f"  Quantitative Analytics: {table_name}")
    print(f"{'='*60}\n")

    con = ingest.get_connection()

    # Verify table exists
    tables = ingest.list_tables(con)
    if table_name not in tables:
        print(f"  ✗ Table '{table_name}' not found. Available tables: {tables}")
        con.close()
        return

    # Summary
    stats = queries.summary_stats(con, table_name)
    print(f"  Dataset: {stats['total_days'].iloc[0]} trading days")
    print(f"  Period:  {stats['start_date'].iloc[0]} → {stats['end_date'].iloc[0]}")
    print()

    # 1. Daily returns
    print("  [1/6] Computing daily returns...")
    returns_df = queries.daily_returns(con, table_name)

    # 2. Bollinger bands (includes MAs)
    print("  [2/6] Computing MAs & Bollinger Bands...")
    bb_df = queries.bollinger_bands(con, table_name)
    visualize.plot_price_with_bollinger(bb_df, title=f"{table_name}: Price with MAs & Bollinger Bands")

    # 3. Rolling volatility
    print("  [3/6] Computing 30-day rolling volatility...")
    vol_df = queries.rolling_volatility(con, table_name)
    visualize.plot_rolling_volatility(vol_df, title=f"{table_name}: 30-Day Rolling Volatility")

    # 4. Drawdown
    print("  [4/6] Computing drawdown from ATH...")
    dd_df = queries.max_drawdown(con, table_name)
    visualize.plot_drawdown(dd_df, title=f"{table_name}: Drawdown from All-Time High")

    worst = dd_df.loc[dd_df["drawdown_pct"].idxmin()]
    print(f"         Max drawdown: {worst['drawdown_pct']:.2f}% on {worst['date']}")

    # 5. Monthly & yearly returns
    print("  [5/6] Computing monthly & yearly returns...")
    monthly_df = queries.monthly_returns(con, table_name)
    yearly_df = queries.yearly_returns(con, table_name)
    visualize.plot_monthly_returns(monthly_df, title=f"{table_name}: Monthly Returns")
    visualize.plot_yearly_returns(yearly_df, title=f"{table_name}: Yearly Returns")

    # 6. Equity curves
    print("  [6/6] Backtesting MA crossover strategy...")
    strategy_df = queries.backtest_ma_crossover(con, table_name)
    bnh_df = queries.buy_and_hold(con, table_name)
    visualize.plot_equity_curves(
        strategy_df, bnh_df,
        title=f"{table_name}: MA Crossover vs Buy & Hold"
    )

    # Cross signals
    signals = queries.cross_signals(con, table_name)
    if not signals.empty:
        print(f"\n  Signal Events ({len(signals)} total):")
        for _, row in signals.head(10).iterrows():
            emoji = "🟢" if row["signal"] == "GOLDEN_CROSS" else "🔴"
            print(f"    {emoji} {row['date']}  {row['signal']}  @ ${row['close']}")
        if len(signals) > 10:
            print(f"    ... and {len(signals) - 10} more")

    # Final summary
    if not strategy_df.empty and not bnh_df.empty:
        final_strat = strategy_df["strategy_equity"].iloc[-1]
        final_bnh = bnh_df["bnh_equity"].iloc[-1]
        print(f"\n  Final Portfolio Values (from $10,000):")
        print(f"    Buy & Hold:        ${final_bnh:,.2f}")
        print(f"    MA Crossover:      ${final_strat:,.2f}")
        outperform = "outperformed" if final_strat > final_bnh else "underperformed"
        diff_pct = ((final_strat / final_bnh) - 1) * 100
        print(f"    Strategy {outperform} by {abs(diff_pct):.1f}%")

    print(f"\n  ✓ All charts saved to output/\n")
    con.close()


def main():
    parser = argparse.ArgumentParser(
        description="findata-analytics — Financial Data Pipeline & Quantitative Analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python main.py --synthetic --analyze synthetic_data
  uv run python main.py --fetch AAPL --analyze aapl_eod
  uv run python main.py --fetch MSFT --start 2022-01-01 --end 2025-01-01 --analyze msft_eod
        """,
    )
    parser.add_argument("--fetch", type=str, help="Fetch EOD data for a ticker (e.g. AAPL)")
    parser.add_argument("--synthetic", action="store_true", help="Generate synthetic OHLCV data")
    parser.add_argument("--analyze", type=str, help="Run analytics on a DuckDB table name")
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    if not any([args.fetch, args.synthetic, args.analyze]):
        parser.print_help()
        sys.exit(0)

    t0 = time.perf_counter()
    table_name = None

    if args.fetch:
        table_name = cmd_fetch(args)

    if args.synthetic:
        table_name = cmd_synthetic(args)

    if args.analyze:
        cmd_analyze(args, args.analyze)
    elif table_name:
        cmd_analyze(args, table_name)

    elapsed = time.perf_counter() - t0
    print(f"Total elapsed time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
