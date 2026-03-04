"""
findata-analytics — Financial Data Pipeline & Quantitative Analytics

Run with: uv run python main.py
"""

import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from data_pipeline import synthetic, ingest
from analytics import queries
import visualize


# ═══════════════════════════════════════════════════════════════════════
# TERMINAL UI
# ═══════════════════════════════════════════════════════════════════════

BLUE = "\033[38;5;75m"
GREEN = "\033[38;5;114m"
ORANGE = "\033[38;5;215m"
RED = "\033[38;5;204m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"

BANNER = f"""
{BLUE}╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║   {BOLD}📈  findata-analytics{RESET}{BLUE}                                        ║
║   {DIM}Financial Data Pipeline & Quantitative Analytics{RESET}{BLUE}               ║
║                                                                  ║
║   Python · DuckDB · SQL Window Functions · GBM · Backtesting     ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝{RESET}
"""


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def show_menu():
    """Display the simplified main menu."""
    print(f"""
{BOLD}Choose a mode:{RESET}

  {GREEN}[1]{RESET}  🚀  Quick Demo        {DIM}— Synthetic data → full analytics → charts (instant){RESET}
  {GREEN}[2]{RESET}  📈  Real Market Data   {DIM}— Fetch live data from EODHD API → analytics → charts{RESET}
  {GREEN}[3]{RESET}  📊  Analyze Existing   {DIM}— Re-run analytics on data already in DuckDB{RESET}

  {DIM}[q]  Quit{RESET}
""")
    return input(f"  {BOLD}Enter choice (1-3 or q): {RESET}").strip().lower()


# ═══════════════════════════════════════════════════════════════════════
# EODHD AUTO-PROMPT
# ═══════════════════════════════════════════════════════════════════════

def _has_eodhd_key() -> bool:
    """Check if EODHD API key is set."""
    val = os.environ.get("EODHD_API_KEY", "").strip()
    return bool(val) and val != "your-eodhd-api-key-here"


def _prompt_eodhd_key() -> bool:
    """Prompt user for EODHD key. Returns True if key was set."""
    print(f"""
  {BOLD}📡 Real data requires an EODHD API key.{RESET}
  {DIM}Get a free key at: https://eodhd.com/register{RESET}
  {DIM}Or press Enter to go back to the menu.{RESET}
""")
    key = input(f"  {BOLD}EODHD API key: {RESET}").strip()

    if not key:
        return False

    # Save to .env
    env_path = Path(".env")
    lines = []
    if env_path.exists():
        lines = [l for l in env_path.read_text().splitlines() if not l.startswith("EODHD_API_KEY=")]
    lines.append(f"EODHD_API_KEY={key}")
    env_path.write_text("\n".join(lines) + "\n")
    os.environ["EODHD_API_KEY"] = key

    print(f"\n  {GREEN}✅ Saved to .env — you won't be asked again.{RESET}\n")
    return True


def prompt_ticker():
    """Ask for a ticker symbol."""
    print()
    ticker = input(f"  {BOLD}Enter ticker symbol{RESET} {DIM}(e.g. AAPL, MSFT, TSLA): {RESET}").strip().upper()
    if not ticker:
        print(f"  {RED}No ticker entered.{RESET}")
        return None
    return ticker


def prompt_dates():
    """Ask for optional date range."""
    print(f"  {DIM}Date range (press Enter for defaults: 2020-01-01 → 2025-12-31){RESET}")
    start = input(f"  Start date {DIM}(YYYY-MM-DD){RESET}: ").strip() or "2020-01-01"
    end = input(f"  End date   {DIM}(YYYY-MM-DD){RESET}: ").strip() or "2025-12-31"
    return start, end


def prompt_table():
    """List available tables and let the user pick one."""
    con = ingest.get_connection()
    tables = ingest.list_tables(con)
    con.close()

    if not tables:
        print(f"\n  {RED}No tables found in DuckDB. Run a demo or fetch data first.{RESET}\n")
        return None

    print(f"\n  {BOLD}Available tables in DuckDB:{RESET}")
    for i, t in enumerate(tables, 1):
        print(f"    {GREEN}[{i}]{RESET} {t}")

    choice = input(f"\n  {BOLD}Enter table number: {RESET}").strip()
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(tables):
            return tables[idx]
    except ValueError:
        pass

    print(f"  {RED}Invalid choice.{RESET}")
    return None


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def run_fetch(ticker: str, start: str, end: str) -> str | None:
    """Fetch real market data from EODHD API and store in DuckDB."""
    from data_pipeline import eodhd_client

    table_name = f"{ticker.lower().replace('.', '_').replace('-', '_')}_eod"

    print(f"\n{BLUE}{'─'*60}{RESET}")
    print(f"  {BOLD}📡 Fetching {ticker} from EODHD API...{RESET}")
    print(f"{BLUE}{'─'*60}{RESET}\n")

    df = eodhd_client.fetch_eod_data(ticker, start_date=start, end_date=end)

    if df.empty:
        print(f"  {RED}No data returned. Check your ticker or API key.{RESET}")
        return None

    con = ingest.get_connection()
    count = ingest.ingest_dataframe(con, table_name, df)

    stats = queries.summary_stats(con, table_name)
    print(f"\n  {GREEN}✓ Loaded {count} rows into DuckDB table '{table_name}'{RESET}")
    print(f"    Period:  {stats['start_date'].iloc[0]} → {stats['end_date'].iloc[0]}")
    print(f"    Price:   ${stats['min_close'].iloc[0]} – ${stats['max_close'].iloc[0]} (avg ${stats['avg_close'].iloc[0]})")

    con.close()
    return table_name


def run_synthetic() -> str:
    """Generate synthetic financial data and store in DuckDB."""
    table_name = "synthetic_data"

    print(f"\n{BLUE}{'─'*60}{RESET}")
    print(f"  {BOLD}🧪 Generating Synthetic Data (Geometric Brownian Motion){RESET}")
    print(f"{BLUE}{'─'*60}{RESET}\n")

    # Sequential (Pandas)
    print(f"  {ORANGE}Mode 1: Sequential (Pandas){RESET}")
    df, seq_m = synthetic.generate_sequential(start_year=2015, end_year=2025, start_price=100.0)
    print(f"    Rows: {seq_m['total_rows']}  |  p50: {seq_m['creation_p50_ms']:.3f} ms  |  p99: {seq_m['creation_p99_ms']:.3f} ms")

    con = ingest.get_connection()
    count = ingest.ingest_dataframe(con, table_name, df)
    print(f"    {GREEN}✓ {count} rows → '{table_name}'{RESET}")

    # Parallel (PyArrow)
    print(f"\n  {ORANGE}Mode 2: Parallel (PyArrow + ProcessPoolExecutor){RESET}")
    tables, par_m = synthetic.generate_parallel(start_year=2015, end_year=2025, start_price=100.0, workers=8)
    print(f"    Rows: {par_m['total_rows']}  |  Workers: {par_m['workers']}  |  p50: {par_m['creation_p50_ms']:.3f} ms  |  p99: {par_m['creation_p99_ms']:.3f} ms")
    print(f"    Total: {par_m['total_elapsed_s']:.3f}s")

    parallel_table = "synthetic_data_parallel"
    count = ingest.ingest_arrow_tables(con, parallel_table, tables)
    print(f"    {GREEN}✓ {count} rows → '{parallel_table}' (zero-copy){RESET}")

    con.close()
    return table_name


def run_analyze(table_name: str):
    """Run the full analytics suite on a DuckDB table and generate charts."""
    print(f"\n{BLUE}{'─'*60}{RESET}")
    print(f"  {BOLD}📊 Quantitative Analytics: {table_name}{RESET}")
    print(f"{BLUE}{'─'*60}{RESET}")

    con = ingest.get_connection()

    tables = ingest.list_tables(con)
    if table_name not in tables:
        print(f"\n  {RED}✗ Table '{table_name}' not found. Available: {tables}{RESET}")
        con.close()
        return

    stats = queries.summary_stats(con, table_name)
    print(f"\n  {DIM}Dataset: {stats['total_days'].iloc[0]} trading days | {stats['start_date'].iloc[0]} → {stats['end_date'].iloc[0]}{RESET}\n")

    # 1. Daily returns
    print(f"  {GREEN}[1/6]{RESET} Daily returns...")
    queries.daily_returns(con, table_name)

    # 2. Bollinger bands
    print(f"  {GREEN}[2/6]{RESET} MAs & Bollinger Bands...")
    bb_df = queries.bollinger_bands(con, table_name)
    visualize.plot_price_with_bollinger(bb_df, title=f"{table_name}: Price with MAs & Bollinger Bands")

    # 3. Rolling volatility
    print(f"  {GREEN}[3/6]{RESET} 30-day rolling volatility...")
    vol_df = queries.rolling_volatility(con, table_name)
    visualize.plot_rolling_volatility(vol_df, title=f"{table_name}: 30-Day Rolling Volatility")

    # 4. Drawdown
    print(f"  {GREEN}[4/6]{RESET} Drawdown from ATH...")
    dd_df = queries.max_drawdown(con, table_name)
    visualize.plot_drawdown(dd_df, title=f"{table_name}: Drawdown from All-Time High")
    worst = dd_df.loc[dd_df["drawdown_pct"].idxmin()]
    print(f"         {RED}Max drawdown: {worst['drawdown_pct']:.2f}% on {worst['date']}{RESET}")

    # 5. Monthly & yearly returns
    print(f"  {GREEN}[5/6]{RESET} Monthly & yearly returns...")
    monthly_df = queries.monthly_returns(con, table_name)
    yearly_df = queries.yearly_returns(con, table_name)
    visualize.plot_monthly_returns(monthly_df, title=f"{table_name}: Monthly Returns")
    visualize.plot_yearly_returns(yearly_df, title=f"{table_name}: Yearly Returns")

    # 6. Equity curves
    print(f"  {GREEN}[6/6]{RESET} MA crossover backtest...")
    strategy_df = queries.backtest_ma_crossover(con, table_name)
    bnh_df = queries.buy_and_hold(con, table_name)
    visualize.plot_equity_curves(strategy_df, bnh_df, title=f"{table_name}: MA Crossover vs Buy & Hold")

    # Cross signals
    signals = queries.cross_signals(con, table_name)
    if not signals.empty:
        print(f"\n  {BOLD}Signal Events ({len(signals)}):{RESET}")
        for _, row in signals.head(10).iterrows():
            emoji = "🟢" if row["signal"] == "GOLDEN_CROSS" else "🔴"
            print(f"    {emoji} {row['date']}  {row['signal']}  @ ${row['close']}")
        if len(signals) > 10:
            print(f"    {DIM}... and {len(signals) - 10} more{RESET}")

    # Portfolio summary
    if not strategy_df.empty and not bnh_df.empty:
        final_strat = strategy_df["strategy_equity"].iloc[-1]
        final_bnh = bnh_df["bnh_equity"].iloc[-1]
        print(f"\n  {BOLD}Final Portfolio Values (from $10,000):{RESET}")
        print(f"    Buy & Hold:        ${final_bnh:,.2f}")
        print(f"    MA Crossover:      ${final_strat:,.2f}")
        outperform = "outperformed" if final_strat > final_bnh else "underperformed"
        diff_pct = ((final_strat / final_bnh) - 1) * 100
        color = GREEN if final_strat > final_bnh else RED
        print(f"    {color}Strategy {outperform} by {abs(diff_pct):.1f}%{RESET}")

    print(f"\n  {GREEN}✓ All charts saved to output/{RESET}\n")
    con.close()


# ═══════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════

def main():
    clear_screen()
    print(BANNER)

    print(f"""
{BOLD}What this project does:{RESET}
  Generates or fetches financial data, stores it in DuckDB, runs 10 SQL
  analytics functions, backtests a trading strategy, and generates charts.

  {GREEN}→ Press 1 and Enter for an instant demo (no API key needed){RESET}
""")

    while True:
        choice = show_menu()

        if choice == "q":
            print(f"\n  {DIM}Goodbye! 👋{RESET}\n")
            break

        t0 = time.perf_counter()

        if choice == "1":
            # Quick demo — synthetic → analyze → charts
            table = run_synthetic()
            run_analyze(table)

        elif choice == "2":
            # Real data — check for EODHD key
            if not _has_eodhd_key():
                if not _prompt_eodhd_key():
                    continue
            ticker = prompt_ticker()
            if not ticker:
                continue
            start, end = prompt_dates()
            table = run_fetch(ticker, start, end)
            if table:
                run_analyze(table)

        elif choice == "3":
            # Analyze existing table
            table = prompt_table()
            if table:
                run_analyze(table)

        else:
            print(f"\n  {RED}Invalid choice. Enter 1-3 or q.{RESET}")
            continue

        elapsed = time.perf_counter() - t0
        print(f"  {DIM}Completed in {elapsed:.2f}s{RESET}\n")

        input(f"  {DIM}Press Enter to return to menu...{RESET}")
        clear_screen()
        print(BANNER)


if __name__ == "__main__":
    main()
