<p align="center">
  <h1 align="center">📈 findata-analytics</h1>
  <p align="center">
    <strong>Financial data pipeline & quantitative analytics with Python, DuckDB, and EODHD</strong>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/database-DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB">
    <img src="https://img.shields.io/badge/data-EODHD_API-0A66C2?style=for-the-badge" alt="EODHD">
    <img src="https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge" alt="MIT License">
  </p>
</p>

---

## 📋 Overview

An end-to-end financial analytics platform that ingests real market data via the EODHD API (or generates synthetic data via Geometric Brownian Motion), stores it in DuckDB, runs SQL-native quantitative analysis using window functions and CTEs, and produces publication-quality visualizations — all from the command line.

---

## ⚡ Features

| Category | What It Does |
|----------|-------------|
| **Real Data Pipeline** | Fetches EOD OHLCV data from EODHD for any ticker |
| **Synthetic Data Generator** | Creates realistic price series via Geometric Brownian Motion |
| **DuckDB Analytics** | 10 SQL analytics functions using window functions, CTEs, and LAG |
| **Performance Benchmarking** | Compares sequential (Pandas) vs parallel (PyArrow) ingestion |
| **MA Crossover Backtest** | Tests a 50/200-day moving average strategy vs buy-and-hold |
| **Visualization Suite** | 6 chart types with dark-mode styling |

---

## 🚀 Getting Started

### Prerequisites

| Requirement | Details |
|-------------|---------|
| **Python** | 3.11+ |
| **Package Manager** | [uv](https://docs.astral.sh/uv/) |
| **API Key** | [EODHD](https://eodhd.com/register) — free tier available |

### Installation

**1. Install [`uv`](https://docs.astral.sh/uv/)** (Python package manager)

macOS / Linux:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows (PowerShell):
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Or via Homebrew (macOS):
```bash
brew install uv
```

**2. Clone the repository**

```bash
git clone https://github.com/Mnfisher93/findata-analytics.git
```

**3. Navigate into the project**

```bash
cd findata-analytics
```

**4. Install dependencies**

```bash
uv sync
```

> `uv` handles all dependency resolution and virtual environment creation automatically — no manual `pip install` required.

**5. Set up your EODHD API key**

macOS / Linux:
```bash
cp .env.example .env
```

Windows (PowerShell):
```powershell
Copy-Item .env.example .env
```

Open `.env` in a text editor and paste your [EODHD API key](https://eodhd.com/register) (free tier works).

> **No API key?** You can still run the project with synthetic data using `--synthetic` mode — no key required.

### Usage

**Run with real market data (requires API key):**

```bash
uv run python main.py --fetch AAPL --analyze aapl_eod
```

**Run with synthetic data (no API key needed):**

```bash
uv run python main.py --synthetic --analyze synthetic_data
```

**Custom date range:**

```bash
uv run python main.py --fetch MSFT --start 2022-01-01 --end 2025-01-01 --analyze msft_eod
```

All charts are saved to the `output/` directory automatically.

---

## 🏗️ Architecture

```
                    ┌─────────────────────────────┐
                    │         main.py (CLI)        │
                    │   --fetch · --synthetic ·    │
                    │        --analyze             │
                    └──────┬──────────┬────────────┘
                           │          │
              ┌────────────▼──┐  ┌────▼──────────────┐
              │ Data Pipeline │  │  Analytics Engine  │
              ├───────────────┤  ├────────────────────┤
              │ eodhd_client  │  │ queries.py         │
              │ synthetic.py  │  │ (10 SQL functions) │
              │ ingest.py     │  └────────┬───────────┘
              └───────┬───────┘           │
                      │                   │
              ┌───────▼───────────────────▼───┐
              │          DuckDB               │
              │    (findata.db — local file)   │
              └───────────────┬───────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   visualize.py    │
                    │  (6 chart types)  │
                    └───────────────────┘
```

---

## 📁 Project Structure

```
findata-analytics/
├── pyproject.toml               # Dependency manifest (uv)
├── .env.example                 # API key template
├── main.py                      # CLI entry point
├── data_pipeline/
│   ├── eodhd_client.py          # EODHD API → pandas DataFrame
│   ├── synthetic.py             # GBM synthetic data (sequential + parallel)
│   └── ingest.py                # DuckDB ingestion (pandas + PyArrow zero-copy)
├── analytics/
│   └── queries.py               # SQL analytics engine (10 functions)
├── visualize.py                 # Matplotlib chart generation
└── output/                      # Generated charts (.png)
```

---

## 🔬 SQL Analytics Engine

All quantitative analysis runs as **SQL queries directly inside DuckDB** — leveraging window functions, CTEs, and aggregation for efficient time-series computation.

| Function | SQL Technique | What It Computes |
|----------|---------------|------------------|
| `daily_returns()` | `LAG()` window function | Day-over-day % change |
| `moving_averages()` | `AVG() OVER (ROWS BETWEEN)` | 50-day and 200-day MAs |
| `bollinger_bands()` | `STDDEV_POP() OVER ()` | ±2σ bands around 50-day MA |
| `cross_signals()` | `LAG()` + conditional logic | Golden Cross / Death Cross dates |
| `rolling_volatility()` | `STDDEV_POP() OVER ()` | 30-day annualized volatility |
| `max_drawdown()` | `MAX() OVER (UNBOUNDED)` | Running peak, drawdown % |
| `monthly_returns()` | CTEs + `LAG()` | Month-over-month returns |
| `yearly_returns()` | CTEs + `LAG()` | Year-over-year returns |
| `backtest_ma_crossover()` | `EXP(SUM(LN()))` | Strategy equity curve ($10K start) |
| `buy_and_hold()` | `EXP(SUM(LN()))` | Benchmark equity curve |

---

## ⚙️ Performance: Sequential vs Parallel

The synthetic data generator supports two modes to demonstrate Python concurrency patterns:

| Mode | DataFrame | Concurrency | DuckDB Ingestion |
|------|-----------|-------------|------------------|
| Sequential | Pandas | Single-threaded | Copy (Pandas → Arrow internally) |
| Parallel | PyArrow | `ProcessPoolExecutor` (8 workers) | Zero-copy (Arrow-native) |

**Why it matters:** Python's GIL means threads share one CPU core for Python bytecode. `ProcessPoolExecutor` spawns **separate processes** — true parallelism for CPU-bound work like data generation. PyArrow tables are DuckDB's native format, eliminating the data copy overhead that Pandas requires.

---

## 📊 Visualization

Charts are generated with a dark-mode theme and saved to `output/`:

- **Price with MAs & Bollinger Bands** — close price, 50/200-day MAs, ±2σ bands
- **30-Day Rolling Volatility** — annualized volatility over time
- **Drawdown from ATH** — percentage drop from running peak
- **Equity Curves** — MA crossover strategy vs buy-and-hold benchmark
- **Monthly Returns** — color-coded bar chart (green = positive, red = negative)
- **Yearly Returns** — annual performance summary

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `duckdb` | Embedded analytical database |
| `eodhd` | EODHD financial data API client |
| `pandas` | Data manipulation and cleaning |
| `numpy` | Numerical computation (GBM simulation) |
| `pyarrow` | Zero-copy DuckDB ingestion |
| `polars` | High-performance DataFrames (optional) |
| `matplotlib` | Chart generation |
| `python-dotenv` | Environment variable management |

---

## 🔗 Acknowledgements

This project was built from scratch, drawing concepts and inspiration from:

- [**financial-time-series-workshop**](https://github.com/tobalo/financial-time-series-workshop) by [@tobalo](https://github.com/tobalo) — DuckDB pipelines, GBM synthetic data, PyArrow zero-copy patterns, and performance benchmarking
- [**SP500-Analytics-Project**](https://github.com/yjiang1003/SP500-Analytics-Project) by [@yjiang1003](https://github.com/yjiang1003) — SQL window functions, CTEs, moving average crossover backtesting, and equity curve comparison

---

## 📄 License

MIT
