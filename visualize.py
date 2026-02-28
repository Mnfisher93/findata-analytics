"""
Visualization Module
Generates publication-quality financial charts from analytics DataFrames.
All charts are saved to the output/ directory.
"""

import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for saving to files
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Style
plt.rcParams.update({
    "figure.facecolor": "#0d1117",
    "axes.facecolor": "#0d1117",
    "axes.edgecolor": "#30363d",
    "axes.labelcolor": "#c9d1d9",
    "text.color": "#c9d1d9",
    "xtick.color": "#8b949e",
    "ytick.color": "#8b949e",
    "grid.color": "#21262d",
    "grid.alpha": 0.6,
    "font.family": "sans-serif",
    "font.size": 10,
})


def _save(fig, name: str):
    """Save figure to output/ directory."""
    path = os.path.join(OUTPUT_DIR, f"{name}.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  📊 Saved: {path}")


def plot_price_with_bollinger(df: pd.DataFrame, title: str = "Price with MAs & Bollinger Bands"):
    """Plot close price, 50/200-day MAs, and Bollinger Bands."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    fig, ax = plt.subplots(figsize=(14, 6))

    # Bollinger Bands fill
    ax.fill_between(
        df["date"], df["bb_lower"], df["bb_upper"],
        color="#58a6ff", alpha=0.08, label="Bollinger Bands (±2σ)"
    )

    # Price line
    ax.plot(df["date"], df["close"], color="#58a6ff", linewidth=1, label="Close Price", zorder=2)

    # 50-day MA
    ax.plot(df["date"], df["ma_50"], color="#f0883e", linewidth=1.5, label="50-Day MA", zorder=3)

    # 200-day MA
    valid_200 = df["ma_200"].notna()
    ax.plot(
        df.loc[valid_200, "date"], df.loc[valid_200, "ma_200"],
        color="#3fb950", linewidth=2, label="200-Day MA", zorder=4
    )

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price ($)")
    ax.legend(loc="upper left", fontsize=9, facecolor="#161b22", edgecolor="#30363d")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()

    _save(fig, "price_bollinger")


def plot_rolling_volatility(df: pd.DataFrame, title: str = "30-Day Rolling Volatility (Annualized)"):
    """Plot annualized rolling volatility."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["ann_volatility"].notna()]

    fig, ax = plt.subplots(figsize=(14, 4))

    ax.plot(df["date"], df["ann_volatility"] * 100, color="#f78166", linewidth=1)
    ax.fill_between(df["date"], 0, df["ann_volatility"] * 100, color="#f78166", alpha=0.15)

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Volatility (%)")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()

    _save(fig, "rolling_volatility")


def plot_drawdown(df: pd.DataFrame, title: str = "Drawdown from All-Time High"):
    """Plot drawdown percentage from running peak."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    fig, ax = plt.subplots(figsize=(14, 4))

    ax.fill_between(df["date"], df["drawdown_pct"], 0, color="#f85149", alpha=0.4)
    ax.plot(df["date"], df["drawdown_pct"], color="#f85149", linewidth=0.8)

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Drawdown (%)")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()

    _save(fig, "drawdown")


def plot_equity_curves(
    strategy_df: pd.DataFrame,
    bnh_df: pd.DataFrame,
    title: str = "Equity Curves: MA Crossover vs Buy & Hold",
):
    """Plot strategy equity curve against buy-and-hold benchmark."""
    strategy_df = strategy_df.copy()
    bnh_df = bnh_df.copy()
    strategy_df["date"] = pd.to_datetime(strategy_df["date"])
    bnh_df["date"] = pd.to_datetime(bnh_df["date"])

    fig, ax = plt.subplots(figsize=(14, 6))

    ax.plot(bnh_df["date"], bnh_df["bnh_equity"], color="#58a6ff", linewidth=1.5, label="Buy & Hold")
    ax.plot(
        strategy_df["date"], strategy_df["strategy_equity"],
        color="#3fb950", linewidth=1.5, label="MA Crossover Strategy"
    )

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value ($)")
    ax.legend(loc="upper left", fontsize=10, facecolor="#161b22", edgecolor="#30363d")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()

    _save(fig, "equity_curves")


def plot_monthly_returns(df: pd.DataFrame, title: str = "Month-over-Month Returns"):
    """Plot monthly returns as a bar chart."""
    df = df.copy()
    df["period"] = pd.to_datetime(
        df["yr"].astype(str) + "-" + df["mo"].astype(str).str.zfill(2) + "-01"
    )

    fig, ax = plt.subplots(figsize=(14, 4))

    colors = ["#3fb950" if x >= 0 else "#f85149" for x in df["monthly_return_pct"]]
    ax.bar(df["period"], df["monthly_return_pct"], width=20, color=colors, alpha=0.8)

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Return (%)")
    ax.grid(True, alpha=0.3, axis="y")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45)

    _save(fig, "monthly_returns")


def plot_yearly_returns(df: pd.DataFrame, title: str = "Year-over-Year Returns"):
    """Plot yearly returns as a bar chart."""
    fig, ax = plt.subplots(figsize=(10, 5))

    colors = ["#3fb950" if x >= 0 else "#f85149" for x in df["yearly_return_pct"]]
    ax.bar(df["yr"].astype(str), df["yearly_return_pct"], width=0.6, color=colors, alpha=0.8)

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Return (%)")
    ax.grid(True, alpha=0.3, axis="y")
    plt.xticks(rotation=45)

    _save(fig, "yearly_returns")
