#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

root = Path(__file__).resolve().parents[1]
outputs = root / "outputs"
img_dir = root / "docs" / "kpi_images"
img_dir.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 200,
    "savefig.bbox": "tight",
})

def render_table(df: pd.DataFrame, title: str, outfile: Path, max_rows: int = 20):
    if df is None or df.empty:
        fig, ax = plt.subplots(figsize=(6, 1))
        ax.axis('off')
        ax.text(0.0, 0.5, f"{title}\nNo data available.", fontsize=10, va='center')
        fig.savefig(outfile)
        plt.close(fig)
        return
    df2 = df.head(max_rows)
    ncols = len(df2.columns)
    nrows = len(df2.index)
    width = max(6, min(16, ncols * 1.2))
    height = max(1.5, min(20, 1 + nrows * 0.35))
    fig, ax = plt.subplots(figsize=(width, height))
    ax.axis('off')
    table = ax.table(cellText=df2.values, colLabels=list(df2.columns), loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.2)
    ax.set_title(title, fontsize=12, pad=10)
    fig.savefig(outfile)
    plt.close(fig)

# Load sources (prefer Pandas CSVs for consistent formatting; fall back to SQL JSONs)
# Repeat Customers
rc_csv = outputs / "pandas_repeat_customers.csv"
if rc_csv.exists():
    rc_df = pd.read_csv(rc_csv)
else:
    with open(outputs / "sql_repeat_customers.json") as f:
        rc_df = pd.DataFrame(json.load(f))
render_table(rc_df, "Repeat Customers", img_dir / "repeat_customers.png")

# Monthly Trends
mt_csv = outputs / "pandas_monthly_trends.csv"
if mt_csv.exists():
    mt_df = pd.read_csv(mt_csv)
else:
    with open(outputs / "sql_monthly_trends.json") as f:
        mt_df = pd.DataFrame(json.load(f))
render_table(mt_df, "Monthly Order Trends", img_dir / "monthly_trends.png")

# Regional Revenue
rr_csv = outputs / "pandas_regional_revenue.csv"
if rr_csv.exists():
    rr_df = pd.read_csv(rr_csv)
else:
    with open(outputs / "sql_regional_revenue.json") as f:
        rr_df = pd.DataFrame(json.load(f))
render_table(rr_df, "Regional Revenue", img_dir / "regional_revenue.png")

# Top Spenders
ts_csv = outputs / "pandas_top_spenders.csv"
if ts_csv.exists():
    ts_df = pd.read_csv(ts_csv)
else:
    with open(outputs / "sql_top_spenders.json") as f:
        ts_df = pd.DataFrame(json.load(f))
render_table(ts_df, "Top Spenders (Last 30 Days)", img_dir / "top_spenders.png")