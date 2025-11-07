#!/usr/bin/env python3
import io
import json
import tempfile
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

# Local imports
from src.processing import PandasAnalytics, SQLAnalytics
from src.database import DatabaseManager, DataLoader
from src.config import Config

st.set_page_config(page_title="Akasa Air - Data Engineering Dashboard", layout="wide")

SAMPLE_CUSTOMERS = Path("data/customers.csv")
SAMPLE_ORDERS = Path("data/orders.xml")

@st.cache_data(show_spinner=False)
def load_pandas_results(customers_path: str, orders_path: str):
    # Override config paths at runtime
    Config.CUSTOMERS_CSV_PATH = customers_path
    Config.ORDERS_XML_PATH = orders_path

    pa = PandasAnalytics()
    df_customers, df_orders = pa.load_data()

    repeat_customers = pa.get_repeat_customers()
    monthly = pa.get_monthly_order_trends()
    regional = pa.get_regional_revenue()
    top_spenders = pa.get_top_spenders(days=30, limit=10)

    return {
        "customers": df_customers,
        "orders": df_orders,
        "repeat_customers": repeat_customers,
        "monthly": monthly,
        "regional": regional,
        "top_spenders": top_spenders,
    }


def to_period_str(df: pd.DataFrame) -> pd.DataFrame:
    if {"year", "month"}.issubset(df.columns):
        df = df.copy()
        df["period"] = df["year"].astype(int).astype(str) + "-" + df["month"].astype(int).astype(str).str.zfill(2)
        return df
    return df


def save_uploaded(file, suffix: str) -> Path:
    tmpdir = Path(tempfile.gettempdir()) / "akasa_streamlit"
    tmpdir.mkdir(parents=True, exist_ok=True)
    out = tmpdir / f"upload_{datetime.now().timestamp()}{suffix}"
    out.write_bytes(file.read())
    return out


st.title("Akasa Air – Data Engineering Dashboard")
st.markdown("Run the pipeline in-memory (Pandas) with sample or uploaded data. Optionally, run SQL KPIs against a MySQL database.")

with st.sidebar:
    st.header("Data Sources")
    use_sample = st.toggle("Use bundled sample data", value=True)
    cu_file = None
    or_file = None
    if not use_sample:
        cu_file = st.file_uploader("Upload customers.csv", type=["csv"])
        or_file = st.file_uploader("Upload orders.xml", type=["xml"])
    st.divider()
    st.header("Options")
    run_sql = st.checkbox("Also run SQL KPIs (requires reachable MySQL)", value=False)

# Resolve paths
if use_sample:
    customers_path = str(SAMPLE_CUSTOMERS)
    orders_path = str(SAMPLE_ORDERS)
else:
    if cu_file is None or or_file is None:
        st.info("Upload both customers.csv and orders.xml to proceed, or enable 'Use bundled sample data'.")
        st.stop()
    saved_csv = save_uploaded(cu_file, ".csv")
    saved_xml = save_uploaded(or_file, ".xml")
    customers_path = str(saved_csv)
    orders_path = str(saved_xml)

run = st.button("Run Analysis", type="primary")

if run:
    with st.spinner("Running Pandas pipeline..."):
        try:
            results = load_pandas_results(customers_path, orders_path)
        except Exception as e:
            st.exception(e)
            st.stop()

    st.success("Pandas KPIs computed.")
    tabs = st.tabs(["Repeat Customers", "Monthly Trends", "Regional Revenue", "Top Spenders (30d)", "Raw Data"])

    with tabs[0]:
        st.subheader("Repeat Customers")
        st.dataframe(results["repeat_customers"], use_container_width=True)
        st.download_button("Download CSV", results["repeat_customers"].to_csv(index=False), file_name="pandas_repeat_customers.csv")

    with tabs[1]:
        st.subheader("Monthly Order Trends")
        monthly = to_period_str(results["monthly"]).copy()
        st.dataframe(monthly, use_container_width=True)
        if {"period", "order_count"}.issubset(monthly.columns):
            chart_df = monthly.set_index("period")["order_count"]
            st.bar_chart(chart_df)
        st.download_button("Download CSV", results["monthly"].to_csv(index=False), file_name="pandas_monthly_trends.csv")

    with tabs[2]:
        st.subheader("Regional Revenue")
        st.dataframe(results["regional"], use_container_width=True)
        if {"region", "total_revenue"}.issubset(results["regional"].columns):
            chart_df = results["regional"].set_index("region")["total_revenue"]
            st.bar_chart(chart_df)
        st.download_button("Download CSV", results["regional"].to_csv(index=False), file_name="pandas_regional_revenue.csv")

    with tabs[3]:
        st.subheader("Top Spenders (Last 30 Days)")
        st.dataframe(results["top_spenders"], use_container_width=True)
        st.download_button("Download CSV", results["top_spenders"].to_csv(index=False), file_name="pandas_top_spenders.csv")

    with tabs[4]:
        st.subheader("Raw Data")
        st.expander("Customers").dataframe(results["customers"], use_container_width=True)
        st.expander("Orders").dataframe(results["orders"], use_container_width=True)

    if run_sql:
        st.divider()
        st.subheader("SQL KPIs (MySQL)")
        st.caption("Provide database connection details for a reachable MySQL instance. On Streamlit Cloud, use st.secrets.")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            db_user = st.text_input("DB_USER", value=st.secrets.get("DB_USER", "root"))
        with col2:
            db_password = st.text_input("DB_PASSWORD", value=st.secrets.get("DB_PASSWORD", ""), type="password")
        with col3:
            db_host = st.text_input("DB_HOST", value=st.secrets.get("DB_HOST", "localhost"))
        with col4:
            db_port = st.text_input("DB_PORT", value=str(st.secrets.get("DB_PORT", "3306")))
        with col5:
            db_name = st.text_input("DB_NAME", value=st.secrets.get("DB_NAME", "akasa_air_db"))
        socket = st.text_input("DB_SOCKET (optional)", value=st.secrets.get("DB_SOCKET", ""))

        if st.button("Run SQL KPIs"):
            try:
                # Build database URL
                if socket:
                    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?unix_socket={socket}"
                else:
                    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

                # Override Config paths so DataLoader uses current data
                Config.CUSTOMERS_CSV_PATH = customers_path
                Config.ORDERS_XML_PATH = orders_path

                with st.spinner("Loading data into MySQL and computing SQL KPIs..."):
                    dbm = DatabaseManager(database_url=db_url)
                    dbm.initialize()
                    dbm.reset_database()
                    DataLoader(dbm).load_all_data()

                    sql = SQLAnalytics(dbm)
                    rc = sql.get_repeat_customers()
                    mt = sql.get_monthly_order_trends()
                    rr = sql.get_regional_revenue()
                    ts = sql.get_top_spenders(days=30, limit=10)

                st.success("SQL KPIs computed.")
                st.write("Repeat Customers (SQL)")
                st.dataframe(pd.DataFrame(rc))
                st.write("Monthly Order Trends (SQL)")
                st.dataframe(pd.DataFrame(mt))
                st.write("Regional Revenue (SQL)")
                st.dataframe(pd.DataFrame(rr))
                st.write("Top Spenders (SQL)")
                st.dataframe(pd.DataFrame(ts))
            except Exception as e:
                st.exception(e)
                
else:
    st.info("Click 'Run Analysis' to process the data.")