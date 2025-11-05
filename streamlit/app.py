# streamlit/app.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# ==============================
# Database setup
# ==============================
PGHOST = os.getenv("PGHOST", "wm-postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASS = os.getenv("PGPASSWORD", "postgres")
PGDB   = os.getenv("PGDATABASE", "demai")

ENGINE_URL = f"postgresql+psycopg2://{PGUSER}:{PGPASS}@{PGHOST}:{PGPORT}/{PGDB}"
engine = create_engine(ENGINE_URL, pool_pre_ping=True)

DEFAULT_TOPICS = ["crypto", "economics", "finance", "politics", "war"]

# ==============================
# Streamlit Layout
# ==============================
st.set_page_config(page_title="Wikimedia + Stocks Dashboard", layout="wide")
st.title("📡 Wikimedia Live Feed & 📈 Stock Monitor")

tab1, tab2 = st.tabs(["🌍 Wikimedia Topics", "💹 Stock Prices"])

# ==============================
# TAB 1: Wikimedia Topics
# ==============================
with tab1:
    def load_topics_df():
        try:
            with engine.begin() as conn:
                df = pd.read_sql("SELECT key, enabled FROM wm_topic ORDER BY key", conn)
            if df.empty:
                return pd.DataFrame({"key": DEFAULT_TOPICS, "enabled": [True] * len(DEFAULT_TOPICS)})
            return df
        except Exception:
            return pd.DataFrame({"key": DEFAULT_TOPICS, "enabled": [True] * len(DEFAULT_TOPICS)})

    topics_df = load_topics_df()
    enabled_default = topics_df.loc[topics_df["enabled"] == True, "key"].tolist()
    sel_topics = st.multiselect("Topics", topics_df["key"].tolist(), default=enabled_default)
    lookback_min = st.slider("Lookback (minutes)", min_value=1, max_value=180, value=30, step=1)

    topics_literal = "{" + ",".join(sel_topics) + "}" if sel_topics else "{}"
    mins = int(lookback_min)

    sql = f"""
    SELECT dt, wiki, title, user_text, type, added, removed, topics
    FROM wm_recent_change
    WHERE dt >= now() - interval '{mins} minutes'
      AND ('{topics_literal}' = '{{}}' OR topics && '{topics_literal}'::text[])
    ORDER BY dt DESC
    LIMIT 500
    """

    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        st.info("No recent changes matched your filters.")
    else:
        if "dt" in df.columns:
            df["dt"] = pd.to_datetime(df["dt"])

        left, right = st.columns(2)
        with left:
            st.metric("Rows", f"{len(df):,}")
        with right:
            total_added = int(df.get("added", pd.Series(dtype=int)).sum())
            total_removed = int(df.get("removed", pd.Series(dtype=int)).sum())
            st.metric("Net change (chars)", f"{(total_added - total_removed):,}")

        st.subheader("Latest changes")
        st.dataframe(
            df[["dt", "wiki", "title", "user_text", "type", "added", "removed", "topics"]],
            use_container_width=True,
            hide_index=True,
        )

        try:
            df_topics = df.copy().explode("topics").dropna(subset=["topics"])
            counts = df_topics["topics"].value_counts().rename_axis("topic").reset_index(name="count")
            if not counts.empty:
                st.subheader("Events by topic (current view)")
                st.bar_chart(counts.set_index("topic"))
        except Exception:
            pass

    st.caption("Data source: Wikimedia RecentChange stream → Kafka → Spark → PostgreSQL")


# ==============================
# TAB 2: Stocks
# ==============================
with tab2:
    st.header("💹 Live Stock Prices (1-minute updates)")

    try:
        with engine.begin() as conn:
            tickers_df = pd.read_sql("SELECT symbol, name FROM ticker ORDER BY symbol", conn)
    except Exception as e:
        st.error(f"Could not load tickers: {e}")
        tickers_df = pd.DataFrame(columns=["symbol", "name"])

    if tickers_df.empty:
        st.warning("No tickers found in database. Please seed the ticker table first.")
    else:
        sel_symbols = st.multiselect(
            "Select one or more tickers to display:",
            options=tickers_df["symbol"].tolist(),
            default=["AAPL"] if "AAPL" in tickers_df["symbol"].values else [tickers_df["symbol"].iloc[0]],
        )

        if sel_symbols:
            placeholders = ", ".join([f"'{s}'" for s in sel_symbols])
            sql = f"""
            SELECT t.symbol, sv.ts_utc, sv.close
            FROM stock_value_1m sv
            JOIN ticker t ON t.id = sv.ticker_id
            WHERE t.symbol IN ({placeholders})
            ORDER BY t.symbol, sv.ts_utc
            LIMIT 2000
            """

            with engine.begin() as conn:
                df_stock = pd.read_sql(sql, conn)

            if df_stock.empty:
                st.info("No stock data found yet.")
            else:
                df_stock["ts_utc"] = pd.to_datetime(df_stock["ts_utc"])
                st.subheader("Stock price trends (last records)")
                chart_data = df_stock.pivot(index="ts_utc", columns="symbol", values="close")
                st.line_chart(chart_data, use_container_width=True)

                # Show current prices
                latest = (
                    df_stock.sort_values("ts_utc")
                            .groupby("symbol")
                            .tail(1)
                            .set_index("symbol")[["close"]]
                )
                st.subheader("Latest prices")
                st.table(latest.style.format({"close": "{:.2f}"}))

    st.caption("Data source: yfinance (1-minute bars) → PostgreSQL")
