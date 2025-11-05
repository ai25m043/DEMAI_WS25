# streamlit/app.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, bindparam, Boolean
from sqlalchemy.dialects.postgresql import ARRAY, TEXT as PG_TEXT

# ==============================
# Config & DB
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

# ---- optional auto-refresh every 30s (if package is present) ----
try:
    from streamlit_autorefresh import st_autorefresh
    st.sidebar.info("🔄 Auto-refresh is ON (30s)")
    st_autorefresh(interval=30_000, key="auto_refresh")
except Exception:
    st.sidebar.caption("Auto-refresh disabled (install streamlit-autorefresh to enable)")

tab1, tab2 = st.tabs(["🌍 Wikimedia Topics", "💹 Stock Prices"])

# ==============================
# TAB 1: Wikimedia Topics
# ==============================
with tab1:
    def load_topics_df() -> pd.DataFrame:
        try:
            with engine.begin() as conn:
                df = pd.read_sql("SELECT key, enabled FROM wm_topic ORDER BY key", conn)
            if df.empty:
                return pd.DataFrame({"key": DEFAULT_TOPICS, "enabled": [True]*len(DEFAULT_TOPICS)})
            return df
        except Exception:
            return pd.DataFrame({"key": DEFAULT_TOPICS, "enabled": [True]*len(DEFAULT_TOPICS)})

    topics_df = load_topics_df()
    enabled_default = topics_df.loc[topics_df["enabled"] == True, "key"].tolist()
    sel_topics = st.multiselect("Topics", topics_df["key"].tolist(), default=enabled_default)
    lookback_min = st.slider("Lookback (minutes)", 1, 180, 30, 1)

    # Parametric query with proper SQLAlchemy types
    q_rc = text("""
        SELECT dt, wiki, title, user_text, type, added, removed, topics
        FROM wm_recent_change
        WHERE dt >= now() - (:mins || ' minutes')::interval
          AND (:topics_is_empty OR topics && :topics)
        ORDER BY dt DESC
        LIMIT 500
    """).bindparams(
        bindparam("mins", type_=PG_TEXT),
        bindparam("topics_is_empty", type_=Boolean()),
        bindparam("topics", type_=ARRAY(PG_TEXT)),
    )

    params = {
        "mins": str(int(lookback_min)),
        "topics_is_empty": len(sel_topics) == 0,
        "topics": sel_topics if sel_topics else [],  # ARRAY(TEXT)
    }

    with engine.begin() as conn:
        df = pd.read_sql(q_rc, conn, params=params)

    if df.empty:
        st.info("No recent changes matched your filters.")
    else:
        if "dt" in df.columns:
            df["dt"] = pd.to_datetime(df["dt"], errors="coerce")

        left, right = st.columns(2)
        with left:
            st.metric("Rows", f"{len(df):,}")
        with right:
            total_added = int(pd.to_numeric(df.get("added", pd.Series([], dtype="Int64")), errors="coerce").fillna(0).sum())
            total_removed = int(pd.to_numeric(df.get("removed", pd.Series([], dtype="Int64")), errors="coerce").fillna(0).sum())
            st.metric("Net change (chars)", f"{(total_added - total_removed):,}")

        st.subheader("Latest changes")
        st.dataframe(
            df[["dt", "wiki", "title", "user_text", "type", "added", "removed", "topics"]],
            use_container_width=True,
            hide_index=True,
        )

        # Distribution by topic (current view)
        try:
            df_topics = df.copy().explode("topics").dropna(subset=["topics"])
            counts = df_topics["topics"].value_counts().rename_axis("topic").reset_index(name="count")
            if not counts.empty:
                st.subheader("Events by topic")
                st.bar_chart(counts.set_index("topic"))
        except Exception:
            pass

    st.caption("Data flow: Wikimedia RecentChange → Kafka → Spark → PostgreSQL")

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
        default_symbols = ["AAPL"] if "AAPL" in tickers_df["symbol"].values else [tickers_df["symbol"].iloc[0]]
        sel_symbols = st.multiselect(
            "Select one or more tickers to display:",
            options=tickers_df["symbol"].tolist(),
            default=default_symbols,
        )

        if sel_symbols:
            # IN list via ANY(:symbols) with ARRAY(TEXT)
            q_prices = text("""
                SELECT t.symbol, sv.ts_utc, sv.close
                FROM stock_value_1m sv
                JOIN ticker t ON t.id = sv.ticker_id
                WHERE t.symbol = ANY(:symbols)
                ORDER BY t.symbol, sv.ts_utc
                LIMIT 20000
            """).bindparams(bindparam("symbols", type_=ARRAY(PG_TEXT)))

            with engine.begin() as conn:
                df_stock = pd.read_sql(q_prices, conn, params={"symbols": sel_symbols})

            if df_stock.empty:
                st.info("No stock data found yet.")
            else:
                # Normalize types
                df_stock["ts_utc"] = pd.to_datetime(df_stock["ts_utc"], errors="coerce")
                df_stock = df_stock.dropna(subset=["ts_utc"]).sort_values(["symbol", "ts_utc"])

                st.subheader("Stock price trends (latest records)")
                chart_data = df_stock.pivot(index="ts_utc", columns="symbol", values="close")
                st.line_chart(chart_data, use_container_width=True)

                # Latest prices per symbol (handle None safely)
                latest = (
                    df_stock.sort_values("ts_utc")
                            .groupby("symbol", as_index=False)
                            .tail(1)
                            .set_index("symbol")[["close"]]
                            .copy()
                )
                latest["close"] = pd.to_numeric(latest["close"], errors="coerce")

                st.subheader("Latest prices")
                st.dataframe(
                    latest.assign(close=latest["close"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")),
                    use_container_width=True
                )

    st.caption("Data source: yfinance → PostgreSQL")
