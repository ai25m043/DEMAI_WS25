# streamlit/app.py
# ================================================
# Wikimedia Events (by Topic) + Stocks (by Ticker)
# - Multi-select topics & time window
# - World map: red dots sized by event count
# - Events table
# - Multi-select tickers; show low/high/close + volume
# ================================================

import os
import json
from functools import lru_cache

import pandas as pd
import numpy as np
import streamlit as st
import pydeck as pdk
import altair as alt
from sqlalchemy import create_engine, text, bindparam, Boolean as SA_Boolean
from sqlalchemy.dialects.postgresql import ARRAY, TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# DB config
# -----------------------------
PGHOST = os.getenv("PGHOST", "wm-postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASS = os.getenv("PGPASSWORD", "postgres")
PGDB   = os.getenv("PGDATABASE", "demai")
ENGINE_URL = f"postgresql+psycopg2://{PGUSER}:{PGPASS}@{PGHOST}:{PGPORT}/{PGDB}"
engine = create_engine(ENGINE_URL, pool_pre_ping=True, future=True)

# -----------------------------
# Page
# -----------------------------
st.set_page_config(page_title="Wikimedia → Stocks Map", layout="wide")
st.title("🌍 Wikimedia edits → Stock prices")

# -----------------------------
# Sidebar controls
# -----------------------------
with st.sidebar:
    st.subheader("Filters")

    @st.cache_data(show_spinner=False)
    def load_topics() -> pd.DataFrame:
        try:
            with engine.begin() as conn:
                df = pd.read_sql("SELECT key, enabled FROM wm_topic ORDER BY key", conn)
            if df.empty:
                return pd.DataFrame(
                    {"key": ["finance","politics","economics","war","crypto"], "enabled":[True]*5}
                )
            return df
        except Exception:
            return pd.DataFrame(
                {"key": ["finance","politics","economics","war","crypto"], "enabled":[True]*5}
            )

    topics_df = load_topics()
    default_topics = topics_df.loc[topics_df["enabled"] == True, "key"].tolist()
    sel_topics = st.multiselect(
        "Topics",
        topics_df["key"].tolist(),
        default=default_topics,
        help="Zero = no topic filter (show all)"
    )

    lookback_min = st.slider("Lookback (minutes)", 5, 240, 60, 5)

    st.divider()
    st.subheader("Tickers")

    @st.cache_data(show_spinner=False)
    def load_tickers() -> pd.DataFrame:
        with engine.begin() as conn:
            df = pd.read_sql("SELECT symbol, name FROM ticker ORDER BY symbol", conn)
        return df

    tickers_df = load_tickers()
    sel_symbols = st.multiselect(
        "Symbols",
        tickers_df["symbol"].tolist(),
        default=[],
        help="Zero = no stock chart"
    )

# -----------------------------
# Geo helpers (centroids only)
# -----------------------------
CANDIDATE_GEOJSON_URLS = [
    "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson",
    "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json",
    "https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson",
]
ENV_OVERRIDE_URL = os.getenv("WORLD_GEOJSON_URL")

@st.cache_data(show_spinner=False)
def load_world_geojson() -> dict | None:
    import urllib.request
    urls = [ENV_OVERRIDE_URL] + CANDIDATE_GEOJSON_URLS if ENV_OVERRIDE_URL else CANDIDATE_GEOJSON_URLS
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            continue
    return None

def _flatten_coords(coords):
    if not coords: return
    first = coords[0]
    if not isinstance(first, (list, tuple)): return
    if isinstance(first[0], (float, int)): return
    if isinstance(first[0][0], (float, int)):  # Polygon
        for ring in coords:
            for lon, lat in ring:
                yield lon, lat
    else:  # MultiPolygon
        for poly in coords:
            for ring in poly:
                for lon, lat in ring:
                    yield lon, lat

def _mean_centroid(geometry):
    if not geometry: return None
    coords = geometry.get("coordinates")
    if not coords: return None
    xs = ys = n = 0
    for lon, lat in _flatten_coords(coords):
        xs += lon; ys += lat; n += 1
    return [xs / n, ys / n] if n else None

FALLBACK_CENTROIDS = {
    "US": [-98.5795, 39.8283], "CA": [-106.3468, 56.1304], "GB": [-3.4359, 55.3781],
    "FR": [2.2137, 46.2276],  "DE": [10.4515, 51.1657],   "IT": [12.5674, 41.8719],
    "ES": [-3.7492, 40.4637], "PL": [19.1451, 51.9194],   "SE": [18.6435, 60.1282],
    "NO": [8.4689, 60.4720],  "CH": [8.2275, 46.8182],    "AT": [14.5501, 47.5162],
    "NL": [5.2913, 52.1326],  "BE": [4.4699, 50.5039],    "IE": [-7.6921, 53.1424],
    "PT": [-8.2245, 39.3999], "GR": [21.8243, 39.0742],   "TR": [35.2433, 38.9637],
    "RU": [105.3188, 61.5240],"CN": [104.1954, 35.8617],  "JP": [138.2529, 36.2048],
    "KR": [127.7669, 35.9078],"IN": [78.9629, 20.5937],   "AU": [133.7751, -25.2744],
    "BR": [-51.9253, -14.2350],"AR": [-63.6167, -38.4161],"MX": [-102.5528, 23.6345],
    "ZA": [22.9375, -30.5595],"EG": [30.8025, 26.8206],   "SA": [45.0792, 23.8859],
    "AE": [54.3773, 23.4241], "SG": [103.8198, 1.3521],   "HK": [114.1694, 22.3193],
    "TW": [120.9605, 23.6978], "XK": [20.9020, 42.6026],
}

@lru_cache(maxsize=1)
def centroids_by_iso2() -> dict[str, list[float]]:
    gj = load_world_geojson()
    out = {}
    if gj:
        for f in gj.get("features", []):
            props = f.get("properties", {}) or {}
            iso2 = (props.get("ISO_A2") or props.get("ISO2") or props.get("iso_a2") or "").upper()
            if not iso2:
                continue
            c = _mean_centroid(f.get("geometry"))
            if c:
                out[iso2] = c
    for k, v in FALLBACK_CENTROIDS.items():
        out.setdefault(k, v)
    return out

# -----------------------------
# Queries (⚠️ Boolean types must be SA_Boolean())
# -----------------------------
Q_EVENTS = text("""
  SELECT
    dt,
    page_country_code::char(2) AS iso2,
    wiki, title, user_text, type, added, removed, topics
  FROM wm_recent_change
  WHERE dt >= now() - (:mins || ' minutes')::interval
    AND page_country_code IS NOT NULL
    AND (:topics_is_empty OR topics && :topics)
  ORDER BY dt DESC
  LIMIT 2000
""").bindparams(
    bindparam("mins", type_=PG_TEXT),
    bindparam("topics_is_empty", type_=SA_Boolean()),
    bindparam("topics", type_=ARRAY(PG_TEXT)),
)

Q_PRICES = text("""
  SELECT
    ti.symbol,
    sv.ts_utc,
    sv.open, sv.high, sv.low, sv.close, sv.volume
  FROM stock_value_1m sv
  JOIN ticker ti ON ti.id = sv.ticker_id
  WHERE sv.interval = '1m'
    AND sv.ts_utc >= now() AT TIME ZONE 'UTC' - (:mins || ' minutes')::interval
    AND (:symbols_is_empty OR ti.symbol = ANY(:symbols))
  ORDER BY sv.ts_utc ASC
""").bindparams(
    bindparam("mins", type_=PG_TEXT),
    bindparam("symbols_is_empty", type_=SA_Boolean()),
    bindparam("symbols", type_=ARRAY(PG_TEXT)),
)

# -----------------------------
# Load data
# -----------------------------
with st.spinner("Loading…"):
    try:
        with engine.begin() as conn:
            events = pd.read_sql(
                Q_EVENTS, conn,
                params={
                    "mins": str(lookback_min),
                    "topics_is_empty": len(sel_topics) == 0,
                    "topics": sel_topics if sel_topics else []
                }
            )
            prices = pd.DataFrame()
            if len(sel_symbols) > 0:
                prices = pd.read_sql(
                    Q_PRICES, conn,
                    params={
                        "mins": str(lookback_min),
                        "symbols_is_empty": False,
                        "symbols": sel_symbols
                    }
                )
            else:
                # still exercise the query with the boolean param
                _ = pd.read_sql(
                    Q_PRICES, conn,
                    params={
                        "mins": str(lookback_min),
                        "symbols_is_empty": True,
                        "symbols": []
                    }
                )
    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        st.stop()

# Normalize times
if not events.empty:
    events["dt"] = pd.to_datetime(events["dt"], errors="coerce", utc=True).dt.tz_convert("UTC")
if not prices.empty:
    prices["ts_utc"] = pd.to_datetime(prices["ts_utc"], errors="coerce", utc=True).dt.tz_convert("UTC")

# -----------------------------
# Map data (country dots)
# -----------------------------
st.subheader("World activity map")
markers = []
centroids = centroids_by_iso2()

if not events.empty:
    agg_cnt = (events.groupby("iso2", dropna=True)
                     .size()
                     .rename("count")
                     .reset_index())
    # distinct topics per country (for tooltip)
    expl = events[["iso2", "topics"]].explode("topics").dropna()
    topics_per_country = (expl.groupby("iso2")["topics"]
                              .unique()
                              .apply(list)
                              .to_dict())

    for _, row in agg_cnt.iterrows():
        iso2 = row["iso2"]
        pos = centroids.get(str(iso2))
        if not pos:
            continue
        cnt = int(row["count"])
        markers.append({
            "iso2": iso2,
            "count": cnt,
            "topics": ", ".join(sorted(topics_per_country.get(iso2, []))) if iso2 in topics_per_country else "",
            "position": pos,                # [lon, lat]
            "radius": min(6 + cnt * 2, 40),
            "fill_color": [255, 80, 80, 220],
            "line_color": [30, 30, 30],
        })

layer = pdk.Layer(
    "ScatterplotLayer",
    data=markers,
    pickable=True,
    get_position="position",
    get_radius="radius",
    radius_units="pixels",
    radius_min_pixels=3,
    radius_max_pixels=60,
    get_fill_color="fill_color",
    get_line_color="line_color",
)

tooltip = {
    "html": (
        "<b>{iso2}</b>"
        "<br/>Events: {count}"
        "<br/>Topics: {topics}"
    )
}

st.pydeck_chart(pdk.Deck(
    initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1.2),
    layers=[layer],
    tooltip=tooltip,
), use_container_width=True)

# KPIs
c1, c2 = st.columns(2)
with c1:
    st.metric("Countries with events", f"{events['iso2'].nunique() if not events.empty else 0}")
with c2:
    st.metric("Events in window", f"{len(events) if not events.empty else 0}")

# -----------------------------
# Events table
# -----------------------------
st.subheader("Filtered events")
if not events.empty:
    show_cols = ["dt", "iso2", "wiki", "title", "user_text", "type", "added", "removed", "topics"]
    st.dataframe(events[show_cols], use_container_width=True, hide_index=True)
else:
    st.info("No events for the selected window/topics.")

# -----------------------------
# Stock charts (low/high/close + volume)
# -----------------------------
st.subheader("Stocks (selected tickers)")
if not prices.empty:
    y_min = prices["low"].min() * 0.99  # 1 % tiefer
    y_max = prices["high"].max() * 1.01  # 1 % höher
    # Price band (low↔high) + close line
    price_chart = alt.layer(
        alt.Chart(prices).mark_area(opacity=0.15).encode(
            x=alt.X("ts_utc:T", title="Time (UTC)"),
            y=alt.Y("low:Q", title="Price", scale=alt.Scale(domain=[y_min, y_max])),
            y2="high:Q",
            color=alt.Color("symbol:N", legend=alt.Legend(title="Symbol"))
        ),
        alt.Chart(prices).mark_line().encode(
            x="ts_utc:T",
            y=alt.Y("close:Q", title="Close", scale=alt.Scale(domain=[y_min, y_max])),
            color="symbol:N"
        )
    ).properties(height=280).resolve_scale(y='independent')

    st.altair_chart(price_chart, use_container_width=True)

# --------------------------------
# Percentage change variant (Δ %)
# --------------------------------
if not prices.empty:
    prices_sorted = prices.sort_values("ts_utc")

    # Prozentuale Veränderung relativ zum ersten Close pro Symbol
    first_close = prices_sorted.groupby("symbol")["close"].transform("first")
    prices_pct = prices_sorted.assign(
        p_low   = (prices_sorted["low"]   / first_close - 1) * 100,
        p_high  = (prices_sorted["high"]  / first_close - 1) * 100,
        p_close = (prices_sorted["close"] / first_close - 1) * 100,
    )

    y_min_p = float(prices_pct["p_low"].min()) * 1.05
    y_max_p = float(prices_pct["p_high"].max()) * 1.05

    ref_rule = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(strokeDash=[4,4]).encode(y="y:Q")

    pct_chart = alt.layer(
        alt.Chart(prices_pct).mark_area(opacity=0.15).encode(
            x=alt.X("ts_utc:T", title="Time (UTC)"),
            y=alt.Y("p_low:Q", title="Change (%)", scale=alt.Scale(domain=[y_min_p, y_max_p])),
            y2="p_high:Q",
            color=alt.Color("symbol:N", legend=alt.Legend(title="Symbol"))
        ),
        alt.Chart(prices_pct).mark_line().encode(
            x="ts_utc:T",
            y=alt.Y("p_close:Q", title="Change (%)", scale=alt.Scale(domain=[y_min_p, y_max_p])),
            color="symbol:N"
        ),
        ref_rule
    ).properties(
        height=260,
        title="Normalized change since start (%)"
    )

    st.altair_chart(pct_chart, use_container_width=True)

    # Volume chart
    vol_chart = alt.Chart(prices).mark_bar(opacity=0.4).encode(
        x=alt.X("ts_utc:T", title="Time (UTC)"),
        y=alt.Y("volume:Q", title="Volume"),
        color=alt.Color("symbol:N", legend=alt.Legend(title="Symbol"))
    ).properties(height=160)

    st.altair_chart(vol_chart, use_container_width=True)
else:
    st.caption("Pick one or more symbols to see price/volume charts.")

st.caption("Flow: Wikimedia → Kafka → Bridge (adds country) → Spark (adds topics) → PostgreSQL → Streamlit")
