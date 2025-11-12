# streamlit/app.py
# ===============================
# Wikimedia topic activity → Stocks (per-country)
# Choropleth color = corr(events_t, returns_t+lag)
# Marker size = events in last 10 minutes
# ===============================

import os
import json
from functools import lru_cache

import pandas as pd
import numpy as np
import streamlit as st
import pydeck as pdk
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY, TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError

# ---------------------------------
# Config
# ---------------------------------
PGHOST = os.getenv("PGHOST", "wm-postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASS = os.getenv("PGPASSWORD", "postgres")
PGDB   = os.getenv("PGDATABASE", "demai")

ENGINE_URL = f"postgresql+psycopg2://{PGUSER}:{PGPASS}@{PGHOST}:{PGPORT}/{PGDB}"
engine = create_engine(ENGINE_URL, pool_pre_ping=True, future=True)

DEFAULT_TOPICS = ["crypto", "economics", "finance", "politics", "war"]

st.set_page_config(page_title="Wikimedia → Stocks (by Country)", layout="wide")
st.title("🌍 Wikimedia → Stocks (Topic Influence Map)")

# ---------------------------------
# Sidebar controls
# ---------------------------------
with st.sidebar:
    st.subheader("Controls")

    REFRESH_SECS = int(os.getenv("REFRESH_SECS", "30"))
    try:
        from streamlit_autorefresh import st_autorefresh
        enable_auto = st.toggle("🔄 Auto-refresh", value=True, help=f"Refresh every {REFRESH_SECS}s")
        if enable_auto:
            st.caption(f"Auto-refresh every {REFRESH_SECS}s")
            st_autorefresh(interval=REFRESH_SECS * 1000, key="auto_refresh")
    except Exception:
        st.caption("Install `streamlit-autorefresh` to enable auto-refresh")

    # Topics from DB (fallback to defaults)
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
    available_topics = topics_df["key"].tolist()
    default_index = available_topics.index("finance") if "finance" in available_topics else 0
    topic = st.selectbox("Topic", available_topics, index=default_index, help="Wikimedia topic to match in title/comment")

    lookback_min = st.slider("Lookback window (minutes)", 5, 240, 60, 5)
    lag_min = st.slider("Lag (events → returns), minutes", 0, 60, 10, 1,
                        help="Compare events at t with returns at t+lag")

    st.divider()
    st.caption("Color = correlation (blue negative → red positive). Size = events in last 10 minutes.")

# ---------------------------------
# Geo helpers (robust world GeoJSON + centroids)
# ---------------------------------
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
    if not coords:
        return
    first = coords[0]
    if not isinstance(first, (list, tuple)):
        return
    if isinstance(first[0], (float, int)):
        return
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
    if not geometry:
        return None
    coords = geometry.get("coordinates")
    if not coords:
        return None
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
def world_index():
    gj = load_world_geojson()
    feat_by_iso2, centroid_by_iso2 = {}, {}
    if gj:
        for f in gj.get("features", []):
            props = f.get("properties", {}) or {}
            iso2 = (props.get("ISO_A2") or props.get("ISO2") or props.get("iso_a2") or "").upper()
            if not iso2:
                continue
            feat_by_iso2[iso2] = f
            c = _mean_centroid(f.get("geometry"))
            if c:
                centroid_by_iso2[iso2] = c
    for k, v in FALLBACK_CENTROIDS.items():
        centroid_by_iso2.setdefault(k, v)
    return gj, feat_by_iso2, centroid_by_iso2

# ---------------------------------
# Queries
# ---------------------------------
Q_EVENTS = text("""
  SELECT
    date_trunc('minute', dt AT TIME ZONE 'UTC') AS ts_minute_utc,
    page_country_code::char(2)                  AS iso2,
    COUNT(*)::int                               AS event_count
  FROM wm_recent_change
  WHERE dt >= now() AT TIME ZONE 'UTC' - (:mins || ' minutes')::interval
    AND page_country_code IS NOT NULL
    AND topics && :topics
  GROUP BY 1,2
""").bindparams(
    bindparam("mins", type_=PG_TEXT),
    bindparam("topics", type_=ARRAY(PG_TEXT)),
)

Q_RETURNS = text("""
  SELECT iso2::char(2) AS iso2, ts_minute_utc, avg_return_1m
  FROM v_country_return_minute
  WHERE ts_minute_utc >= now() AT TIME ZONE 'UTC' - (:mins || ' minutes')::interval
""").bindparams(
    bindparam("mins", type_=PG_TEXT),
)

Q_LATEST_EVENTS = text("""
  SELECT dt, page_country_code AS iso2, wiki, title, user_text, type, added, removed, topics
  FROM wm_recent_change
  WHERE dt >= now() - (:mins || ' minutes')::interval
    AND page_country_code IS NOT NULL
    AND topics && :topics
  ORDER BY dt DESC
  LIMIT 500
""").bindparams(
    bindparam("mins", type_=PG_TEXT),
    bindparam("topics", type_=ARRAY(PG_TEXT)),
)

# ---------------------------------
# Time helpers: normalize tz to UTC-naive everywhere
# ---------------------------------
def to_utc_naive(series: pd.Series) -> pd.Series:
    if series.dtype == "datetime64[ns, UTC]":
        return series.dt.tz_convert("UTC").dt.tz_localize(None)
    # strings/object → parse first
    if series.dtype == "object":
        s = pd.to_datetime(series, errors="coerce", utc=True)
        return s.dt.tz_convert("UTC").dt.tz_localize(None)
    # naive → ensure datetime then leave naive (assume already UTC)
    if str(series.dtype) == "datetime64[ns]":
        return pd.to_datetime(series, errors="coerce")
    # anything else → coerce
    s = pd.to_datetime(series, errors="coerce", utc=True)
    return s.dt.tz_convert("UTC").dt.tz_localize(None)

# ---------------------------------
# Data loading
# ---------------------------------
with st.spinner("Loading data…"):
    try:
        with engine.begin() as conn:
            ev = pd.read_sql(Q_EVENTS, conn, params={"mins": str(lookback_min), "topics": [topic]})
            rets = pd.read_sql(Q_RETURNS, conn, params={"mins": str(lookback_min)})
            latest = pd.read_sql(Q_LATEST_EVENTS, conn, params={"mins": str(lookback_min), "topics": [topic]})
    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        st.stop()

# Normalize timestamps to UTC-naive BEFORE any merges / comparisons
if not ev.empty:
    ev["ts_minute_utc"] = to_utc_naive(ev["ts_minute_utc"])
if not rets.empty:
    rets["ts_minute_utc"] = to_utc_naive(rets["ts_minute_utc"])
if not latest.empty:
    latest["dt"] = to_utc_naive(latest["dt"])

# ---------------------------------
# Build per-country lagged correlation
# ---------------------------------
if not ev.empty and not rets.empty:
    df = (ev.merge(rets, on=["iso2", "ts_minute_utc"], how="outer")
            .sort_values(["iso2", "ts_minute_utc"]))
    df["event_count"] = df["event_count"].fillna(0)

    def _lag_country(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("ts_minute_utc")
        if lag_min > 0:
            g["ret_lagged"] = g["avg_return_1m"].shift(-lag_min)
        else:
            g["ret_lagged"] = g["avg_return_1m"]
        return g

    df = df.groupby("iso2", group_keys=False).apply(_lag_country)

    def _corr_safe(x: pd.Series, y: pd.Series) -> float:
        x = x.astype(float)
        y = y.astype(float)
        m = (~x.isna()) & (~y.isna())
        if m.sum() < 3:
            return np.nan
        try:
            return float(pd.Series(x[m]).corr(pd.Series(y[m])))
        except Exception:
            return np.nan

    corr = (df.groupby("iso2")
              .apply(lambda g: _corr_safe(g["event_count"], g["ret_lagged"]))
              .rename("corr")
              .reset_index())

    # Recent activity window (10m) — use UTC-naive now
    # Build a UTC-naive cutoff timestamp (minute-aligned)
    cutoff = (
        pd.Timestamp.now(tz="UTC")   # tz-aware UTC
        .floor("T")                # align to minute
        .tz_convert(None)          # drop tz → naive
    )

    # Ensure the series is real datetime64[ns] (naive) and compare
    ev["ts_minute_utc"] = pd.to_datetime(ev["ts_minute_utc"], utc=False, errors="coerce")
    recent = (
        ev[ev["ts_minute_utc"] >= cutoff]
        .groupby("iso2")["event_count"]
        .sum()
        .rename("events_10m")
        .reset_index()
    )


    map_df = corr.merge(recent, on="iso2", how="outer")
    map_df["corr"] = map_df["corr"].fillna(0.0)
    map_df["events_10m"] = map_df["events_10m"].fillna(0).astype(int)
else:
    df = pd.DataFrame(columns=["iso2", "ts_minute_utc", "event_count", "avg_return_1m", "ret_lagged"])
    map_df = pd.DataFrame(columns=["iso2", "corr", "events_10m"])

# ---------------------------------
# Map rendering
# ---------------------------------
gj, feat_by_iso2, centroid_by_iso2 = world_index()
corr_by_iso = dict(zip(map_df["iso2"], map_df["corr"]))
evt_by_iso  = dict(zip(map_df["iso2"], map_df["events_10m"]))

def color_corr(iso2: str):
    c = float(corr_by_iso.get(iso2.upper(), 0.0))
    t = max(-1.0, min(1.0, c))
    if t >= 0:
        r = int(200 + 55 * t)
        g = int(200 * (1 - t) + 40 * t)
        b = int(200 * (1 - t) + 40 * t)
    else:
        t = abs(t)
        r = int(200 * (1 - t) + 40 * t)
        g = int(200 * (1 - t) + 80 * t)
        b = int(200 + 55 * t)
    return [r, g, b]

layers = []

if gj and feat_by_iso2:
    features = []
    for iso2, f in feat_by_iso2.items():
        props = f.get("properties", {}) or {}
        name = props.get("ADMIN") or props.get("COUNTRY") or props.get("NAME") or iso2
        features.append({
            "type": "Feature",
            "properties": {
                "iso2": iso2,
                "name": name,
                "corr": float(corr_by_iso.get(iso2, 0.0)),
                "events": int(evt_by_iso.get(iso2, 0)),
                "fill_color": color_corr(iso2),
            },
            "geometry": f.get("geometry"),
        })
    layers.append(pdk.Layer(
        "GeoJsonLayer",
        data={"type": "FeatureCollection", "features": features},
        stroked=True, filled=True, pickable=True,
        get_fill_color="properties.fill_color",
        get_line_color=[60, 60, 60],
        line_width_min_pixels=0.5,
    ))

markers = []
for iso2, cnt in evt_by_iso.items():
    pos = centroid_by_iso2.get(iso2)
    if not pos:
        continue
    radius = min(8 + int(cnt) * 2, 40)
    markers.append({
        "iso2": iso2,
        "count": int(cnt),
        "position": pos,
        "radius": radius,
        "fill_color": [255, 120, 120, 220],
        "line_color": [30, 30, 30],
    })
layers.append(pdk.Layer(
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
))

tooltip = {
    "html": (
        "<b>{properties.name}</b>"
        "<br/>ISO2: {properties.iso2}"
        "<br/>Corr (events→ret): {properties.corr}"
        "<br/>Events (10m): {properties.events}"
    )
} if gj else {"text": "Events marker"}

st.pydeck_chart(pdk.Deck(
    initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1.2),
    layers=layers,
    tooltip=tooltip,
), use_container_width=True)

# ---------------------------------
# KPIs
# ---------------------------------
k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Active countries", f"{len({k for k,v in evt_by_iso.items() if v>0}):,}")
with k2:
    st.metric("Events in last 10m", f"{int(sum(evt_by_iso.values())):,}")
with k3:
    mean_abs_corr = float(np.nanmean(np.abs(map_df["corr"])) if not map_df.empty else 0.0)
    st.metric("Mean |corr|", f"{mean_abs_corr:.3f}")

# ---------------------------------
# Drill-down
# ---------------------------------
st.subheader("Drill-down: aligned time series")
if not df.empty:
    iso_options = sorted(df["iso2"].dropna().unique().tolist())
    sel_iso = st.selectbox("Country (ISO2)", iso_options, index=iso_options.index("US") if "US" in iso_options else 0)

    g = df[df["iso2"] == sel_iso].sort_values("ts_minute_utc").copy()
    if not g.empty:
        e = g["event_count"].astype(float)
        g["events_norm"] = (e - e.min()) / (e.max() - e.min() + 1e-9)
        plot_df = g.set_index("ts_minute_utc")[["events_norm", "avg_return_1m"]]
        st.line_chart(plot_df, use_container_width=True)
        cval = g.dropna(subset=["ret_lagged"])
        corr_val = cval["event_count"].corr(cval["ret_lagged"]) if not cval.empty else np.nan
        st.caption(f"Pearson corr (events@t vs returns@t+{lag_min}m) for {sel_iso}: **{(corr_val if pd.notna(corr_val) else 0):.3f}**")
    else:
        st.info("No data for selected country in current window.")
else:
    st.info("Not enough data to compute correlations. Try a longer lookback or another topic.")

# ---------------------------------
# Latest matching events
# ---------------------------------
st.subheader("Latest matching events")
if not latest.empty:
    st.dataframe(
        latest[["dt", "iso2", "wiki", "title", "user_text", "type", "added", "removed", "topics"]],
        use_container_width=True, hide_index=True
    )
    ev_min = (latest.assign(ts=lambda d: d["dt"].dt.floor("T"))
                      .groupby("ts").size().rename("events").reset_index())
    if not ev_min.empty:
        st.caption("Events per minute (table sample)")
        st.bar_chart(ev_min.set_index("ts"))
else:
    st.caption("No recent events matched the selected topic and window.")

st.caption("Data flow: Wikimedia → Kafka → Bridge (country) → Spark (topics) → PostgreSQL → Streamlit")
