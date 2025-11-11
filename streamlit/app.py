# streamlit/app.py
import os
import json
from functools import lru_cache

import pandas as pd
import streamlit as st
import pydeck as pdk
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
# Page & Auto-refresh
# ==============================
st.set_page_config(page_title="Wikimedia Live Map", layout="wide")
st.title("🌍 Wikimedia Live Events — Country Map")

# Sidebar auto-refresh controls
REFRESH_SECS = int(os.getenv("REFRESH_SECS", "30"))
enable_auto = False
refresh_counter = 0
try:
    from streamlit_autorefresh import st_autorefresh
    enable_auto = st.sidebar.toggle("🔄 Auto-refresh", value=True, help="Refresh the dashboard periodically")
    if enable_auto:
        st.sidebar.caption(f"Auto-refresh every {REFRESH_SECS}s")
        # returns an incrementing counter; we can use it to bust caches if needed
        refresh_counter = st_autorefresh(interval=REFRESH_SECS * 1000, key="auto_refresh")
except Exception:
    st.sidebar.caption("Auto-refresh package not installed (add streamlit-autorefresh)")

# ===========================================================
# Geo helpers (robust world GeoJSON + centroids)
# ===========================================================
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
    """Yield (lon,lat) for Polygon or MultiPolygon."""
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
    """Simple centroid: average of all polygon vertices (fast, robust)."""
    if not geometry:
        return None
    coords = geometry.get("coordinates")
    if not coords:
        return None
    xs = ys = n = 0
    for lon, lat in _flatten_coords(coords):
        xs += lon; ys += lat; n += 1
    return [xs / n, ys / n] if n else None

# Fallback centroids (lon, lat) for tricky/missing ISO2
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
    "TW": [120.9605, 23.6978], "XK": [20.9020, 42.6026],  # Kosovo (often missing)
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

# ===========================================================
# Controls
# ===========================================================
with st.sidebar:
    st.subheader("Filters")
    # Load topic toggles (fallback to defaults if table is empty/unavailable)
    def load_topics_df() -> pd.DataFrame:
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
    lookback_min = st.slider("Lookback (minutes)", 1, 180, 30, 1)

# ===========================================================
# Data
# ===========================================================
q_agg = text("""
    SELECT page_country_code AS iso2, COUNT(*)::int AS cnt
    FROM wm_recent_change
    WHERE dt >= now() - (:mins || ' minutes')::interval
      AND (:topics_is_empty OR topics && :topics)
      AND page_country_code IS NOT NULL
    GROUP BY page_country_code
""").bindparams(
    bindparam("mins", type_=PG_TEXT),
    bindparam("topics_is_empty", type_=Boolean()),
    bindparam("topics", type_=ARRAY(PG_TEXT)),
)
params = {
    "mins": str(int(lookback_min)),
    "topics_is_empty": len(sel_topics) == 0,
    "topics": sel_topics if sel_topics else [],
}
with engine.begin() as conn:
    by_country = pd.read_sql(q_agg, conn, params=params)

q_rc = text("""
    SELECT dt, wiki, title, user_text, type, added, removed, topics, page_country_code
    FROM wm_recent_change
    WHERE dt >= now() - (:mins || ' minutes')::interval
      AND (:topics_is_empty OR topics && :topics)
      AND page_country_code IS NOT NULL
    ORDER BY dt DESC
    LIMIT 500
""").bindparams(
    bindparam("mins", type_=PG_TEXT),
    bindparam("topics_is_empty", type_=Boolean()),
    bindparam("topics", type_=ARRAY(PG_TEXT)),
)
with engine.begin() as conn:
    df = pd.read_sql(q_rc, conn, params=params)

# ===========================================================
# Map
# ===========================================================
gj, feat_by_iso2, centroid_by_iso2 = world_index()
activity = {r["iso2"].upper(): int(r["cnt"]) for _, r in by_country.iterrows()} if not by_country.empty else {}
max_cnt = max(activity.values()) if activity else 0

def color_for(iso2: str):
    c = activity.get(iso2.upper(), 0)
    if c <= 0:
        return [220, 220, 220]
    t = min(1.0, c / max(1, max_cnt))
    r = int(255 * t + 80 * (1 - t))
    g = int(200 * (1 - 0.7 * t) + 60 * t)
    b = int(100 * (1 - t))
    return [r, g, b]

layers = []

# Polygons (colored by volume)
if gj and feat_by_iso2:
    colored_features = []
    for iso2, f in feat_by_iso2.items():
        props = f.get("properties", {}) or {}
        cnt = int(activity.get(iso2, 0))
        colored_features.append({
            "type": "Feature",
            "properties": {
                "iso2": iso2,
                "name": props.get("ADMIN") or props.get("COUNTRY") or props.get("NAME") or iso2,
                "count": cnt,
                "fill_color": color_for(iso2),
                "line_color": [80, 80, 80],
                "line_width": 50,
            },
            "geometry": f.get("geometry"),
        })
    geojson_data = {"type": "FeatureCollection", "features": colored_features}
    layers.append(pdk.Layer(
        "GeoJsonLayer",
        data=geojson_data,
        stroked=True,
        filled=True,
        pickable=True,
        get_fill_color="properties.fill_color",
        get_line_color="properties.line_color",
        get_line_width="properties.line_width",
        line_width_min_pixels=0.5,
    ))

# Markers (centroids)
markers = []
debug_rows = []
for iso2, cnt in activity.items():
    pos = centroid_by_iso2.get(iso2)
    debug_rows.append({"iso2": iso2, "count": cnt, "has_centroid": bool(pos)})
    if not pos:
        continue
    radius_px = min(6 + cnt * 2, 28)  # keep visible at global zoom
    markers.append({
        "iso2": iso2,
        "count": cnt,
        "position": pos,                 # [lon, lat]
        "radius": radius_px,             # pixels
        "fill_color": [255, 80, 80, 210],
        "line_color": [20, 20, 20],
        "line_width": 1,
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
    get_line_width="line_width",
))

view_state = pdk.ViewState(latitude=20, longitude=0, zoom=1.2)
tooltip = {
    "html": "<b>{properties.name}</b><br/>ISO2: {properties.iso2}<br/>Events: {properties.count}"
} if gj else {"text": "Events marker"}

st.pydeck_chart(pdk.Deck(
    initial_view_state=view_state,
    layers=layers,
    tooltip=tooltip,
), use_container_width=True)

# ===========================================================
# KPIs & Tables
# ===========================================================
left, right = st.columns(2)
with left:
    st.metric("Active countries", f"{len(activity):,}")
with right:
    st.metric("Events in window", f"{int(sum(activity.values())):,}")

if activity:
    dbg = pd.DataFrame(debug_rows).sort_values("count", ascending=False)
    st.caption("Centroid availability (for markers)")
    st.dataframe(dbg, use_container_width=True, hide_index=True)

st.subheader("Latest changes")
if not df.empty:
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    st.dataframe(
        df[["dt", "page_country_code", "wiki", "title", "user_text", "type", "added", "removed", "topics"]],
        use_container_width=True, hide_index=True
    )
    try:
        df_topics = df.copy().explode("topics").dropna(subset=["topics"])
        counts = df_topics["topics"].value_counts().rename_axis("topic").reset_index(name="count")
        if not counts.empty:
            st.subheader("Events by topic")
            st.bar_chart(counts.set_index("topic"))
    except Exception:
        pass
else:
    st.info("No recent changes matched your filters.")

st.caption("Data flow: Wikimedia → Kafka → Bridge (country) → Spark (topics) → PostgreSQL → Streamlit map")
