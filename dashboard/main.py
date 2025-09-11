# dashboard/main.py

import streamlit as st
import pymongo
import pandas as pd
from datetime import datetime

# --- Configuration ---
MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "wikipedia"
COLLECTION_NAME = "page_scores"

# --- Connect to MongoDB ---
@st.cache_resource
def get_collection():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]

collection = get_collection()

# --- Streamlit UI ---
st.set_page_config(page_title="Wikipedia Controversy Monitor", layout="wide")
st.title("📚 Wikipedia Controversy Monitor")

# --- Sidebar Filters ---
st.sidebar.header("Filters")
min_score = st.sidebar.slider("Minimum Controversy Score", 0.0, 10.0, 1.0, 0.1)

# --- Query data ---
@st.cache_data(ttl=30)
def load_data():
    cursor = collection.find({"controversy_score": {"$gte": min_score}}).sort("controversy_score", -1).limit(100)
    data = list(cursor)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["window_start"] = pd.to_datetime(df["window_start"])
    df["window_end"] = pd.to_datetime(df["window_end"])
    return df

df = load_data()

# --- Display ---
if df.empty:
    st.warning("No controversial pages found with the selected filters.")
else:
    st.subheader("🔥 Top Controversial Pages")
    st.dataframe(df[["title", "num_edits", "num_editors", "num_reverts", "controversy_score"]], use_container_width=True)

    st.subheader("📈 Score over Time (select a page)")
    page_options = df["title"].unique().tolist()
    selected_title = st.selectbox("Select a page to view time trend:", page_options)

    page_df = df[df["title"] == selected_title].sort_values("window_start")
    st.line_chart(
        page_df.set_index("window_start")[["controversy_score"]],
        use_container_width=True
    )