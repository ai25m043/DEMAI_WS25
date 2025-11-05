#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_stocks.py
---------------
Fetch recent OHLCV for tickers stored in PostgreSQL using PySpark + yfinance
and upsert them into stock_value_1m.

Key fixes vs previous version:
- Create 'ts' deterministically from index BEFORE lowercasing (handles 'Datetime'/'Date').
- Flatten potential MultiIndex columns from yfinance (('Open', ''), etc).
- Strong per-symbol logging with sample rows.
- Safe conversions + smaller batch flush for visibility.
"""

import os
from typing import Iterable, List, Tuple

# --- yfinance runtime toggles (must be set before import yfinance happens in workers) ---
os.environ.setdefault("YF_USE_CURL", "0")      # avoid curl_cffi cookie DB contention
os.environ.setdefault("YF_MAX_WORKERS", "1")   # also pass threads=False to yf

import pandas as pd
from pyspark.sql import SparkSession, functions as F

# -----------------------------
# Environment
# -----------------------------
APP_NAME = os.getenv("APP_NAME", "stocks-fetcher")

PG_HOST = os.getenv("PG_HOST", "wm-postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "demai")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?stringtype=unspecified"

T_TICKER = os.getenv("T_TICKER", "ticker")
T_VALUES = os.getenv("T_VALUES", "stock_value_1m")

# Spark partitions for the symbol list (tune if needed)
SYMBOL_PARTITIONS = int(os.getenv("SYMBOL_PARTITIONS", "8"))

# Intervals to try (ladder). You can narrow to e.g. "1d,1h,15m" to debug faster.
TARGET_INTERVALS = [i.strip() for i in os.getenv("YF_INTERVALS", "1m,2m,5m,15m,1h,1d").split(",") if i.strip()]

# Map interval -> valid Yahoo "period"
PERIOD_BY_INTERVAL = {
    "1m":  "5d",
    "2m":  "5d",
    "5m":  "1mo",
    "15m": "1mo",
    "30m": "1mo",
    "1h":  "3mo",
    "1d":  "1y",
}

# Some suffixes dislike ultra-fine intervals (ladder overrides)
FALLBACK_BY_SUFFIX = {
    ".DE":  ["2m","5m","15m","1h","1d"],
    ".PA":  ["2m","5m","15m","1h","1d"],
    ".AS":  ["2m","5m","15m","1h","1d"],
    ".L":   ["2m","5m","15m","1h","1d"],
    ".SW":  ["2m","5m","15m","1h","1d"],
    ".ST":  ["2m","5m","15m","1h","1d"],
    ".HK":  ["5m","15m","1h","1d"],
    ".KS":  ["5m","15m","1h","1d"],
    ".AX":  ["5m","15m","1h","1d"],
    ".SI":  ["5m","15m","1h","1d"],
    ".JO":  ["5m","15m","1h","1d"],
    ".SA":  ["5m","15m","1h","1d"],
    ".TO":  ["2m","5m","15m","1h","1d"],
    ".NS":  ["2m","5m","15m","1h","1d"],
}

# Debug helper: run only one symbol (set DRY_RUN_ONE=SYMBOL in env)
DRY_RUN_ONE = os.getenv("DRY_RUN_ONE", "").strip() or None


def choose_intervals(symbol: str) -> List[str]:
    for suf, ladder in FALLBACK_BY_SUFFIX.items():
        if symbol.endswith(suf):
            return [i for i in TARGET_INTERVALS if i in ladder]
    return TARGET_INTERVALS


def period_for_interval(interval: str) -> str:
    return PERIOD_BY_INTERVAL.get(interval, "1mo")


# -----------------------------
# Spark session
# -----------------------------
spark = (
    SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# -----------------------------
# Load tickers (id, symbol)
# -----------------------------
tickers_df = (
    spark.read
         .format("jdbc")
         .option("url", JDBC_URL)
         .option("dbtable", T_TICKER)
         .option("user", PG_USER)
         .option("password", PG_PASS)
         .option("driver", "org.postgresql.Driver")
         .load()
         .select(F.col("id").cast("int").alias("ticker_id"),
                 F.col("symbol").cast("string").alias("symbol"))
)

if DRY_RUN_ONE:
    tickers_df = tickers_df.where(F.col("symbol") == F.lit(DRY_RUN_ONE))

tickers_df = tickers_df.orderBy("symbol")
count_tickers = tickers_df.count()
print(f"[stocks] ticker count = {count_tickers}")
tickers_df.show(10, truncate=False)

if count_tickers == 0:
    print("[stocks] no tickers found. Check table 'ticker'.")
    spark.stop()
    raise SystemExit(0)

tickers_df = tickers_df.repartition(min(SYMBOL_PARTITIONS, max(1, count_tickers)))


# -----------------------------
# Partition writer
# -----------------------------
def fetch_and_upsert_partition(rows: Iterable[Tuple[int, str]]):
    import os
    import time
    from datetime import timezone

    import psycopg2
    from psycopg2.extras import execute_values
    import yfinance as yf
    import numpy as np
    import pandas as pd

    os.environ["YF_USE_CURL"] = "0"   # double-enforce in worker

    pid = os.getpid()
    print(f"[stocks][pid={pid}] partition START")

    # DB setup
    conn = None
    upserted_total = 0

    def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Flatten possible MultiIndex columns from yfinance."""
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join([str(x) for x in tup if str(x)]).strip() for tup in df.columns]
        else:
            df.columns = [str(c) for c in df.columns]
        return df

    def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
        """
        1) Ensure index is UTC datetime, name it 'ts'.
        2) reset_index() so we have a proper 'ts' column.
        3) Lower-case and sanitize column names.
        4) Keep only ts/open/high/low/close/volume.
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df = flatten_columns(df)

        # Make sure index -> UTC datetime and becomes 'ts'
        try:
            df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
        except Exception:
            pass
        df.index.name = "ts"

        # Pandas >=1.4 supports names="ts"; the generic path also works
        try:
            df = df.reset_index(names="ts")
        except TypeError:
            df = df.reset_index()

        # normalize columns AFTER reset_index so 'ts' is deterministic
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        # standardize possible yfinance variants (e.g., 'adj_close', 'close')
        rename_map = {
            "adj_close": "adj_close",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
        # keep what's present
        keep = [c for c in ["ts", "open", "high", "low", "close", "volume", "adj_close"] if c in df.columns]
        df = df[keep].copy()

        # Prefer 'close' over 'adj_close' for storage; if only adj_close exists, map it to close.
        if "close" not in df.columns and "adj_close" in df.columns:
            df["close"] = df["adj_close"]
        if "adj_close" in df.columns:
            df = df.drop(columns=["adj_close"])

        # Types & NaNs
        for c in ["open", "high", "low", "close"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

        # Drop rows without ts
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            df = df[df["ts"].notna()]
        else:
            # Shouldn't happen, but keep contract
            return pd.DataFrame()

        return df

    try:
        conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
        cur = conn.cursor()
        batch = []
        FLUSH_EVERY = 500  # small for debugging visibility

        for ticker_id, symbol in rows:
            last_err = None
            got_rows = 0
            chosen = choose_intervals(symbol)

            for interval in chosen:
                try:
                    df = yf.download(
                        tickers=symbol,
                        period=period_for_interval(interval),
                        interval=interval,
                        auto_adjust=False,
                        prepost=True,
                        threads=False,   # important when YF_MAX_WORKERS=1
                        progress=False,
                        timeout=20,
                    )

                    df = normalize_frame(df)
                    if df.empty or "ts" not in df.columns:
                        # For debugging: show what we got originally (shape & columns)
                        if isinstance(df, pd.DataFrame):
                            print(f"[stocks][pid={pid}] {symbol}: got data but no usable rows after normalize; shape={df.shape}")
                        continue

                    # Log a tiny preview
                    cols = [c for c in ["ts","open","high","low","close","volume"] if c in df.columns]
                    print(f"[stocks][pid={pid}] {symbol}: interval={interval} cols={cols} sample:")
                    print(df[cols].head(3).to_string(index=False))

                    added = 0
                    for _, r in df.iterrows():
                        ts = r["ts"]
                        if pd.isna(ts):
                            continue
                        o = None if "open" not in r or pd.isna(r["open"]) else float(r["open"])
                        h = None if "high" not in r or pd.isna(r["high"]) else float(r["high"])
                        l = None if "low"  not in r or pd.isna(r["low"])  else float(r["low"])
                        c = None if "close" not in r or pd.isna(r["close"]) else float(r["close"])
                        v = None
                        if "volume" in r and pd.notna(r["volume"]):
                            try:
                                v = int(r["volume"])
                            except Exception:
                                v = None

                        batch.append((
                            int(ticker_id),
                            ts.to_pydatetime().replace(tzinfo=timezone.utc),
                            o, h, l, c, v, False, "yfinance"
                        ))
                        added += 1

                    if added > 0:
                        got_rows += added
                        print(f"[stocks][pid={pid}] {symbol}: interval={interval} rows={added}")
                        break  # stop ladder once an interval succeeded

                except Exception as e:
                    last_err = e
                    print(f"[stocks][pid={pid}] {symbol}: interval={interval} ERROR -> {repr(e)}")
                    time.sleep(0.2)
                    continue

            if got_rows == 0:
                if last_err is not None:
                    print(f"[stocks][pid={pid}] {symbol}: NO DATA from {chosen}. last_err={repr(last_err)}")
                else:
                    print(f"[stocks][pid={pid}] {symbol}: NO DATA (no error) from {chosen}")

            # flush intermittently
            if len(batch) >= FLUSH_EVERY:
                try:
                    execute_values(cur, f"""
                        INSERT INTO {T_VALUES}
                          (ticker_id, ts_utc, open, high, low, close, volume, prepost, source)
                        VALUES %s
                        ON CONFLICT (ticker_id, ts_utc) DO UPDATE
                        SET open=EXCLUDED.open,
                            high=EXCLUDED.high,
                            low=EXCLUDED.low,
                            close=EXCLUDED.close,
                            volume=EXCLUDED.volume,
                            prepost=EXCLUDED.prepost,
                            source=EXCLUDED.source
                    """, batch)
                    conn.commit()
                    upserted_total += len(batch)
                    print(f"[stocks][pid={pid}] UPSERT flush: {len(batch)} rows (total {upserted_total})")
                except Exception as sql_err:
                    print(f"[stocks][pid={pid}] UPSERT ERROR: {sql_err}")
                finally:
                    batch.clear()

        # final flush
        if batch:
            try:
                execute_values(cur, f"""
                    INSERT INTO {T_VALUES}
                      (ticker_id, ts_utc, open, high, low, close, volume, prepost, source)
                    VALUES %s
                    ON CONFLICT (ticker_id, ts_utc) DO UPDATE
                    SET open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        volume=EXCLUDED.volume,
                        prepost=EXCLUDED.prepost,
                        source=EXCLUDED.source
                """, batch)
                conn.commit()
                upserted_total += len(batch)
                print(f"[stocks][pid={pid}] UPSERT final: {len(batch)} rows (total {upserted_total})")
            except Exception as sql_err:
                print(f"[stocks][pid={pid}] UPSERT ERROR (final): {sql_err}")
            finally:
                batch.clear()

    except Exception as e:
        print(f"[stocks][pid={pid}] partition error: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        print(f"[stocks][pid={pid}] partition END, total upserted={upserted_total}")


# Kick off
pairs = tickers_df.select("ticker_id", "symbol").rdd.map(lambda r: (r["ticker_id"], r["symbol"]))
pairs.foreachPartition(fetch_and_upsert_partition)

print("[stocks] driver DONE")
spark.stop()
