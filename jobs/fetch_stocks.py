#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_stocks.py
---------------
Minute OHLCV fetcher for symbols stored in 'ticker' and upserts into 'stock_value_1m'.

Design:
- Load all symbols from DB.
- Determine last stored ts_utc per (ticker_id, interval).
- For each symbol, fetch from last_ts (exclusive) or backfill window if empty.
- Normalize to UTC, ensure numeric types, and upsert in batches.
- Retry transient Yahoo/network errors a few times before skipping a symbol.

Environment (with defaults):
  PG_HOST=wm-postgres
  PG_PORT=5432
  PG_DB=demai
  PG_USER=postgres
  PG_PASS=postgres

  YF_INTERVAL=1m
  YF_BACKFILL_MIN=180          # minutes to backfill if symbol has no data yet
  YF_THREADS=1                 # yfinance internal threads; per-symbol loop anyway
  YF_PREPOST=false             # include pre/post market data
  YF_MAX_TRIES=3               # retry attempts per symbol
  YF_BATCH_UPSERT=2000         # rows per execute_values batch
"""

import os
import sys
import time
import math
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd
import psycopg2
import psycopg2.extras as pgx
import yfinance as yf

# --------------------------
# Logging
# --------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fetch_stocks")

# --------------------------
# Config
# --------------------------
PG_HOST = os.getenv("PG_HOST", "wm-postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "demai")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

YF_INTERVAL       = os.getenv("YF_INTERVAL", "1m")
YF_BACKFILL_MIN   = int(os.getenv("YF_BACKFILL_MIN", "180"))
YF_THREADS        = int(os.getenv("YF_THREADS", "1"))
YF_PREPOST        = os.getenv("YF_PREPOST", "false").lower() in ("1","true","yes")
YF_MAX_TRIES      = int(os.getenv("YF_MAX_TRIES", "3"))
YF_BATCH_UPSERT   = int(os.getenv("YF_BATCH_UPSERT", "2000"))

# keep yfinance calm in containers
os.environ.setdefault("YF_USE_CURL", "0")
os.environ.setdefault("YF_MAX_WORKERS", str(max(1, YF_THREADS)))

SOURCE_TAG = "yfinance"

# --------------------------
# DB helpers
# --------------------------
def connect_pg():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"
    return psycopg2.connect(dsn)

def fetch_tickers(conn) -> List[Tuple[int, str]]:
    """
    Returns list of (ticker_id, symbol).
    Only symbols that look presentable for Yahoo.
    """
    sql = """
        SELECT id, symbol
        FROM ticker
        WHERE symbol IS NOT NULL AND length(symbol) > 0
        ORDER BY id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]

def fetch_last_ts_map(conn, interval: str) -> Dict[int, Optional[datetime]]:
    """
    Returns {ticker_id: last_ts_utc_or_None} for given interval.
    """
    sql = """
        SELECT ticker_id, MAX(ts_utc) AS last_ts
        FROM stock_value_1m
        WHERE interval = %s
        GROUP BY ticker_id
    """
    out: Dict[int, Optional[datetime]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (interval,))
        for tid, last_ts in cur.fetchall():
            out[int(tid)] = last_ts  # tz-aware from timestamptz
    return out

def upsert_ohlcv_rows(conn, rows: List[Tuple]):
    """
    rows: list of tuples matching the INSERT column order below.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO stock_value_1m
            (ticker_id, ts_utc, interval, open, high, low, close, volume, prepost, source, is_final)
        VALUES %s
        ON CONFLICT (ticker_id, ts_utc, interval)
        DO UPDATE SET
            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume,
            prepost= EXCLUDED.prepost,
            source = EXCLUDED.source,
            ingested_at = now(),
            is_final = EXCLUDED.is_final
    """
    inserted = 0
    with conn.cursor() as cur:
        # chunk to avoid massive packets
        for i in range(0, len(rows), YF_BATCH_UPSERT):
            chunk = rows[i:i+YF_BATCH_UPSERT]
            pgx.execute_values(cur, sql, chunk, template=None, page_size=len(chunk))
            inserted += len(chunk)
    conn.commit()
    return inserted

# --------------------------
# yfinance helpers
# --------------------------
def safe_history(symbol: str, start_dt_utc: datetime, interval: str, prepost: bool, tries: int) -> pd.DataFrame:
    """
    Fetch history for a single symbol with retries.
    Returns a DataFrame with index as DatetimeIndex (tz-aware if Yahoo provides).
    """
    # yfinance sometimes ignores 'start' depending on interval; still best effort
    delay = 1.0
    for attempt in range(1, tries+1):
        try:
            # Use Ticker.history for clean per-symbol fetch
            tk = yf.Ticker(symbol)
            df = tk.history(
                start=start_dt_utc,  # UTC-aware dt ok
                interval=interval,
                actions=False,
                prepost=prepost,
                auto_adjust=False,
                repair=True,
                raise_errors=True,
            )
            return df
        except Exception as e:
            log.warning("[%s] history() attempt %d/%d failed: %s", symbol, attempt, tries, e)
            if attempt < tries:
                time.sleep(delay)
                delay = min(delay * 2.0, 8.0)
    # fallback to download() (rarely helps but try once)
    try:
        log.info("[%s] fallback to yf.download()", symbol)
        df = yf.download(
            tickers=symbol,
            start=start_dt_utc,
            interval=interval,
            group_by="column",   # single symbol -> flat columns
            prepost=prepost,
            auto_adjust=False,
            progress=False,
            repair=True,
        )
        return df
    except Exception as e:
        log.error("[%s] yf.download fallback failed: %s", symbol, e)
        return pd.DataFrame()

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only OHLCV columns, ensure tz-aware UTC index, coerce numerics.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    # Flatten potential weirdness
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([c for c in tup if c]) for tup in df.columns]
    # Try to select OHLCV columns under typical names
    candidates = {
        "Open":  "open",
        "High":  "high",
        "Low":   "low",
        "Close": "close",
        "Volume":"volume",
    }
    # handle lowercase variations
    lower_map = {c.lower(): c for c in df.columns}
    data = {}
    for yf_name, std in candidates.items():
        c = None
        if yf_name in df.columns:
            c = yf_name
        elif yf_name.lower() in lower_map:
            c = lower_map[yf_name.lower()]
        if c is not None:
            data[std] = pd.to_numeric(df[c], errors="coerce")
        else:
            data[std] = pd.Series([None] * len(df), index=df.index)

    out = pd.DataFrame(data, index=df.index)

    # Ensure index is tz-aware, convert to UTC
    idx = out.index
    if not isinstance(idx, pd.DatetimeIndex):
        # yfinance should always return DatetimeIndex; guard anyway
        out = out.reset_index().rename(columns={"index": "ts"})
        out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
        out = out.set_index("ts")
    else:
        if idx.tz is None:
            out.index = idx.tz_localize(timezone.utc)
        else:
            out.index = idx.tz_convert(timezone.utc)

    # Drop rows where all OHLC are NaN
    out = out.dropna(subset=["open","high","low","close"], how="all")
    # Keep at minute precision
    out.index = out.index.map(lambda x: x.replace(second=0, microsecond=0))
    out = out[~out.index.duplicated(keep="last")]
    return out.sort_index()

# --------------------------
# Main worker
# --------------------------
def main():
    log.info("Connecting to PostgreSQL at %s:%s (db=%s, user=%s)", PG_HOST, PG_PORT, PG_DB, PG_USER)
    try:
        conn = connect_pg()
    except Exception as e:
        log.error("Failed to connect to PostgreSQL: %s", e)
        sys.exit(2)

    try:
        tickers = fetch_tickers(conn)
    except Exception as e:
        log.error("Failed to read tickers: %s", e)
        conn.close()
        sys.exit(2)

    if not tickers:
        log.warning("No tickers found. Seed the 'ticker' table first.")
        conn.close()
        return

    log.info("Loaded %d tickers.", len(tickers))

    try:
        last_ts_map = fetch_last_ts_map(conn, YF_INTERVAL)
    except Exception as e:
        log.error("Failed to load last timestamps: %s", e)
        last_ts_map = {}

    now_utc = datetime.now(timezone.utc)

    total_rows = 0
    symbols_ok = 0
    symbols_err = 0

    for ticker_id, symbol in tickers:
        last_ts = last_ts_map.get(ticker_id)
        if last_ts is None:
            start_utc = now_utc - timedelta(minutes=YF_BACKFILL_MIN)
            log.info("[%s] backfill from %s → now (interval=%s, prepost=%s)",
                     symbol, start_utc.isoformat(), YF_INTERVAL, YF_PREPOST)
        else:
            # advance by one minute to avoid re-inserting the last bar
            start_utc = (last_ts + timedelta(minutes=1)).astimezone(timezone.utc)
            log.info("[%s] incremental from %s → now (interval=%s, prepost=%s)",
                     symbol, start_utc.isoformat(), YF_INTERVAL, YF_PREPOST)

        # Fetch with retries
        try:
            raw = safe_history(symbol, start_utc, YF_INTERVAL, YF_PREPOST, YF_MAX_TRIES)
        except Exception as e:
            log.error("[%s] unhandled exception during fetch: %s", symbol, e)
            symbols_err += 1
            continue

        if raw is None or raw.empty:
            log.info("[%s] no new rows returned.", symbol)
            symbols_ok += 1
            continue

        # Normalize
        try:
            df = normalize_df(raw)
        except Exception as e:
            log.error("[%s] normalize failed: %s", symbol, e)
            symbols_err += 1
            continue

        if df.empty:
            log.info("[%s] nothing to insert after normalization.", symbol)
            symbols_ok += 1
            continue

        # Build row tuples for upsert
        # (ticker_id, ts_utc, interval, open, high, low, close, volume, prepost, source, is_final)
        rows = []
        for ts, rec in df.iterrows():
            try:
                rows.append((
                    ticker_id,
                    ts,                     # tz-aware UTC
                    YF_INTERVAL,
                    None if pd.isna(rec["open"])  else float(rec["open"]),
                    None if pd.isna(rec["high"])  else float(rec["high"]),
                    None if pd.isna(rec["low"])   else float(rec["low"]),
                    None if pd.isna(rec["close"]) else float(rec["close"]),
                    None if pd.isna(rec["volume"]) else int(rec["volume"]),
                    bool(YF_PREPOST),
                    SOURCE_TAG,
                    True,  # is_final
                ))
            except Exception as e:
                log.warning("[%s] row build skipped for %s: %s", symbol, ts, e)

        if not rows:
            log.info("[%s] no valid rows after build; skipping upsert.", symbol)
            symbols_ok += 1
            continue

        # Upsert
        try:
            inserted = upsert_ohlcv_rows(conn, rows)
            total_rows += inserted
            symbols_ok += 1
            log.info("[%s] upserted %d rows. Sample: first=%s last=%s",
                     symbol, inserted, rows[0][1].isoformat(), rows[-1][1].isoformat())
        except Exception as e:
            conn.rollback()
            symbols_err += 1
            log.error("[%s] upsert failed (rolled back): %s", symbol, e)

        # small politeness delay if you want to avoid hammering
        if YF_THREADS <= 1:
            time.sleep(0.2)

    try:
        conn.close()
    except Exception:
        pass

    log.info("Done. Symbols OK=%d, Errors=%d, Rows upserted=%d",
             symbols_ok, symbols_err, total_rows)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        sys.exit(1)
    except Exception as e:
        log.exception("Fatal error: %s", e)
        sys.exit(2)
