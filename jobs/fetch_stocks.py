#!/usr/bin/env python3
"""
Simple stock data fetcher: yfinance -> PostgreSQL
Fetches OHLCV data for all tickers and stores in stock_value_1m table.
"""

import os
import sys
import time
from datetime import datetime, timedelta
import traceback

import pandas as pd
import pytz
import psycopg2
from psycopg2.extras import execute_values
import yfinance as yf

# =========================================
# Configuration
# =========================================
PG_HOST = os.getenv("PG_HOST", "wm-postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "demai")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

INTERVALS = os.getenv("YF_INTERVALS", "1m,5m,1h").split(",")
INTERVALS = [x.strip() for x in INTERVALS if x.strip()]

FETCH_CYCLE_SECS = int(os.getenv("FETCH_CYCLE_SECS", "60"))

# How far back to fetch for each interval
LOOKBACK = {
    "1m": "7d",    # yfinance limit
    "2m": "60d",
    "5m": "60d",
    "15m": "60d",
    "30m": "60d",
    "1h": "730d",
    "1d": "5y",
}

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# =========================================
# Database Functions
# =========================================
def get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )

def load_tickers():
    """Load all tickers from database."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, symbol, tz FROM ticker ORDER BY symbol")
    tickers = cur.fetchall()
    cur.close()
    conn.close()
    
    result = [
        {"id": t[0], "symbol": t[1], "tz": t[2] or "UTC"}
        for t in tickers
    ]
    log(f"Loaded {len(result)} tickers from database")
    return result

def insert_stock_data(rows):
    """
    Bulk insert stock data into stock_value_1m table.
    Uses ON CONFLICT DO UPDATE to handle duplicates.
    
    rows: list of tuples (ticker_id, ts_utc, interval, open, high, low, close, volume, prepost, source, ingested_at, is_final)
    """
    if not rows:
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    sql = """
        INSERT INTO stock_value_1m 
            (ticker_id, ts_utc, interval, open, high, low, close, volume, prepost, source, ingested_at, is_final)
        VALUES %s
        ON CONFLICT (ticker_id, ts_utc, interval) 
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            is_final = EXCLUDED.is_final,
            ingested_at = EXCLUDED.ingested_at
    """
    
    execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    
    inserted = cur.rowcount
    cur.close()
    conn.close()
    
    return inserted

# =========================================
# yfinance Functions
# =========================================
def fetch_ticker_data(ticker_info, interval):
    """
    Fetch OHLCV data for a single ticker and interval.
    Returns list of rows ready for database insert.
    """
    ticker_id = ticker_info["id"]
    symbol = ticker_info["symbol"]
    tz_str = ticker_info["tz"]
    
    try:
        # Get yfinance ticker object
        ticker = yf.Ticker(symbol)
        
        # Fetch historical data
        period = LOOKBACK.get(interval, "60d")
        df = ticker.history(
            period=period,
            interval=interval,
            auto_adjust=True,
            prepost=True,
            repair=True
        )
        
        if df.empty:
            log(f"  {symbol} ({interval}): No data returned")
            return []
        
        # Reset index to get datetime as column
        df = df.reset_index()
        
        # yfinance uses "Date" for daily data, "Datetime" for intraday
        datetime_col = "Date" if "Date" in df.columns else "Datetime"
        
        # Convert to UTC
        tz = pytz.timezone(tz_str)
        if df[datetime_col].dt.tz is None:
            # Naive datetime - localize then convert to UTC
            df["ts_utc"] = df[datetime_col].apply(lambda dt: tz.localize(dt).astimezone(pytz.UTC))
        else:
            # Already timezone-aware
            df["ts_utc"] = df[datetime_col].dt.tz_convert(pytz.UTC)
        
        # Remove timezone info for PostgreSQL TIMESTAMPTZ (it handles TZ internally)
        df["ts_utc"] = df["ts_utc"].dt.tz_localize(None)
        
        # Determine if bars are final (not still forming)
        now_utc = datetime.utcnow()
        if interval in ("1m", "2m", "5m", "15m", "30m", "1h"):
            # Mark recent bars as non-final
            df["is_final"] = df["ts_utc"] < (now_utc - timedelta(minutes=2))
        else:
            df["is_final"] = True
        
        ingested_at = datetime.utcnow()
        
        # Build rows for insert
        rows = []
        for _, row in df.iterrows():
            rows.append((
                ticker_id,                          # ticker_id
                row["ts_utc"],                      # ts_utc
                interval,                           # interval
                float(row["Open"]) if pd.notna(row["Open"]) else None,    # open
                float(row["High"]) if pd.notna(row["High"]) else None,    # high
                float(row["Low"]) if pd.notna(row["Low"]) else None,      # low
                float(row["Close"]) if pd.notna(row["Close"]) else None,  # close
                int(row["Volume"]) if pd.notna(row["Volume"]) else None,  # volume
                False,                              # prepost (TODO: detect properly)
                "yfinance",                         # source
                ingested_at,                        # ingested_at
                row["is_final"]                     # is_final
            ))
        
        log(f"  {symbol} ({interval}): Fetched {len(rows)} bars")
        return rows
        
    except Exception as e:
        log(f"  {symbol} ({interval}): ERROR - {e}")
        return []

# =========================================
# Main Logic
# =========================================
def fetch_all_data():
    """Fetch data for all tickers and all intervals."""
    log("=" * 70)
    log("Starting fetch cycle")
    log(f"Intervals: {INTERVALS}")
    
    # Load tickers
    tickers = load_tickers()
    if not tickers:
        log("No tickers found in database")
        return
    
    # Fetch data for each ticker and interval
    all_rows = []
    for ticker_info in tickers:
        symbol = ticker_info["symbol"]
        log(f"Processing {symbol}...")
        
        for interval in INTERVALS:
            rows = fetch_ticker_data(ticker_info, interval)
            all_rows.extend(rows)
    
    # Insert all data
    if all_rows:
        log(f"Inserting {len(all_rows)} total rows into database...")
        inserted = insert_stock_data(all_rows)
        log(f"✓ Inserted/updated {inserted} rows")
    else:
        log("No data to insert")
    
    log("Fetch cycle complete")

def main():
    log("=" * 70)
    log("Stock Fetcher Starting")
    log("=" * 70)
    log(f"Python version: {sys.version}")
    log(f"Pandas version: {pd.__version__}")
    log(f"yfinance version: {yf.__version__}")
    log(f"Target: {PG_HOST}:{PG_PORT}/{PG_DB}")
    log(f"Intervals: {INTERVALS}")
    log(f"Fetch cycle: {FETCH_CYCLE_SECS} seconds")
    
    # Test database connection
    log("Testing database connection...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ticker")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        log(f"✓ Connected to database - found {count} tickers")
        
        if count == 0:
            log("WARNING: No tickers in database!")
            log("Add tickers with: INSERT INTO ticker (symbol, name, tz) VALUES ('AAPL', 'Apple Inc.', 'America/New_York');")
    except Exception as e:
        log(f"✗ Database connection failed: {e}")
        log("Waiting 30 seconds before retry...")
        time.sleep(30)
        return main()  # Retry
    
    # Main loop
    cycle = 0
    while True:
        cycle += 1
        log(f"\n{'=' * 70}")
        log(f"Cycle #{cycle} - {datetime.now()}")
        
        try:
            fetch_all_data()
        except Exception as e:
            log(f"ERROR in fetch cycle: {e}")
            traceback.print_exc()
        
        log(f"Sleeping {FETCH_CYCLE_SECS} seconds...")
        time.sleep(FETCH_CYCLE_SECS)

if __name__ == "__main__":
    main()