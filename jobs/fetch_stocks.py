#!/usr/bin/env python3
import os, time, json
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

PG_HOST = os.getenv("PG_HOST", "wm-postgres")
PG_DB   = os.getenv("PG_DB", "demai")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

def get_tickers(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, symbol FROM ticker")
        return cur.fetchall()

def fetch_last_bars(symbols):
    df = yf.download(
        tickers=" ".join(symbols),
        interval="1m",
        period="2m",
        auto_adjust=False,
        prepost=True,
        group_by="ticker",
        progress=False
    )
    data = []
    if isinstance(df.columns, pd.MultiIndex):
        for sym in symbols:
            if (sym, "Close") not in df.columns:
                continue
            sdf = df[sym].dropna().reset_index()
            if sdf.empty:
                continue
            last = sdf.iloc[-1]
            ts = last["Datetime"].to_pydatetime().replace(tzinfo=timezone.utc)
            data.append((sym, ts, last["Open"], last["High"], last["Low"], last["Close"], int(last["Volume"])))
    else:
        last = df.dropna().reset_index().iloc[-1]
        ts = last["Datetime"].to_pydatetime().replace(tzinfo=timezone.utc)
        sym = symbols[0]
        data.append((sym, ts, last["Open"], last["High"], last["Low"], last["Close"], int(last["Volume"])))
    return data

def store_rows(conn, rows):
    with conn.cursor() as cur:
        sql = """
            INSERT INTO stock_value_1m (ticker_id, ts_utc, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (ticker_id, ts_utc) DO NOTHING
        """
        execute_values(cur, sql, rows)
    conn.commit()

def main():
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    tickers = get_tickers(conn)
    if not tickers:
        print("[stocks] no tickers found.")
        return

    while True:
        symbols = [s for _, s in tickers]
        print(f"[stocks] fetching 1m bars for {len(symbols)} tickers…")
        bars = fetch_last_bars(symbols)
        rows = []
        for sym, ts, o, h, l, c, v in bars:
            tid = next((t[0] for t in tickers if t[1] == sym), None)
            if tid:
                rows.append((tid, ts, o, h, l, c, v))
        if rows:
            store_rows(conn, rows)
            print(f"[stocks] inserted {len(rows)} rows at {datetime.now().isoformat()}")
        time.sleep(60)  # wait 1 minute

if __name__ == "__main__":
    main()
