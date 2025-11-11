-- ============================================
-- init.sql  (creates DB if missing + stocks + wikimedia; country + topics)
-- ============================================

-- =========================
-- 0) Create database if not exists (psql-friendly)
--    Requires psql (\gexec). If you can't use psql, see the dblink fallback below.
-- =========================
SELECT
  'CREATE DATABASE demai TEMPLATE template0 ENCODING ''UTF8'' LC_COLLATE ''en_US.utf8'' LC_CTYPE ''en_US.utf8'''
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'demai'
)\gexec

\connect demai

-- =========================
-- (Optional) 0b) Pure-SQL fallback using dblink (uncomment if not using psql).
-- Requires: superuser/appropriate privileges to CREATE EXTENSION dblink.
-- =========================
-- CREATE EXTENSION IF NOT EXISTS dblink;
-- DO $$
-- BEGIN
--   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'demai') THEN
--     PERFORM dblink_exec('dbname=' || current_database(),
--       'CREATE DATABASE demai TEMPLATE template0 ENCODING ''UTF8'' LC_COLLATE ''en_US.utf8'' LC_CTYPE ''en_US.utf8''' );
--   END IF;
-- END $$;
-- -- Now connect manually to the new DB (outside this DO block) before running the rest.

-- =========================
-- 1) Topics master (controls which topics to save)
-- =========================
CREATE TABLE IF NOT EXISTS wm_topic (
  id       SERIAL PRIMARY KEY,
  key      TEXT UNIQUE NOT NULL,
  label    TEXT NOT NULL,
  enabled  BOOLEAN NOT NULL DEFAULT TRUE,
  keywords TEXT[] NOT NULL DEFAULT '{}'
);

INSERT INTO wm_topic(key, label, keywords, enabled) VALUES
  ('other',     'Other',     ARRAY[]::TEXT[], FALSE), -- keep as reference but disabled
  ('finance',   'Finance',   ARRAY['finance','financial','bank','stock','market','inflation'], TRUE),
  ('politics',  'Politics',  ARRAY['politic','election','parliament','minister','party'], TRUE),
  ('economics', 'Economics', ARRAY['economics','gdp','unemployment','macro'], TRUE),
  ('war',       'War',       ARRAY['war','conflict','battle','invasion','military'], TRUE),
  ('crypto',    'Crypto',    ARRAY['crypto','bitcoin','ethereum','blockchain','token'], TRUE),
  ('technology','Technology',ARRAY['tech','software','cloud','semiconductor'], TRUE),
  ('healthcare','Healthcare',ARRAY['health','biotech','pharma','medical'], TRUE),
  ('energy',    'Energy',    ARRAY['energy','oil','gas','solar','wind'], TRUE),
  ('consumer',  'Consumer',  ARRAY['retail','consumer','ecommerce','beverage'], TRUE)
ON CONFLICT (key) DO UPDATE
SET label = EXCLUDED.label,
    keywords = EXCLUDED.keywords,
    enabled = EXCLUDED.enabled;

-- =========================
-- 2) Wikimedia recent changes (country + topics)
-- =========================
CREATE TABLE IF NOT EXISTS wm_recent_change (
  -- Identifiers & time
  meta_id            TEXT PRIMARY KEY,                -- meta.id
  dt                 TIMESTAMPTZ,                     -- meta.dt
  timestamp_unix     BIGINT,                          -- 'timestamp' (unix seconds)

  -- Source wiki context
  wiki               TEXT,                            -- short id like 'enwiki'
  server_name        TEXT,                            -- e.g. en.wikipedia.org
  server_url         TEXT,
  server_script_path TEXT,

  -- Page/change metadata
  page_id            BIGINT,
  title              TEXT,
  namespace          INT,
  type               TEXT,
  comment            TEXT,
  user_text          TEXT,
  bot                BOOLEAN,
  minor              BOOLEAN,
  patrolled          BOOLEAN,
  old_len            INT,
  new_len            INT,
  added              INT,
  removed            INT,

  -- Assigned topics (resolved by Spark; NEVER 'other' here)
  topics             TEXT[] NOT NULL DEFAULT '{}',

  -- Raw original JSON
  raw                JSONB,

  -- Geo / enrichment results
  page_qid           TEXT,                            -- Wikidata item for the page (QID)
  page_country_qid   TEXT,                            -- Wikidata QID of country
  page_country_code  CHAR(2),                         -- ISO alpha-2 (e.g., 'GB')
  geo_method         TEXT,                            -- e.g. 'P17','P131_chain','P276','P625_polygon'
  geo_confidence     SMALLINT,                        -- 0..100 (optional)
  page_lat           DOUBLE PRECISION,
  page_lon           DOUBLE PRECISION,
  page_geohash       TEXT,

  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wm_rc_dt           ON wm_recent_change (dt DESC);
CREATE INDEX IF NOT EXISTS brin_wm_rc_dt          ON wm_recent_change USING BRIN (dt);
CREATE INDEX IF NOT EXISTS idx_wm_rc_country      ON wm_recent_change (page_country_code);
CREATE INDEX IF NOT EXISTS idx_wm_rc_topics_gin   ON wm_recent_change USING GIN (topics);
CREATE INDEX IF NOT EXISTS idx_wm_rc_page_geo     ON wm_recent_change (page_lat, page_lon);

-- Only rows with a country AND at least one topic (no 'other' ever inserted)
CREATE OR REPLACE VIEW v_wm_recent_change_enabled AS
SELECT *
FROM wm_recent_change
WHERE page_country_code IS NOT NULL
  AND topics IS NOT NULL
  AND cardinality(topics) > 0;

-- =========================
-- 3) Stocks
-- =========================

-- 3a) Tickers
CREATE TABLE IF NOT EXISTS ticker (
  id           SERIAL PRIMARY KEY,
  symbol       TEXT UNIQUE NOT NULL,
  name         TEXT,
  exchange     TEXT,
  currency     TEXT,
  mic          TEXT,                 -- e.g., XNAS, XLON
  tz           TEXT,                 -- IANA TZ, e.g. 'America/New_York'
  last_updated TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ticker_symbol ON ticker(symbol);

-- 3b) Minute OHLCV
CREATE TABLE IF NOT EXISTS stock_value_1m (
  id          BIGSERIAL PRIMARY KEY,
  ticker_id   INT NOT NULL REFERENCES ticker(id) ON DELETE CASCADE,
  ts_utc      TIMESTAMPTZ NOT NULL,
  interval    TEXT NOT NULL DEFAULT '1m',
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  volume      BIGINT,
  prepost     BOOLEAN NOT NULL DEFAULT FALSE,
  source      TEXT NOT NULL DEFAULT 'yfinance',
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_final    BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT chk_stock_value_interval
    CHECK (interval IN ('1m','2m','5m','15m','30m','1h','1d')),
  CONSTRAINT chk_ohlc_nonneg CHECK (
    (open  IS NULL OR open  >= 0) AND
    (high  IS NULL OR high  >= 0) AND
    (low   IS NULL OR low   >= 0) AND
    (close IS NULL OR close >= 0)
  ),
  CONSTRAINT chk_volume_nonneg CHECK (volume IS NULL OR volume >= 0),
  UNIQUE (ticker_id, ts_utc, interval)
);
CREATE INDEX IF NOT EXISTS idx_stock_value_1m_ticker_ts
  ON stock_value_1m (ticker_id, ts_utc DESC);
CREATE INDEX IF NOT EXISTS brin_stock_value_1m_ts
  ON stock_value_1m USING BRIN (ts_utc) WITH (pages_per_range = 32);

-- 3c) Latest views
CREATE OR REPLACE VIEW v_stock_value_latest AS
SELECT DISTINCT ON (ticker_id)
  ticker_id, ts_utc, open, high, low, close, volume, prepost, source, interval, ingested_at, is_final
FROM stock_value_1m
WHERE interval = '1m'
ORDER BY ticker_id, ts_utc DESC;

CREATE OR REPLACE VIEW v_stock_value_latest_enriched AS
SELECT
  ti.symbol, ti.name, ti.exchange, ti.currency, ti.mic, ti.tz,
  sv.ts_utc, sv.open, sv.high, sv.low, sv.close, sv.volume, sv.prepost, sv.source, sv.ingested_at, sv.is_final
FROM stock_value_1m sv
JOIN ticker ti ON ti.id = sv.ticker_id
WHERE sv.interval = '1m'
  AND (ti.id, sv.ts_utc) IN (
    SELECT ticker_id, MAX(ts_utc)
    FROM stock_value_1m
    WHERE interval = '1m'
    GROUP BY ticker_id
  );

-- =========================
-- 4) Seed global tickers (Yahoo Finance symbols)
-- =========================
INSERT INTO ticker(symbol, name, exchange, currency, mic, tz)
VALUES
  ('AAPL','Apple Inc.','NASDAQ','USD','XNAS','America/New_York'),
  ('MSFT','Microsoft Corp.','NASDAQ','USD','XNAS','America/New_York'),
  ('AMZN','Amazon.com Inc.','NASDAQ','USD','XNAS','America/New_York'),
  ('TSLA','Tesla Inc.','NASDAQ','USD','XNAS','America/New_York'),
  ('META','Meta Platforms Inc.','NASDAQ','USD','XNAS','America/New_York'),
  ('NVDA','NVIDIA Corp.','NASDAQ','USD','XNAS','America/New_York'),
  ('SHOP.TO','Shopify Inc.','TSX','CAD','XTSE','America/Toronto'),
  ('HSBA.L','HSBC Holdings plc','LSE','GBP','XLON','Europe/London'),
  ('BP.L','BP p.l.c.','LSE','GBP','XLON','Europe/London'),
  ('SAP.DE','SAP SE','XETRA','EUR','XETR','Europe/Berlin'),
  ('VOW3.DE','Volkswagen AG','XETRA','EUR','XETR','Europe/Berlin'),
  ('MC.PA','LVMH Moet Hennessy Louis Vuitton SE','EURONEXT PARIS','EUR','XPAR','Europe/Paris'),
  ('AIR.PA','Airbus SE','EURONEXT PARIS','EUR','XPAR','Europe/Paris'),
  ('NOVN.SW','Novartis AG','SIX Swiss Exchange','CHF','XSWX','Europe/Zurich'),
  ('UBSG.SW','UBS Group AG','SIX Swiss Exchange','CHF','XSWX','Europe/Zurich'),
  ('ASML.AS','ASML Holding NV','EURONEXT Amsterdam','EUR','XAMS','Europe/Amsterdam'),
  ('VOLV-B.ST','Volvo AB (B)','Stockholm','SEK','XSTO','Europe/Stockholm'),
  ('7203.T','Toyota Motor Corp.','Tokyo','JPY','XTKS','Asia/Tokyo'),
  ('6758.T','Sony Group Corp.','Tokyo','JPY','XTKS','Asia/Tokyo'),
  ('005930.KS','Samsung Electronics Co.','KOSPI','KRW','XKRX','Asia/Seoul'),
  ('0700.HK','Tencent Holdings Ltd.','HKEX','HKD','XHKG','Asia/Hong_Kong'),
  ('BABA','Alibaba Group Holding Ltd.','NYSE','USD','XNYS','America/New_York'),
  ('TCS.NS','Tata Consultancy Services Ltd.','NSE','INR','XNSE','Asia/Kolkata'),
  ('INFY.NS','Infosys Ltd.','NSE','INR','XNSE','Asia/Kolkata'),
  ('BHP.AX','BHP Group Ltd.','ASX','AUD','XASX','Australia/Sydney'),
  ('CBA.AX','Commonwealth Bank of Australia','ASX','AUD','XASX','Australia/Sydney'),
  ('PETR4.SA','Petrobras PN','B3','BRL','BVMF','America/Sao_Paulo'),
  ('VALE3.SA','Vale SA','B3','BRL','BVMF','America/Sao_Paulo'),
  ('NPN.JO','Naspers Ltd.','JSE','ZAR','XJSE','Africa/Johannesburg'),
  ('D05.SI','DBS Group Holdings Ltd.','SGX','SGD','XSES','Asia/Singapore')
ON CONFLICT (symbol) DO UPDATE
SET name         = EXCLUDED.name,
    exchange     = EXCLUDED.exchange,
    currency     = EXCLUDED.currency,
    mic          = EXCLUDED.mic,
    tz           = EXCLUDED.tz,
    last_updated = now();
