-- ============================================
-- init.sql (with global tickers)
-- ============================================

-- =========================
-- 1) Topics master
-- =========================
CREATE TABLE IF NOT EXISTS wm_topic (
  id       SERIAL PRIMARY KEY,
  key      TEXT UNIQUE NOT NULL,
  label    TEXT NOT NULL,
  enabled  BOOLEAN NOT NULL DEFAULT TRUE,
  keywords TEXT[] NOT NULL DEFAULT '{}'
);

INSERT INTO wm_topic(key, label, keywords) VALUES
  ('other',     'Other',     ARRAY[]::TEXT[]),
  ('finance',   'Finance',   ARRAY['finance','financial','bank','stock','market','inflation']),
  ('politics',  'Politics',  ARRAY['politic','election','parliament','minister','party']),
  ('economics', 'Economics', ARRAY['economics','gdp','unemployment','macro']),
  ('war',       'War',       ARRAY['war','conflict','battle','invasion','military']),
  ('crypto',    'Crypto',    ARRAY['crypto','bitcoin','ethereum','blockchain','token']),
  ('equity',    'Equity',    ARRAY['stock','share','equity','ipo']),
  ('etf',       'ETF',       ARRAY['etf','exchange traded fund']),
  ('index',     'Index',     ARRAY['index','indices','benchmark']),
  ('forex',     'Forex',     ARRAY['fx','forex','eurusd','usdjpy','gbpusd','currency']),
  ('commodity', 'Commodity', ARRAY['oil','gold','silver','copper','corn']),
  ('technology','Technology',ARRAY['tech','software','cloud','semiconductor']),
  ('healthcare','Healthcare',ARRAY['health','biotech','pharma','medical']),
  ('energy',    'Energy',    ARRAY['energy','oil','gas','solar','wind']),
  ('consumer',  'Consumer',  ARRAY['retail','consumer','ecommerce','beverage'])
ON CONFLICT (key) DO UPDATE
SET label = EXCLUDED.label,
    keywords = EXCLUDED.keywords;

-- =========================
-- 2) Wikimedia recent changes
-- =========================
CREATE TABLE IF NOT EXISTS wm_recent_change (
  meta_id   TEXT PRIMARY KEY,
  dt        TIMESTAMPTZ,
  wiki      TEXT,
  page_id   BIGINT,
  title     TEXT,
  namespace INT,
  type      TEXT,
  user_text TEXT,
  comment   TEXT,
  old_len   INT,
  new_len   INT,
  added     INT,
  removed   INT,
  topics    TEXT[] NOT NULL DEFAULT ARRAY['other']::TEXT[],
  raw       JSONB
);

CREATE INDEX IF NOT EXISTS idx_wm_rc_dt     ON wm_recent_change (dt DESC);
CREATE INDEX IF NOT EXISTS idx_wm_rc_topics ON wm_recent_change USING GIN (topics);

CREATE OR REPLACE VIEW v_wm_recent_change_enabled AS
SELECT rc.*
FROM wm_recent_change rc
WHERE EXISTS (
  SELECT 1 FROM wm_topic t
  WHERE t.enabled AND t.key = ANY (rc.topics)
);

-- =========================
-- 3) Stock data
-- =========================

-- 3a) Tickers (1 topic per ticker)
CREATE TABLE IF NOT EXISTS ticker (
  id           SERIAL PRIMARY KEY,
  symbol       TEXT UNIQUE NOT NULL,
  name         TEXT,
  exchange     TEXT,
  currency     TEXT,
  topic_id     INT NOT NULL REFERENCES wm_topic(id) ON DELETE RESTRICT,
  last_updated TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ticker_topic  ON ticker(topic_id);
CREATE INDEX IF NOT EXISTS idx_ticker_symbol ON ticker(symbol);

-- 3b) Stock values (1-minute OHLCV)
CREATE TABLE IF NOT EXISTS stock_value_1m (
  id         BIGSERIAL PRIMARY KEY,
  ticker_id  INT NOT NULL REFERENCES ticker(id) ON DELETE CASCADE,
  ts_utc     TIMESTAMPTZ NOT NULL,
  open       NUMERIC,
  high       NUMERIC,
  low        NUMERIC,
  close      NUMERIC,
  volume     BIGINT,
  prepost    BOOLEAN DEFAULT FALSE,
  source     TEXT DEFAULT 'yfinance',
  UNIQUE (ticker_id, ts_utc)
);

CREATE INDEX IF NOT EXISTS idx_stock_value_1m_ticker_ts
  ON stock_value_1m (ticker_id, ts_utc DESC);

-- =========================
-- 4) Views
-- =========================

CREATE OR REPLACE VIEW v_ticker AS
SELECT t.id, t.symbol, t.name, t.exchange, t.currency,
       tp.key AS topic_key, tp.label AS topic_label
FROM ticker t
JOIN wm_topic tp ON t.topic_id = tp.id;

CREATE OR REPLACE VIEW v_stock_value_latest AS
SELECT DISTINCT ON (ticker_id)
  ticker_id, ts_utc, open, high, low, close, volume, prepost, source
FROM stock_value_1m
ORDER BY ticker_id, ts_utc DESC;

CREATE OR REPLACE VIEW v_stock_value_latest_enriched AS
SELECT
  ti.symbol, ti.name, ti.exchange, ti.currency,
  tp.key AS topic_key, tp.label AS topic_label,
  sv.ts_utc, sv.open, sv.high, sv.low, sv.close, sv.volume
FROM stock_value_1m sv
JOIN ticker ti ON ti.id = sv.ticker_id
JOIN wm_topic tp ON tp.id = ti.topic_id
WHERE (ti.id, sv.ts_utc) IN (
  SELECT ticker_id, MAX(ts_utc)
  FROM stock_value_1m
  GROUP BY ticker_id
);

-- =========================
-- 5) Seed global tickers
-- =========================

WITH eq AS (SELECT id FROM wm_topic WHERE key = 'equity')
INSERT INTO ticker(symbol, name, exchange, currency, topic_id)
SELECT s.symbol, s.name, s.exchange, s.currency, eq.id
FROM (VALUES
  -- United States
  ('AAPL','Apple Inc.','NASDAQ','USD'),
  ('MSFT','Microsoft Corp.','NASDAQ','USD'),
  ('AMZN','Amazon.com Inc.','NASDAQ','USD'),
  ('TSLA','Tesla Inc.','NASDAQ','USD'),
  ('META','Meta Platforms Inc.','NASDAQ','USD'),
  ('NVDA','NVIDIA Corp.','NASDAQ','USD'),

  -- Canada
  ('SHOP.TO','Shopify Inc.','TSX','CAD'),

  -- United Kingdom
  ('HSBA.L','HSBC Holdings plc','LSE','GBP'),
  ('BP.L','BP p.l.c.','LSE','GBP'),

  -- Germany
  ('SAP.DE','SAP SE','XETRA','EUR'),
  ('VOW3.DE','Volkswagen AG','XETRA','EUR'),

  -- France
  ('MC.PA','LVMH Moet Hennessy Louis Vuitton SE','EURONEXT PARIS','EUR'),
  ('AIR.PA','Airbus SE','EURONEXT PARIS','EUR'),

  -- Switzerland
  ('NOVN.SW','Novartis AG','SIX Swiss Exchange','CHF'),
  ('UBSG.SW','UBS Group AG','SIX Swiss Exchange','CHF'),

  -- Netherlands
  ('ASML.AS','ASML Holding NV','EURONEXT Amsterdam','EUR'),

  -- Sweden
  ('VOLV-B.ST','Volvo AB (B)','Stockholm','SEK'),

  -- Japan
  ('7203.T','Toyota Motor Corp.','Tokyo','JPY'),
  ('6758.T','Sony Group Corp.','Tokyo','JPY'),

  -- South Korea
  ('005930.KS','Samsung Electronics Co.','KOSPI','KRW'),

  -- China / Hong Kong
  ('0700.HK','Tencent Holdings Ltd.','HKEX','HKD'),
  ('BABA','Alibaba Group Holding Ltd.','NYSE','USD'),

  -- India
  ('TCS.NS','Tata Consultancy Services Ltd.','NSE','INR'),
  ('INFY.NS','Infosys Ltd.','NSE','INR'),

  -- Australia
  ('BHP.AX','BHP Group Ltd.','ASX','AUD'),
  ('CBA.AX','Commonwealth Bank of Australia','ASX','AUD'),

  -- Brazil
  ('PETR4.SA','Petrobras PN','B3','BRL'),
  ('VALE3.SA','Vale SA','B3','BRL'),

  -- South Africa
  ('NPN.JO','Naspers Ltd.','JSE','ZAR'),

  -- Singapore
  ('D05.SI','DBS Group Holdings Ltd.','SGX','SGD')
) AS s(symbol, name, exchange, currency)
ON CONFLICT (symbol) DO UPDATE
SET name = EXCLUDED.name,
    exchange = EXCLUDED.exchange,
    currency = EXCLUDED.currency,
    topic_id = EXCLUDED.topic_id,
    last_updated = now();
