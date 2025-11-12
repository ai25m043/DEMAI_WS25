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

-- ============================================================
-- 5) Country dim + Ticker → Country mapping
-- ============================================================
CREATE TABLE IF NOT EXISTS country_dim (
  iso2 CHAR(2) PRIMARY KEY,
  name TEXT NOT NULL
);

INSERT INTO country_dim(iso2, name) VALUES
  ('US','United States'), ('CA','Canada'), ('GB','United Kingdom'), ('FR','France'),
  ('DE','Germany'), ('IT','Italy'), ('ES','Spain'), ('NL','Netherlands'),
  ('BE','Belgium'), ('IE','Ireland'), ('PT','Portugal'), ('SE','Sweden'),
  ('NO','Norway'), ('CH','Switzerland'), ('AT','Austria'), ('PL','Poland'),
  ('GR','Greece'), ('TR','Türkiye'), ('RU','Russia'), ('CN','China'),
  ('JP','Japan'), ('KR','South Korea'), ('IN','India'), ('AU','Australia'),
  ('BR','Brazil'), ('AR','Argentina'), ('MX','Mexico'), ('ZA','South Africa'),
  ('EG','Egypt'), ('SA','Saudi Arabia'), ('AE','United Arab Emirates'),
  ('SG','Singapore'), ('HK','Hong Kong'), ('TW','Taiwan')
ON CONFLICT (iso2) DO NOTHING;

ALTER TABLE ticker
  ADD COLUMN IF NOT EXISTS iso2 CHAR(2),
  ADD CONSTRAINT fk_ticker_country
    FOREIGN KEY (iso2) REFERENCES country_dim(iso2);

CREATE INDEX IF NOT EXISTS idx_ticker_iso2 ON ticker(iso2);

-- Map seeded tickers → countries
UPDATE ticker SET iso2='US' WHERE symbol IN ('AAPL','MSFT','AMZN','TSLA','META','NVDA','BABA');
UPDATE ticker SET iso2='CA' WHERE symbol IN ('SHOP.TO');
UPDATE ticker SET iso2='GB' WHERE symbol IN ('HSBA.L','BP.L');
UPDATE ticker SET iso2='DE' WHERE symbol IN ('SAP.DE','VOW3.DE');
UPDATE ticker SET iso2='FR' WHERE symbol IN ('MC.PA','AIR.PA');
UPDATE ticker SET iso2='CH' WHERE symbol IN ('NOVN.SW','UBSG.SW');
UPDATE ticker SET iso2='NL' WHERE symbol IN ('ASML.AS');
UPDATE ticker SET iso2='SE' WHERE symbol IN ('VOLV-B.ST');
UPDATE ticker SET iso2='JP' WHERE symbol IN ('7203.T','6758.T');
UPDATE ticker SET iso2='KR' WHERE symbol IN ('005930.KS');
UPDATE ticker SET iso2='HK' WHERE symbol IN ('0700.HK');
UPDATE ticker SET iso2='IN' WHERE symbol IN ('TCS.NS','INFY.NS');
UPDATE ticker SET iso2='AU' WHERE symbol IN ('BHP.AX','CBA.AX');
UPDATE ticker SET iso2='BR' WHERE symbol IN ('PETR4.SA','VALE3.SA');
UPDATE ticker SET iso2='ZA' WHERE symbol IN ('NPN.JO');
UPDATE ticker SET iso2='SG' WHERE symbol IN ('D05.SI');

-- ============================================================
-- 6) Minute aggregates per country (views)
-- ============================================================

-- Wikimedia events per minute per country (UTC minute)
CREATE OR REPLACE VIEW v_wm_country_minute AS
SELECT
  date_trunc('minute', dt AT TIME ZONE 'UTC') AS ts_minute_utc,
  page_country_code::char(2)                 AS iso2,
  COUNT(*)::int                              AS event_count
FROM v_wm_recent_change_enabled
GROUP BY 1,2;

CREATE INDEX IF NOT EXISTS idx_wm_rc_country_dt ON wm_recent_change(page_country_code, dt);

-- Symbol 1m returns (per ticker), then average by country/minute
CREATE OR REPLACE VIEW v_symbol_return_1m AS
SELECT
  t.id            AS ticker_id,
  t.symbol,
  t.iso2::char(2) AS iso2,
  sv.ts_utc       AS ts_minute_utc,
  sv.close,
  LAG(sv.close) OVER (PARTITION BY t.id ORDER BY sv.ts_utc) AS prev_close
FROM stock_value_1m sv
JOIN ticker t ON t.id = sv.ticker_id
WHERE sv.interval = '1m';

CREATE OR REPLACE VIEW v_country_return_minute AS
SELECT
  iso2,
  ts_minute_utc,
  AVG( (close - prev_close) / NULLIF(prev_close,0) ) AS avg_return_1m,
  COUNT(*)::int                                      AS symbols_count
FROM v_symbol_return_1m
WHERE iso2 IS NOT NULL AND prev_close IS NOT NULL
GROUP BY iso2, ts_minute_utc;

-- Join both signals + rolling windows for the map
CREATE OR REPLACE VIEW v_country_map_roll AS
WITH recent AS (
  SELECT
    COALESCE(r.iso2, w.iso2)                     AS iso2,
    COALESCE(r.ts_minute_utc, w.ts_minute_utc)   AS ts_minute_utc,
    COALESCE(w.event_count, 0)                   AS event_count,
    r.avg_return_1m,
    r.symbols_count
  FROM v_country_return_minute r
  FULL OUTER JOIN v_wm_country_minute w
    ON r.iso2 = w.iso2 AND r.ts_minute_utc = w.ts_minute_utc
  WHERE COALESCE(r.ts_minute_utc, w.ts_minute_utc) >= now() AT TIME ZONE 'UTC' - interval '60 minutes'
)
SELECT
  iso2,
  MAX(ts_minute_utc) AS ts_latest_utc,
  SUM(event_count) FILTER (WHERE ts_minute_utc >= now() AT TIME ZONE 'UTC' - interval '10 minutes') AS events_10m,
  SUM(event_count) FILTER (WHERE ts_minute_utc >= now() AT TIME ZONE 'UTC' - interval '30 minutes') AS events_30m,
  AVG(avg_return_1m) FILTER (WHERE ts_minute_utc >= now() AT TIME ZONE 'UTC' - interval '10 minutes') AS ret_avg_10m,
  AVG(avg_return_1m) FILTER (WHERE ts_minute_utc >= now() AT TIME ZONE 'UTC' - interval '30 minutes') AS ret_avg_30m,
  MAX(symbols_count) AS symbols_seen
FROM recent
GROUP BY iso2;

-- ============================================
-- update_wm_topic_keywords_multilang_global.sql
-- Adds multilingual keywords (EN, DE, FR, ES, IT, HI, AR, RU, TR, ZH)
-- ============================================

-- ============================================
-- Multilingual topic keyword updates (USE ONLY UPDATE)
-- Run AFTER the wm_topic seed INSERT/UPSERT
-- ============================================

-- ============================================
-- Multilingual topic keyword updates (EN/DE richest; others compact)
-- Paste this AFTER the wm_topic seed INSERT/UPSERT in init.sql
-- Only UPDATE statements.
-- ============================================

UPDATE wm_topic SET keywords = ARRAY[]::TEXT[] WHERE key = 'other';

-- =========================
-- FINANCE
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'finance','financial','fintech','bank','banking','lender','lending','loan','credit','mortgage','debt','bond','fixed income',
  'equity','stock','share','market','exchange','index','benchmark','etf','mutual fund','hedge fund','private equity','venture capital',
  'derivative','option','future','swap','cds','rate','interest rate','yield','curve','spread','carry','dividend',
  'ipo','direct listing','secondary offering','follow-on','spo','buyback','repurchase','m&a','merger','acquisition','spinoff','spac',
  'valuation','multiple','ev/ebitda','dcf','cash flow','liquidity','leverage','default','bankruptcy','chapter 11',
  'forex','fx','currency','devaluation','revaluation','peg','capital controls',
  'inflation','deflation','disinflation','stagflation','tightening','easing','qe','qt','taper',
  -- DE (rich)
  'finanzen','fintech','bank','banken','bankwesen','kredit','darlehen','hypothek','schuld','anleihe','festverzinslich',
  'aktie','aktien','börse','markt','handelsplatz','index','benchmark','etf','investmentfonds','hedgefonds','private equity',
  'derivat','option','future','swap','zins','zinsrate','rendite','zinskurve','spread','carry','dividende',
  'börsengang','ipo','direktlisting','folgeemission','rückkauf','aktienrückkauf','fusion','übernahme','abspaltung','spac',
  'bewertung','multiplikator','ev/ebitda','dCF','cashflow','liquidität','verschuldung','ausfall','insolvenz',
  'devisen','währung','abwertung','aufwertung','wechselkursbindung','kapitalkontrollen',
  'inflation','deflation','disinflation','stagflation','straffung','lockerung','anleihekäufe','bilanzabbau',
  -- FR
  'finance','banque','prêt','crédit','obligation','actions','bourse','indice','etf','taux','rendement','dividende','fusion','acquisition','devises','inflation',
  -- ES
  'finanzas','banco','préstamo','crédito','bono','acción','bolsa','índice','etf','tipo de interés','rendimiento','dividendo','fusión','adquisición','divisas','inflación',
  -- IT
  'finanza','banca','prestito','credito','obbligazione','azioni','borsa','indice','etf','tasso','rendimento','dividendo','fusione','acquisizione','valuta','inflazione',
  -- TR
  'finans','banka','kredi','tahvil','hisse','borsa','endeks','faiz','getiri','temettü','döviz','enflasyon',
  -- RU
  'финансы','банк','кредит','облигация','акции','биржа','индекс','ставка','доходность','дивиденды','валюта','инфляция',
  -- AR
  'تمويل','بنك','قرض','ائتمان','سند','سهم','بورصة','مؤشر','سعر الفائدة','عائد','عملة','تضخم',
  -- FA (Farsi)
  'مالی','بانک','وام','اعتبار','اوراق قرضه','سهام','بازار','نرخ بهره','بازده','ارز','تورم',
  -- HE (Hebrew)
  'כספים','בנק','הלוואה','אשראי','אג״ח','מניה','בורסה','מדד','ריבית','תשואה','מטבע','אינפלציה',
  -- UR (Urdu)
  'مالیات','بینک','قرض','کریڈٹ','بانڈ','حصص','مارکیٹ','سود','افراطِ زر','زرِ مبادلہ',
  -- SR/HR/BS (Balkan)
  'finansije','banka','kredit','obveznica','akcija','berza','indeks','kamatna stopa','prinos','dividenda','valuta','inflacija',
  -- SQ (Albanian)
  'financa','bankë','kredi','obligacion','aksion','bursë','indeks','normë interesi','dividendë','monedhë','inflacion',
  -- SL/MK (Slovenian/Macedonian)
  'finančne','banka','kredit','obveznica','delnica','borza','indeks','obrestna mera','donos','valuta','inflacija','финансии','банка','кредит','обврзници','берза',
  -- ZH
  '金融','银行','贷款','债券','股票','指数','交易所','利率','收益率','股息','外汇','通货膨胀',
  -- JA
  '金融','銀行','ローン','債券','株式','指数','為替','金利','配当','インフレ',
  -- KO
  '금융','은행','대출','채권','주식','지수','환율','금리','배당','인플레이션'
] WHERE key = 'finance';

-- =========================
-- POLITICS
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'politics','election','vote','referendum','ballot','turnout','campaign','parliament','congress','senate','house',
  'minister','ministry','cabinet','government','president','prime minister','chancellor','governor','mayor',
  'party','coalition','opposition','policy','bill','law','act','decree','constitution','amendment',
  'court','supreme court','constitutional court','judiciary','protest','demonstration','rally','strike',
  'sanction','diplomacy','treaty','summit','coalition talks','confidence vote',
  -- DE (rich)
  'politik','wahl','abstimmung','volksentscheid','stichwahl','wahlbeteiligung','wahlkampf',
  'parlament','bundestag','bundesrat','regierung','kabinet','ministerium','minister',
  'bundeskanzler','bundespräsident','ministerpräsident','bürgermeister',
  'partei','koalition','opposition','koalitionsverhandlungen','vertrauensfrage',
  'gesetz','gesetzesentwurf','verordnung','verfassung','grundgesetz','änderung',
  'gericht','bundesverfassungsgericht','justiz','demonstration','protest','kundgebung','streik',
  'sanktion','diplomatie','vertrag','gipfel',
  -- FR
  'politique','élection','vote','référendum','parlement','gouvernement','ministre','président','parti','coalition','loi','constitution','cour','manifestation','diplomatie',
  -- ES
  'política','elección','voto','referéndum','parlamento','gobierno','ministro','presidente','partido','coalición','ley','constitución','corte','protesta','diplomacia',
  -- IT
  'politica','elezione','voto','referendum','parlamento','governo','ministro','presidente','partito','coalizione','legge','costituzione','corte','protesta','diplomazia',
  -- TR
  'siyaset','seçim','oy','referandum','parlamento','hükümet','bakan','cumhurbaşkanı','parti','koalisyon','yasa','anayasa','mahkeme','protesto','diplomasi',
  -- RU
  'политика','выборы','голосование','референдум','парламент','правительство','министр','президент','партия','коалиция','закон','конституция','суд','протест','дипломатия',
  -- AR
  'سياسة','انتخابات','تصويت','استفتاء','برلمان','حكومة','وزير','رئيس','حزب','تحالف','قانون','دستور','محكمة','احتجاج','دبلوماسية',
  -- FA
  'سیاست','انتخابات','رأی','همه‌پرسی','پارلمان','دولت','وزیر','رئیس‌جمهور','حزب','ائتلاف','قانون','قانون اساسی','دادگاه','اعتراض','دیپلماسی',
  -- HE
  'פוליטיקה','בחירות','משאל עם','כנסת','ממשלה','שר','נשיא','מפלגה','קואליציה','חוק','חוקה','בית משפט','מחאה','דיפלומטיה',
  -- HI/UR
  'राजनीति','चुनाव','मतदान','संसद','सरकार','मंत्री','राष्ट्रपति','कानून','संविधान','विरोध','कूटनीति',
  'سیاست','انتخابات','پارلیمنٹ','حکومت','وزیر','صدر','قانون','آئین','احتجاج','سفارت کاری',
  -- SR/HR/BS (Balkan)
  'politika','izbori','glasanje','referendum','parlament','vlada','ministar','predsjednik','premijer','stranka','koalicija','opozicija','zakon','ustav','sud','protest','diplomatija',
  -- SQ (Albanian)
  'politikë','zgjedhje','votim','parlament','qeveri','ministër','president','parti','koalicion','opozitë','ligj','kushtetutë','gjykatë','protestë','diplomaci',
  -- SL/MK
  'politika','volitve','parlament','vlada','zakon','ustava','sodišče','protest','diplomatija','политика','избори','парламент','влада','закон','устав','суд','протест',
  -- ZH/JA/KO
  '政治','选举','投票','议会','政府','部长','总统','法律','宪法','法院','抗议','外交',
  '政治','選挙','投票','議会','政府','大統領','法律','憲法','裁判所','抗議','外交',
  '정치','선거','투표','의회','정부','장관','대통령','법률','헌법','법원','시위','외교'
] WHERE key = 'politics';

-- =========================
-- ECONOMICS
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'economy','economics','macro','gdp','growth','slowdown','recession','soft landing','hard landing',
  'inflation','deflation','disinflation','stagflation','cpi','ppi','pce','core','headline',
  'unemployment','jobless claims','employment','payrolls','wage','participation rate',
  'trade balance','current account','budget','deficit','surplus','debt-to-gdp',
  'pmi','ism','retail sales','industrial production','housing starts','confidence index','sentiment',
  -- DE (rich)
  'wirtschaft','konjunktur','makro','bip','wachstum','rezession','weiche landung','harte landung',
  'inflation','deflation','disinflation','stagflation','vpi','erzeugerpreise','pce','kernrate','gesamt',
  'arbeitslosigkeit','arbeitsmarkt','beschäftigung','lohn','erwerbsquote',
  'handelsbilanz','leistungsbilanz','haushalt','defizit','überschuss','schuldenquote',
  'pmi','einkaufsmanagerindex','einzelhandelsumsatz','industrieproduktion','bauanträge','vertrauen','stimmung',
  -- FR/ES/IT (compact)
  'économie','pib','inflation','récession','chômage','déficit','pmi',
  'economía','pib','inflación','recesión','desempleo','déficit','pmi',
  'economia','pil','inflazione','recessione','disoccupazione','deficit','pmi',
  -- TR/RU
  'ekonomi','büyüme','enflasyon','resesyon','işsizlik','bütçe','cari açık',
  'экономика','ввп','инфляция','рецессия','безработица','дефицит','пми',
  -- AR/FA/HE
  'اقتصاد','نمو','ركود','تضخم','بطالة','عجز',
  'اقتصاد','تورم','رکود','بیکاری','کسری',
  'כלכלה','צמיחה','מיתון','אינפלציה','אבטלה','גירעון',
  -- HI/UR/BN
  'अर्थव्यवस्था','जीडीपी','मुद्रास्फीति','मंदी','रोज़गार','घाटा',
  'معیشت','جی ڈی پی','افراطِ زر','کساد بازاری','روزگار','خسارہ',
  'অর্থনীতি','জিডিপি','মুদ্রাস্ফীতি','মন্দা','বেকারত্ব','ঘাটতি',
  -- SR/HR/BS/SQ/SL/MK
  'ekonomija','bnp','inflacija','recesija','nezaposlenost','budžet','deficit',
  'ekonomi','inflacion','recesion','papunësi','buxhet',
  'gospodarstvo','inflacija','recesija','brezposelnost','proračun','дефицит',
  -- ZH/JA/KO
  '经济','宏观','国内生产总值','通胀','衰退','失业','赤字','采购经理指数',
  '経済','gdp','インフレ','景気後退','失業','赤字','pmi',
  '경제','gdp','인플레이션','침체','실업','적자','pmi'
] WHERE key = 'economics';

-- =========================
-- WAR
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'war','conflict','battle','clash','offensive','counteroffensive','invasion','occupation','incursion',
  'airstrike','missile','rocket','shelling','artillery','drone','uav','armor','tank','frontline',
  'mobilization','conscription','ceasefire','truce','hostilities','casualties',
  -- DE (rich)
  'krieg','konflikt','gefecht','offensive','gegenoffensive','invasion','besetzung','vorstoß',
  'luftangriff','rakete','beschuss','artillerie','drohne','panzer','frontlinie',
  'mobilmachung','einberufung','waffenruhe','feuerpause','verluste',
  -- FR/ES/IT
  'guerre','conflit','bataille','invasion','frappe aérienne','missile','drone','cessez-le-feu',
  'guerra','conflicto','batalla','invasión','ataque aéreo','misil','dron','alto el fuego',
  'guerra','conflitto','battaglia','invasione','attacco aereo','missile','drone','cessate il fuoco',
  -- TR/RU/UK
  'savaş','çatışma','istila','hava saldırısı','füze','drone','ateşkes',
  'война','конфликт','битва','вторжение','ракетный удар','дрон','перемирие',
  'війна','конфлікт','битва','вторгнення','ракетний удар','безпілотник','перемирʼя',
  -- AR/FA/HE
  'حرب','صراع','معركة','غزو','هجوم جوي','صاروخ','طائرة مسيرة','هدنة',
  'جنگ','درگیری','نبرد','حمله','حمله هوایی','موشک','پهپاد','آتش‌بس',
  'מלחמה','עימות','קרב','פלישה','תקיפה אווירית','טיל','מל״ט','הפסקת אש',
  -- HI/UR
  'युद्ध','संघर्ष','लड़ाई','आक्रमण','ड्रोन','मिसाइल','युद्धविराम',
  'جنگ','تصادم','حملہ','ڈرون','میزائل','فائر بندی',
  -- SR/HR/BS/SQ/SL/MK
  'rat','sukob','bitka','invazija','napad','vojnik','raketa','dron','primirje',
  'luftë','konflikt','betejë','pushtim','sulm','armëpushim',
  'vojna','spopad','bitka','invazija','napad','dron','premirje','војна','конфликт','битка','инвазија','примирје',
  -- ZH/JA/KO
  '战争','冲突','战斗','入侵','空袭','导弹','无人机','停火',
  '戦争','衝突','戦闘','侵攻','空爆','ミサイル','無人機','停戦',
  '전쟁','분쟁','교전','침공','공습','미사일','드론','휴전'
] WHERE key = 'war';

-- =========================
-- CRYPTO
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'crypto','cryptocurrency','digital asset','bitcoin','btc','ethereum','eth','solana','blockchain','ledger',
  'token','utility token','security token','stablecoin','defi','dex','cex','nft','staking','airdrop','smart contract',
  'layer 2','rollup','gas fee','hashrate','mining','halving','wallet','seed phrase',
  -- DE (rich)
  'krypto','kryptowährung','digitale assets','bitcoin','ethereum','solana','blockchain','distributed ledger',
  'token','stablecoin','defi','dex','börsen','nft','staking','airdrop','smart contract','layer 2','rollup',
  'gasgebühr','hashrate','mining','halbierung','wallet','seed phrase',
  -- FR/ES/IT
  'crypto','monnaie numérique','bitcoin','ethereum','blockchain','jeton','stablecoin','defi','nft','portefeuille',
  'cripto','criptomoneda','bitcoin','ethereum','cadena de bloques','token','stablecoin','defi','nft','billetera',
  'cripto','criptovaluta','bitcoin','ethereum','blockchain','gettone','stablecoin','defi','nft','portafoglio',
  -- TR/RU
  'kripto','kriptopara','blokzincir','token','stablecoin','defi','nft','cüzdan','madencilik',
  'крипто','криптовалюта','блокчейн','токен','стейблкоин','дефи','нфт','кошелёк','майнинг',
  -- AR/FA/HE
  'تشفير','عملة رقمية','بيتكوين','إيثريوم','سلسلة الكتل','رمز','عملة مستقرة','تمويل لامركزي','محفظة',
  'ارز دیجیتال','بیتکوین','اتریوم','بلاکچین','توکن','استیبل کوین','دیفای','کیف پول',
  'קריפטו','מטבע דיגיטלי','ביטקוין','אתריום','בלוקצ׳יין','טוקן','מטבע יציב','ארנק',
  -- HI/UR
  'क्रिप्टो','क्रिप्टोकरेंसी','बिटकॉइन','एथेरियम','ब्लॉकचेन','टोकन','स्टेबलकॉइन','वॉलेट',
  'کرپٹو','کرپٹوکرنسی','بٹ کوائن','ایتھیریئم','بلاک چین','ٹوکن','اسٹیبل کوائن','والیٹ',
  -- SR/HR/BS/SQ/SL/MK
  'kripto','kriptovaluta','bitcoin','ethereum','blokčejn','token','stablecoin','novčanik','rudarenje',
  'kripto','monedhë digjitale','token','portofol','minierë',
  'kripto','kripto valuta','veriga blokov','žeton','denarnica','рударење',
  -- ZH/JA/KO
  '加密','加密货币','比特币','以太坊','区块链','代币','稳定币','去中心化金融','钱包',
  '暗号資産','仮想通貨','ビットコイン','イーサリアム','ブロックチェーン','トークン','ステーブルコイン','ウォレット',
  '암호화폐','비트코인','이더리움','블록체인','토큰','스테이블코인','지갑'
] WHERE key = 'crypto';

-- =========================
-- TECHNOLOGY
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'technology','tech','software','cloud','saas','paas','iaas','api','platform','open source','opensource',
  'semiconductor','chip','cpu','gpu','asic','foundry','fab','node','ai','artificial intelligence','ml','machine learning',
  'deep learning','neural network','llm','genai','data center','edge','cybersecurity','zero trust','devops','sre',
  -- DE (rich)
  'technologie','technik','software','cloud','saas','paas','iaas','api','plattform','open source','opensource',
  'halbleiter','chip','cpu','gpu','asic','fertigung','foundry','strukturbreite','ki','künstliche intelligenz',
  'maschinelles lernen','tiefes lernen','neuronales netz','llm','genai','rechenzentrum','edge','it-sicherheit','cybersicherheit','devops','sre',
  -- FR/ES/IT
  'technologie','logiciel','cloud','api','open source','semi-conducteur','puce','ia','cybersécurité',
  'tecnología','software','nube','api','código abierto','semiconductor','chip','ia','ciberseguridad',
  'tecnologia','software','cloud','api','open source','semiconduttore','chip','ia','cybersicurezza',
  -- TR/RU
  'teknoloji','yazılım','bulut','api','yarı iletken','çip','yapay zeka','siber güvenlik','devops',
  'технологии','ПО','облако','api','полупроводник','чип','искусственный интеллект','кибербезопасность','devops',
  -- AR/FA/HE
  'تكنولوجيا','برمجيات','سحابة','واجهة برمجة التطبيقات','أشباه الموصلات','ذكاء اصطناعي','أمن سيبراني',
  'فناوری','نرم‌افزار','ابر','رابط برنامه‌نویسی','نیمه‌هادی','هوش مصنوعی','امنیت سایبری',
  'טכנולוגיה','תוכנה','ענן','api','שבב','בינה מלאכותית','אבטחת סייבר',
  -- HI/UR
  'प्रौद्योगिकी','सॉफ्टवेयर','क्लाउड','एपीआई','अर्धचालक','कृत्रिम बुद्धिमत्ता','साइबर सुरक्षा',
  'ٹیکنالوجی','سافٹ ویئر','کلاؤڈ','اے پی آئی','سیمی کنڈکٹر','مصنوعی ذہانت','سائبر سکیورٹی',
  -- SR/HR/BS/SQ/SL/MK
  'tehnologija','softver','oblak','api','poluprovodnik','čip','veštačka inteligencija','sajber bezbednost','devops',
  'teknologji','softuer','re','api','gjysmëpërçues','inteligjencë artificiale','siguri kibernetike',
  'tehnologija','programska oprema','oblak','api','polprevodnik','čip','kibernetska varnost','технологија','апи',
  -- ZH/JA/KO
  '技术','软件','云','接口','半导体','芯片','人工智能','网络安全',
  '技術','ソフトウェア','クラウド','API','半導体','チップ','人工知能','サイバーセキュリティ',
  '기술','소프트웨어','클라우드','API','반도체','칩','인공지능','사이버 보안'
] WHERE key = 'technology';

-- =========================
-- HEALTHCARE
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'health','healthcare','biotech','pharma','medical','device','hospital','clinic','vaccine','vaccination',
  'drug','therapy','trial','clinical trial','phase 1','phase 2','phase 3','approval','authorization',
  'fda','ema','who','diagnostic','screening','outbreak','epidemic','pandemic',
  -- DE (rich)
  'gesundheit','gesundheitswesen','biotech','pharma','medizin','medizinprodukt','krankenhaus','klinik','impfstoff','impfung',
  'arznei','medikament','therapie','studie','klinische studie','phase 1','phase 2','phase 3','zulassung','genehmigung',
  'bfarm','ema','who','diagnostik','screening','ausbruch','epidemie','pandemie',
  -- FR/ES/IT
  'santé','soins','biotechnologie','pharmacie','vaccin','essai clinique','autorisation','hôpital',
  'salud','biotecnología','farmacéutico','vacuna','ensayo clínico','aprobación','hospital',
  'salute','biotecnologia','farmaceutico','vaccino','sperimentazione clinica','approvazione','ospedale',
  -- TR/RU
  'sağlık','sağlık hizmeti','biyoteknoloji','ilaç','aşı','klinik çalışma','onay','hastane',
  'здравоохранение','биотехнологии','фармацевтика','вакцина','клинические испытания','одобрение','больница',
  -- AR/FA/HE
  'صحة','رعاية صحية','تقنية حيوية','دواء','لقاح','تجربة سريرية','موافقة','مستشفى',
  'سلامت','بهداشت','بیوتکنولوژی','دارو','واکسن','کارآزمایی بالینی','تأیید','بیمارستان',
  'בריאות','בריאות הציבור','ביוטכנולוגיה','תרופה','חיסון','ניסוי קליני','אישור','בית חולים',
  -- HI/UR
  'स्वास्थ्य','स्वास्थ्य सेवा','बायोटेक','फार्मा','टीका','क्लिनिकल ट्रायल','मंजूरी','अस्पताल',
  'صحت','صحت کی سہولت','بایو ٹیک','ادویات','ویکسین','کلینیکل آزمائش','منظوری','ہسپتال',
  -- SR/HR/BS/SQ/SL/MK
  'zdravstvo','zdravlje','biotehnologija','farmacija','bolnica','klinika','vakcina','kliničko ispitivanje','odobrenje',
  'shëndet','shëndetësi','bioteknologji','farmaci','spital','vaksinë','studim klinik','miratim',
  'zdravje','zdravstvo','biotehnologija','farmacija','bolnišnica','cepljenje','klinično preizkušanje','odobritev','здравство',
  -- ZH/JA/KO
  '医疗','健康','生物技术','制药','疫苗','临床试验','批准','医院',
  '医療','健康','バイオテクノロジー','製薬','ワクチン','治験','承認','病院',
  '의료','건강','바이오테크','제약','백신','임상시험','승인','병원'
] WHERE key = 'healthcare';

-- =========================
-- ENERGY
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'energy','power','electricity','grid','transmission','renewable','solar','pv','wind','hydro','geothermal',
  'nuclear','reactor','uranium','coal','lignite','gas','lng','pipeline','refinery','battery','storage','hydrogen','fuel cell','opec','opec+','price cap',
  -- DE (rich)
  'energie','strom','stromnetz','übertragung','erneuerbar','solar','pv','wind','wasserkraft','geothermie',
  'kernenergie','reaktor','uran','kohle','braunkohle','gas','lng','pipeline','raffinerie','batterie','speicher','wasserstoff','brennstoffzelle','opec',
  -- FR/ES/IT
  'énergie','électricité','réseau','renouvelable','solaire','éolien','nucléaire','pétrole','gaz','batterie','hydrogène',
  'energía','electricidad','red','renovable','solar','eólica','nuclear','petróleo','gas','batería','hidrógeno',
  'energia','elettricità','rete','rinnovabile','solare','eolico','nucleare','petrolio','gas','batteria','idrogeno',
  -- TR/RU
  'enerji','elektrik','şebeke','yenilenebilir','güneş','rüzgar','nükleer','petrol','gaz','lng','boru hattı','hidrojen','batarya',
  'энергия','электроэнергия','сеть','возобновляемая','солнечная','ветровая','атомная','нефть','газ','спг','трубопровод','водород','аккумулятор',
  -- AR/FA/HE
  'طاقة','كهرباء','شبكة','متجدد','شمسي','رياح','نووي','نفط','غاز','غاز مسال','هيدروجين','بطارية',
  'انرژی','برق','شبکه','تجدیدپذیر','خورشیدی','بادی','هسته‌ای','نفت','گاز','ال‌ان‌جی','هیدروژن','باتری',
  'אנרגיה','חשמל','רשת','מתחדש','שמש','רוח','גרעיני','נפט','גז','מימן','סוללה',
  -- HI/UR
  'ऊर्जा','बिजली','ग्रिड','नवीकरणीय','सौर','पवन','परमाणु','तेल','गैस','हाइड्रोजन','बैटरी',
  'توانائی','بجلی','گرڈ','قابل تجدید','شمسی','ہوا','ایٹمی','تیل','گیس','ہائیڈروجن','بیٹری',
  -- SR/HR/BS/SQ/SL/MK
  'energija','električna energija','mreža','obnovljivi','solarna','vjetar','nuklearna','nafta','gas','hidrogen','baterija',
  'energji','energji elektrike','rrjet','burime të rinovueshme','diellore','erë','bërthamore','naftë','gaz','hidrogjen','bateri',
  'energija','omrežje','obnovljivi viri','sončna','vetrna','jedrska','nafta','plin','vodik','baterija','енергија','електрична','мрежа','обновливи','сончева','ветер',
  -- ZH/JA/KO
  '能源','电力','电网','可再生能源','太阳能','风能','核能','石油','天然气','液化天然气','氢能','电池',
  'エネルギー','電力','送電網','再生可能エネルギー','太陽光','風力','原子力','石油','ガス','水素','電池',
  '에너지','전기','전력망','재생에너지','태양광','풍력','원자력','석유','가스','수소','배터리'
] WHERE key = 'energy';

-- =========================
-- CONSUMER
-- =========================
UPDATE wm_topic SET keywords = ARRAY[
  -- EN (rich)
  'consumer','retail','ecommerce','e-commerce','omnichannel','store','chain','supermarket','grocery','apparel','footwear',
  'beverage','food','restaurant','quick service','fast food','delivery','same-day','subscription','loyalty','promo','discount','seasonal sales',
  -- DE (rich)
  'verbraucher','einzelhandel','onlinehandel','e-commerce','omnichannel','filiale','kette','supermarkt','lebensmittel',
  'bekleidung','schuhe','getränke','lebensmittelhandel','gastronomie','schnellrestaurant','lieferservice','abo','kundentreue',
  'rabatt','aktion','saisonsale','umsatz',
  -- FR/ES/IT
  'consommateur','commerce de détail','e-commerce','magasin','supermarché','habillement','boisson','restaurant','promotion',
  'consumidor','minorista','comercio electrónico','tienda','supermercado','ropa','calzado','bebida','restaurante','promoción',
  'consumatore','vendita al dettaglio','commercio elettronico','negozio','supermercato','abbigliamento','bevande','ristorante','promozione',
  -- TR/RU
  'tüketici','perakende','e-ticaret','mağaza','süpermarket','giyim','içecek','restoran','kampanya',
  'потребитель','ритейл','интернет-торговля','магазин','супермаркет','одежда','обувь','напитки','ресторан','скидка',
  -- AR/FA/HE
  'مستهلك','تجزئة','تجارة إلكترونية','متجر','سوبرماركت','ملابس','أحذية','مشروبات','مطعم','عرض ترويجي',
  'مصرف‌کننده','خرده‌فروشی','تجارت الکترونیک','فروشگاه','سوپرمارکت','پوشاک','نوشیدنی','رستوران','تخفیف',
  'צרכן','קמעונאות','מסחר אלקטרוני','חנות','סופרמרקט','הלבשה','מסעדה','מבצע',
  -- HI/UR
  'उपभोक्ता','खुदरा','ईकॉमर्स','दुकान','सुपरमार्केट','कपड़े','जूते','पेय','रेस्तरां','छूट',
  'صارف','ریٹیل','ای کامرس','دکان','سپر مارکیٹ','کپڑے','جوتے','مشروبات','ریستوراں','چھوٹ',
  -- SR/HR/BS/SQ/SL/MK
  'potrošač','maloprodaja','e-trgovina','prodavnica','supermarket','odeća','obuća','pića','restoran','akcija','popust',
  'konsumator','shitje me pakicë','e-tregti','dyqan','supermarket','veshje','pije','restorant','zbritje',
  'potrošnik','maloprodaja','spletna trgovina','trgovina','supermarket','oblačila','obutev','pijača','restavracija','popust','потрошувач','малопродажба','продавница','попуст',
  -- ZH/JA/KO
  '消费者','零售','电子商务','商店','超市','服装','鞋类','饮料','餐厅','折扣','促销',
  '消費者','小売','電子商取引','店舗','スーパーマーケット','衣料','靴','飲料','レストラン','割引','プロモーション',
  '소비자','소매','전자상거래','매장','슈퍼마켓','의류','신발','음료','레스토랑','할인','프로모션'
] WHERE key = 'consumer';
