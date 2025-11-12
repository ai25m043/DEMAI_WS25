# 🧩 User Stories — Wikimedia–Stock Market Correlation Platform

---

### 👩‍💻 User Story 1 — Data Scientist
**As a data scientist**,  
I want to **analyze live Wikipedia edit activity enriched with country and topic metadata**,  
so that I can **quantify how public attention and controversies correlate with stock market fluctuations** in real time.

**Acceptance Criteria:**
- Edits are enriched with `country` (ISO2), `topics`, and `timestamp`.
- Data is stored and queryable in PostgreSQL.
- I can visualize per-country edit spikes alongside average stock returns on the Streamlit dashboard.

---

### 💹 User Story 2 — Financial Analyst / Trader
**As a trader**,  
I want to **see when specific topics (e.g., “war”, “economics”, “energy”) surge in Wikipedia edits**  
so that I can **identify potential global events that may move markets** before they reach mainstream news.

**Acceptance Criteria:**
- The dashboard shows per-country, per-topic edit counts in near real time.
- Stock price trends are overlaid or color-coded on the same map.
- Historical correlations between edit surges and price movements are accessible.

---

### 📰 User Story 3 — Journalist / Researcher
**As a journalist**,  
I want to **track Wikipedia pages with rapidly increasing edits** to detect **emerging controversies or crises**,  
so that I can **discover stories and verify how public attention evolves globally**.

**Acceptance Criteria:**
- The system highlights “edit spikes” by country and topic.
- I can filter by topic (e.g., “politics” or “healthcare”).
- Recent edits include article titles, users, and timestamps for source verification.

---

### 🧭 User Story 4 — Policy Analyst / Government Advisor
**As a policymaker or risk analyst**,  
I want to **monitor information trends that indicate regional instability or sudden interest shifts**,  
so that I can **assess geopolitical or economic risks faster than with traditional intelligence feeds**.

**Acceptance Criteria:**
- I can view aggregated edit activity by country, topic, and time.
- Correlation with local stock index movements is clearly visualized.
- Reports can be exported for further risk assessment or modeling.

---

### ⚙️ User Story 5 — Data Engineer / DevOps
**As a data engineer**,  
I want to **run a robust and maintainable data pipeline** from **Wikimedia → Kafka → Spark → PostgreSQL → Streamlit**,  
so that the system **stays reliable, observable, and easy to scale** as the data grows.

**Acceptance Criteria:**
- The bridge reconnects automatically on SSE disconnects.
- Kafka and Spark handle backpressure and maintain message integrity.
- PostgreSQL schema supports indexed queries by time, country, and topic.
- Monitoring logs (`seen`, `sent`, `dropped`) and metrics are visible for health checks.

---

## 🌐 Core Vision
> **If a country’s Wikipedia pages experience a surge in edits on sensitive topics (politics, war, economics),  
> this likely reflects real-world controversy or attention shifts — which can precede or correlate with stock market changes.**

This project turns **human attention signals** into **quantifiable, geo-aware economic indicators**.


# 🌍 Wikimedia → Kafka Bridge — Data Flow Summary

## 1️⃣ Data Sources

| Source                           | Description                                                         | Example Fields                                                                 |
| -------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| **Wikimedia EventStreams (SSE)** | Real-time feed of all Wikipedia edits                               | `title`, `wiki`, `server_name`, `namespace`, `type`, `user`, `timestamp`, etc. |
| **MediaWiki API (per wiki)**     | Used to find the linked Wikidata item (QID) for each Wikipedia page | `/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item`                  |
| **Wikidata API / EntityData**    | Used to fetch entity relationships such as country and ISO codes    | `/wiki/Special:EntityData/Q123.json`                                           |

---

## 2️⃣ Event Ingestion

- The bridge connects to **Wikimedia RecentChange Stream** via **Server-Sent Events (SSE)**.
- Each incoming event represents **a single Wikipedia edit**.
- Events arrive as **JSON objects** and are parsed and counted (`seen`, `sent`, `dropped`).

---

## 3️⃣ Filtering

Before enrichment, events are filtered to keep only meaningful data:

✅ **Keep only**
- Pages from `*.wikipedia.org` (ignore Commons, Wikidata, MetaWiki)
- Namespace `0` (main article space — real encyclopedia pages)
- Events that have a valid `title`

❌ **Drop if**
- Non-Wikipedia project (`commons`, `wikidata`, etc.)
- Talk/category/file pages (`ns ≠ 0`)
- Missing title or Wikidata link

Each dropped event stores `_skip_reason` such as `non-wikipedia`, `non-article`, or `no-qid`.

# 🧾 Example — Wikimedia RecentChange Event

## 1️⃣ Raw event (from EventStreams)
This is the JSON structure directly received from the Wikimedia SSE stream:

``` json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "uri": "https://en.wikipedia.org/wiki/Verve_Coffee_Roasters",
    "request_id": "73110c02-1024-475c-b717-9b5b7a3af729",
    "id": "70b4b643-aa3f-4725-a90a-88859c4cc158",
    "domain": "en.wikipedia.org",
    "stream": "mediawiki.recentchange",
    "dt": "2025-11-11T15:48:37.471Z",
    "topic": "codfw.mediawiki.recentchange",
    "partition": 0,
    "offset": 1713625115
  },
  "id": 1964639494,
  "type": "edit",
  "namespace": 0,
  "title": "Verve Coffee Roasters",
  "title_url": "https://en.wikipedia.org/wiki/Verve_Coffee_Roasters",
  "comment": "Updated Verve's Japan locations for accuracy and added citation for 2023 award.",
  "timestamp": 1762876115,
  "user": "Jd9607",
  "bot": false,
  "minor": false,
  "length": { "old": 4637, "new": 5087 },
  "revision": { "old": 1314143421, "new": 1321610309 },
  "server_url": "https://en.wikipedia.org",
  "server_name": "en.wikipedia.org",
  "server_script_path": "/w",
  "wiki": "enwiki"
}
```

---

## 4️⃣ Enrichment: Adding Country Information

For every valid Wikipedia edit, the bridge performs **semantic enrichment** using Wikidata.

### Step 1 — Find the page’s Wikidata item (QID)
- Query the local wiki’s **MediaWiki API** (`pageprops`) to get `wikibase_item` (e.g., `Q17030230`).
- If missing, fallback to **Wikidata sitelink lookup** (`wbgetentities`).

### Step 2 — Fetch the Wikidata entity
- Retrieve full claims for that QID via the **Wikidata EntityData API**.

### Step 3 — Determine the country
- If entity has property **`P17` (country)** → use directly.
- Else, climb location hierarchy via **`P131` (“located in the administrative territory”)** up to 4 levels until a country is found.
- Fetch ISO code from **`P297`** (ISO-3166-1 alpha-2).

### Step 4 — Add geo fields

| Added field         | Example                  | Description                      |
| ------------------- | ------------------------ | -------------------------------- |
| `page_qid`          | `"Q17030230"`            | Wikidata item for the page       |
| `page_country_qid`  | `"Q30"`                  | Wikidata item for the country    |
| `page_country_code` | `"US"`                   | ISO-2 country code               |
| `geo_method`        | `"P17"` / `"P131_chain"` | Method used to infer the country |
| `geo_confidence`    | `100` / `80`             | Confidence score                 |

``` json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "uri": "https://en.wikipedia.org/wiki/Verve_Coffee_Roasters",
    "id": "70b4b643-aa3f-4725-a90a-88859c4cc158",
    "dt": "2025-11-11T15:48:37.471Z"
  },
  "type": "edit",
  "namespace": 0,
  "title": "Verve Coffee Roasters",
  "comment": "Updated Verve's Japan locations for accuracy and added citation for 2023 award.",
  "timestamp": 1762876115,
  "user": "Jd9607",
  "bot": false,
  "minor": false,
  "length": { "old": 4637, "new": 5087 },
  "server_name": "en.wikipedia.org",
  "server_url": "https://en.wikipedia.org",
  "server_script_path": "/w",
  "wiki": "enwiki",

  "page_qid": "Q17030230",               // Wikidata ID for "Verve Coffee Roasters"
  "page_country_qid": "Q30",             // Wikidata ID for "United States"
  "page_country_code": "US",             // ISO 3166-1 alpha-2 code
  "geo_method": "P17",                   // Property used: "country" (P17)
  "geo_confidence": 100,                 // Confidence score
  "_skip_reason": null                   // Not dropped
}

```

---

## 5️⃣ Output to Kafka

- Each enriched event is serialized as **JSON** and sent to Kafka topic **`wm-recentchange`**.
- Only events with a valid `page_country_code` are forwarded.
- Kafka producer uses safe delivery (`acks=all`, retries, backoff).
- Heartbeat logs show pipeline health: `seen`, `sent`, `dropped`.

---

## 6️⃣ Spark Stream Processing (Downstream)

- **Spark Structured Streaming** subscribes to the same Kafka topic.
- It parses the JSON events and stores them into **PostgreSQL (`wm_recent_change`)**.
- Spark classifies edits by **topic** (finance, politics, war, etc.) using keyword matching.
- Aggregations are created by **country**, **topic**, and **minute** for dashboards and analytics.

# ⚡ Spark Stream — Topic Tagging & Persisting Wikimedia Events

This Spark job consumes **enriched Wikimedia edit events** from Kafka, assigns **topics** (e.g., `war`, `finance`, `politics`) based on a keyword dictionary in Postgres, and persists only the **country-resolved & topic-matched** rows into `wm_recent_change`.

---

## 🔌 Inputs & Outputs

- **Input stream:** Kafka topic `wm-recentchange`  
  - Messages are JSON **already enriched by the bridge** with:
    - `page_country_code` (ISO2), `page_qid`, `page_country_qid`, `geo_method`, `geo_confidence`
- **Lookup table (Postgres):** `wm_topic`
  - Columns: `key` (topic id), `enabled` (bool), `keywords` (text[])
  - Rows with `key = 'other'` are **ignored**; only `enabled = true` are used
- **Output table (Postgres):** `wm_recent_change`
  - One row per **unique event** (`meta_id`), with `topics` stored as `text[]`

---

## 🧠 Topic Dictionary Load

1. The job loads `wm_topic` via JDBC.
2. It **filters** to `enabled = true` and `key != 'other'`.
3. It **explodes** `keywords`, lowercases them to `kw_lc`, and **broadcasts** this small lookup for fast joins.

> Why broadcast? Keeps the join local and efficient for every micro-batch.

---

## 📥 Stream Ingestion (Kafka → Spark)

- Spark Structured Streaming subscribes to `wm-recentchange` with:
  - `startingOffsets` (default `latest`)
  - `kafka.group.id` for consumer group
  - `failOnDataLoss = false` for resilience
- Each micro-batch calls `write_batch(df, batch_id)` via `foreachBatch`.

---

## 🧾 JSON Parsing & Early Filters

1. Cast Kafka `value` to `string`, parse JSON using a fixed **schema** (`wm_schema`).
2. Keep the original JSON string as `raw` (for debugging/tracing).
3. **Country precondition:** filter to rows where `page_country_code IS NOT NULL`.
4. Materialize:
   - `meta_id = meta.id`
   - `dt = to_timestamp(meta.dt)` (stored as `dt_ts` internally)

> Only **country-resolved** events proceed (bridge guarantees geo enrichment).

---

## 🔤 Tokenization & Topic Matching (Exact-Token)

1. Build a **lowercased text** field: `title + " " + comment`.
2. Tokenize with `split('[^a-z0-9]+')`, remove empties, `array_distinct`.
3. **Explode tokens** and **join** to the broadcasted keyword table on **exact token = keyword**.
   - This avoids false positives like `"award"` matching `"war"`.

> Exact-token matching ensures **precision** over naive substring search.

---

## 🔁 Aggregate to One Row per Event

- Group by `meta_id`; for each event:
  - `first(struct(...))` to reassemble the event fields
  - `collect_set(topic_key)` to gather all matched topics
- Filter to **events with at least one topic** (`size(topics) > 0`).

> Result: Each event is labeled with **one or more topics** from `wm_topic`.

---

## 🗄️ Persist to Postgres (JDBC)

- Selected columns are cast to DB types and written `mode('append')` to `wm_recent_change`:
  - Identifiers & times: `meta_id`, `dt`, `timestamp_unix`
  - Wiki context: `wiki`, `server_*`
  - Page/change metadata: `title`, `namespace`, `type`, `comment`, `user_text`, `bot`, `minor`, `patrolled`
  - Size deltas: `old_len`, `new_len`, `added`, `removed`
  - Geo: `page_qid`, `page_country_qid`, `page_country_code`, `geo_method`, `geo_confidence`
  - Placeholders for geo-coordinates: `page_lat`, `page_lon`, `page_geohash` (currently `NULL`)
  - Original JSON: `raw` as `string` (DB `jsonb` column will parse it)
  - **Topics:** `topics` (text array)

> The table has indexes for time, country, and topics; views aggregate by minute/country for the dashboard.

---

## ♻️ Checkpointing & Reliability

- Streaming query uses `checkpointLocation` (e.g., `/tmp/chk-wm-topics`) to track progress.
- If the job restarts, it **resumes** from the last committed offsets.
- The job prints micro-batch counts (`rows=n`) and shows a sample (`out.show(5)`).

---

## 🔧 Configuration (Env Vars)

- Kafka: `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `KAFKA_STARTING_OFFSETS`
- Postgres: `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS`
- Checkpoint: `CHECKPOINT`
- Spark forces `spark.sql.session.timeZone=UTC` to keep timestamps consistent.

---

## 🔍 Why This Design?

- **Country-first filter** keeps storage clean and analytics geo-aware.
- **Exact-token topic matching** yields **reliable labels** (no noisy substring matches).
- **Broadcasted keywords** scales well for real-time streaming.
- **Single-row per event** + `topics[]` makes downstream aggregation simple.

---

## 🧩 End-to-End in One Glance

```mermaid
flowchart LR
  K[Kafka: wm-recentchange] --> S[Spark foreachBatch]
  S --> P[Parse JSON + country filter]
  P --> T[Tokenize title+comment]
  T --> J[Join exact token ↔ topic keywords (broadcast)]
  J --> G[Group by meta_id → collect topics]
  G --> F[Filter size(topics) > 0]
  F --> W[JDBC append → Postgres: wm_recent_change]


---




## 🧠 Summary — End-to-End Flow

| Stage                   | Role                                             | Output                        |
| ----------------------- | ------------------------------------------------ | ----------------------------- |
| **EventStreams (SSE)**  | Live Wikipedia edits                             | Raw JSON events               |
| **Bridge.py**           | Filters + enriches with Wikidata (adds geo info) | Enriched JSON → Kafka         |
| **Kafka Topic**         | Reliable message bus                             | Persistent event stream       |
| **Spark Job**           | Parses, tags topics, stores to DB                | PostgreSQL `wm_recent_change` |
| **Streamlit Dashboard** | Visualization layer                              | Country map + live trends     |

---

## 🚀 Key Insight

This pipeline transforms **global Wikipedia edit activity** into **geo-localized, topic-aware data signals**  
that can be **correlated with financial market changes** —  
revealing how real-time **information attention and controversy** may influence economies and stock movements.
