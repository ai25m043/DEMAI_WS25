# bridge/bridge.py
import os, sys, time, json, re, traceback
import requests
from sseclient import SSEClient
from kafka import KafkaProducer, errors as kerr

EVENTS_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.getenv("WIKI_USER_AGENT", "WM-Live-Bridge/0.3 (contact: you@example.com)")
TOPIC = os.getenv("KAFKA_TOPIC", "wm-recentchange")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))

MW_API_TMPL = "https://{server}/w/api.php"
WD_API = "https://www.wikidata.org/w/api.php"
WD_ENTITY_TMPL = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"

QID_CACHE = {}            # (server_name,title) -> qid
SITELINK_CACHE = {}       # (site,title) -> qid
ENTITY_CACHE = {}         # qid -> entity json
COUNTRY_CODE_CACHE = {}   # country_qid -> ISO2

RX_QID = re.compile(r"^Q\d+$", re.I)

def log(msg: str): print(f"[bridge] {msg}", flush=True)

# ---------------- Kafka & SSE ----------------
def make_producer(max_wait_sec=120):
    start = time.time(); attempt = 0
    while True:
        attempt += 1
        try:
            log(f"Kafka connect → {BOOTSTRAP} (attempt {attempt})")
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                retries=5,
                acks="all",
                request_timeout_ms=15000,
                max_in_flight_requests_per_connection=1,
            )
            parts = p.partitions_for(TOPIC)
            log(f"Kafka OK. topic={TOPIC} partitions={parts}")
            return p
        except kerr.NoBrokersAvailable as e:
            if time.time() - start > max_wait_sec:
                log("ERROR: Kafka not reachable within timeout")
                raise
            sleep_s = min(5, 1 + attempt)
            log(f"Kafka not ready ({e}). retry in {sleep_s}s…")
            time.sleep(sleep_s)

def make_sse_client(session: requests.Session) -> SSEClient:
    log(f"SSE connect → {EVENTS_URL}")
    resp = session.get(EVENTS_URL, stream=True, timeout=60)
    if resp.status_code == 403:
        log("WARN: 403 from EventStreams. Set WIKI_USER_AGENT with contact info.")
        log(f"UA={USER_AGENT}")
    resp.raise_for_status()
    log("SSE connected.")
    return SSEClient(resp)

# ---------------- Wikidata helpers ----------------
def fetch_entity(session: requests.Session, qid: str) -> dict | None:
    if qid in ENTITY_CACHE: return ENTITY_CACHE[qid]
    try:
        r = session.get(
            WD_ENTITY_TMPL.format(qid=qid),
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        ent = (r.json().get("entities") or {}).get(qid)
        if ent:
            ENTITY_CACHE[qid] = ent
            return ent
    except Exception as e:
        log(f"fetch_entity error for {qid}: {e}")
        return None
    return None

def claim_ids(entity: dict, pid: str) -> list[str]:
    out = []
    for clm in (entity.get("claims") or {}).get(pid, []) or []:
        dv = (clm.get("mainsnak") or {}).get("datavalue", {}).get("value")
        if isinstance(dv, dict) and dv.get("id"):
            out.append(dv["id"])
    return out

def has_instance_of_country(entity: dict) -> bool:
    # P31 (instance of) contains Q6256 (country)?
    for q in claim_ids(entity, "P31"):
        if q.upper() == "Q6256":
            return True
    return False

def iso2_for_country_qid(session: requests.Session, country_qid: str) -> str | None:
    iso2 = COUNTRY_CODE_CACHE.get(country_qid)
    if iso2: return iso2
    cent = fetch_entity(session, country_qid)
    if not cent: return None
    for clm in (cent.get("claims") or {}).get("P297", []) or []:  # ISO-2
        dv = (clm.get("mainsnak") or {}).get("datavalue", {}).get("value")
        if isinstance(dv, str) and len(dv) == 2:
            iso2 = dv.upper()
            COUNTRY_CODE_CACHE[country_qid] = iso2
            return iso2
    return None

def ascend_p131_to_country(session: requests.Session, qid: str, max_hops: int = 4) -> tuple[str|None, str|None]:
    """
    Follow P131 (located in admin territory) up to a country (P31=Q6256) and return (country_qid, iso2).
    """
    cur = qid
    for _ in range(max_hops):
        ent = fetch_entity(session, cur)
        if not ent:
            return None, None
        # If this node is a country, done
        if has_instance_of_country(ent):
            iso2 = iso2_for_country_qid(session, cur)
            return cur, iso2
        # If this node has direct country P17, use that
        p17 = claim_ids(ent, "P17")
        if p17:
            cqid = p17[0]
            iso2 = iso2_for_country_qid(session, cqid)
            return cqid, iso2
        # Otherwise, climb one step via P131
        p131 = claim_ids(ent, "P131")
        if not p131:
            return None, None
        cur = p131[0]
    return None, None

# ---------------- QID resolution ----------------
def fetch_page_qid_pageprops(session: requests.Session, server_name: str, title: str) -> str | None:
    key = (server_name, title)
    if key in QID_CACHE: return QID_CACHE[key]
    api = MW_API_TMPL.format(server=server_name)
    try:
        r = session.get(
            api,
            params={
                "action": "query",
                "format": "json",
                "prop": "pageprops",
                "redirects": "1",
                "titles": title,
                "ppprop": "wikibase_item",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        for _, p in (data.get("query", {}).get("pages", {}) or {}).items():
            qid = (p.get("pageprops") or {}).get("wikibase_item")
            if qid:
                QID_CACHE[key] = qid
                return qid
    except Exception as e:
        log(f"pageprops QID error: {e} server={server_name} title={title}")
    return None

def fetch_qid_via_sitelink(session: requests.Session, site: str, title: str) -> str | None:
    key = (site, title)
    if key in SITELINK_CACHE: return SITELINK_CACHE[key]
    try:
        r = session.get(
            WD_API,
            params={
                "action": "wbgetentities",
                "format": "json",
                "sites": site,       # e.g. 'enwiki'
                "titles": title,
                "props": "info",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        ents = (r.json().get("entities") or {})
        for k, v in ents.items():
            if k != "-1":
                SITELINK_CACHE[key] = k
                return k
    except Exception as e:
        log(f"sitelink QID error: {e} site={site} title={title}")
    return None

# ---------------- Main enrichment ----------------
def enrich_country(session: requests.Session, rc: dict) -> dict:
    # We only keep **Wikipedia article** edits: namespace 0 and server *.wikipedia.org
    server = (rc.get("server_name")
              or rc.get("server_url","").replace("https://","").replace("http://",""))
    if not server or not server.endswith("wikipedia.org"):
        rc["_skip_reason"] = "non-wikipedia"
        return rc

    if rc.get("ns", rc.get("namespace", 0)) != 0:
        rc["_skip_reason"] = "non-article"
        return rc

    title  = rc.get("title")
    if not title:
        rc["_skip_reason"] = "no-title"
        return rc

    rc["page_qid"] = rc["page_country_qid"] = rc["page_country_code"] = None
    rc["geo_method"] = rc["geo_confidence"] = None

    # 1) Try pageprops on that wiki
    qid = fetch_page_qid_pageprops(session, server, title)

    # 2) Fallback: use sitelink via rc['wiki'] (e.g., 'enwiki')
    if not qid:
        site = rc.get("wiki")  # short key like 'enwiki'
        if isinstance(site, str) and site.endswith("wiki"):
            qid = fetch_qid_via_sitelink(session, site, title)

    # If still no QID, drop
    if not qid:
        rc["_skip_reason"] = "no-qid"
        return rc

    rc["page_qid"] = qid

    # Country resolution:
    # a) Direct P17
    ent = fetch_entity(session, qid)
    country_qid = None; iso2 = None
    if ent:
        p17 = claim_ids(ent, "P17")
        if p17:
            country_qid = p17[0]
            iso2 = iso2_for_country_qid(session, country_qid)

    # b) Ascend P131 → country (if no P17 or no ISO2)
    if not iso2:
        cqid, ciso2 = ascend_p131_to_country(session, qid, max_hops=4)
        country_qid = country_qid or cqid
        iso2 = iso2 or ciso2

    if country_qid and iso2 and len(iso2) == 2:
        rc["page_country_qid"]  = country_qid
        rc["page_country_code"] = iso2
        rc["geo_method"] = "P17" if p17 else "P131_chain"
        rc["geo_confidence"] = 100 if rc["geo_method"] == "P17" else 80
    else:
        rc["_skip_reason"] = "no-country"

    return rc

# ---------------- App ----------------
def main():
    log("bridge starting…")
    log(f"ENV BOOTSTRAP={BOOTSTRAP} TOPIC={TOPIC} UA={USER_AGENT}")
    prod = make_producer()

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/event-stream",  # for SSE; JSON calls override this
        "Cache-Control": "no-cache",
    })

    backoff = 1.0
    last_beat = time.time()
    seen = sent = dropped = 0

    while True:
        try:
            client = make_sse_client(session)
            for ev in client.events():
                if ev.event != "message" or not ev.data:
                    continue
                seen += 1
                try:
                    rc = json.loads(ev.data)
                except json.JSONDecodeError:
                    continue

                rc = enrich_country(session, rc)

                # Only forward events that have a country AND are from wikipedia article ns=0
                if rc.get("page_country_code"):
                    prod.send(TOPIC, rc); sent += 1
                else:
                    dropped += 1
                    # Log first few drop reasons to confirm filters
                    if dropped <= 10:
                        log(f"dropped: reason={rc.get('_skip_reason')} title={rc.get('title')} server={rc.get('server_name')}")

                now = time.time()
                if now - last_beat > 5:
                    log(f"heartbeat: seen={seen} sent={sent} dropped={dropped}")
                    last_beat = now

            log("SSE stream ended; reconnecting…")
            time.sleep(backoff); backoff = min(backoff * 2, 30.0)

        except requests.HTTPError as e:
            code = e.response.status_code if e.response else "?"
            log(f"HTTPError from SSE: {code} {e}")
            if code == 403:
                log("Hint: set a real WIKI_USER_AGENT with contact email/URL.")
            time.sleep(backoff); backoff = min(backoff * 2, 30.0)
        except Exception:
            log("ERROR (outer loop):")
            traceback.print_exc(file=sys.stdout)
            time.sleep(backoff); backoff = min(backoff * 2, 30.0)

if __name__ == "__main__":
    main()
