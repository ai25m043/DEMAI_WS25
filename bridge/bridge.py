# bridge/bridge.py
import os, sys, time, json, traceback
import requests
from sseclient import SSEClient
from kafka import KafkaProducer, errors as kerr

EVENTS_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.getenv("WIKI_USER_AGENT", "WM-Live-Bridge/0.1 (contact: you@example.com)")
TOPIC = os.getenv("KAFKA_TOPIC", "wm-recentchange")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))

def log(msg): print(f"[bridge] {msg}", flush=True)

def make_producer(max_wait_sec=120):
    start = time.time(); attempt = 0
    while True:
        attempt += 1
        try:
            log(f"connecting to Kafka at {BOOTSTRAP} (attempt {attempt})")
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                retries=5,
                acks="all",                    # be strict so delivery errors surface
                request_timeout_ms=15000,
                max_in_flight_requests_per_connection=1,  # simpler error semantics
            )
            # Verify metadata & partitions visible
            parts = p.partitions_for(TOPIC)
            log(f"metadata: topic={TOPIC} partitions={parts}")
            return p
        except kerr.NoBrokersAvailable:
            if time.time() - start > max_wait_sec:
                log("ERROR: Kafka not reachable within timeout"); raise
            sleep_s = min(5, 1 + attempt)
            log(f"Kafka not ready, retrying in {sleep_s}s…")
            time.sleep(sleep_s)

def make_sse_client(session: requests.Session) -> SSEClient:
    log(f"connecting to Wikimedia SSE: {EVENTS_URL}")
    resp = session.get(EVENTS_URL, stream=True, timeout=60)
    resp.raise_for_status()
    return SSEClient(resp)

def main():
    log(f"starting bridge → topic={TOPIC}")
    producer = make_producer()

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    })

    backoff = 1.0
    while True:
        try:
            client = make_sse_client(session)
            n = 0
            pending = []
            for ev in client.events():
                if ev.event != "message" or not ev.data:
                    continue
                try:
                    rc = json.loads(ev.data)
                except json.JSONDecodeError:
                    continue
                if rc.get("ns", 0) < 0:
                    continue

                fut = producer.send(TOPIC, rc)
                pending.append(fut)
                n += 1

                # every 50 records: wait on futures and flush
                if n % 50 == 0:
                    errs = 0
                    for f in pending:
                        try:
                            md = f.get(timeout=10)   # raises on delivery error
                        except Exception as e:
                            errs += 1
                            log(f"[deliver-err] {type(e).__name__}: {e}")
                    pending.clear()
                    producer.flush()
                    log(f"produced {n} messages so far… (errs in last batch={errs})")
            backoff = 1.0
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                log("[warn] 403 Forbidden – set WIKI_USER_AGENT with contact info.")
                log(f"[warn] Current User-Agent: {USER_AGENT}")
            else:
                log(f"[warn] HTTP error: {e}")
            time.sleep(backoff); backoff = min(backoff * 2, 30.0)
        except Exception:
            log("ERROR receiving/sending:")
            traceback.print_exc(file=sys.stdout)
            time.sleep(backoff); backoff = min(backoff * 2, 30.0)

if __name__ == "__main__":
    main()
