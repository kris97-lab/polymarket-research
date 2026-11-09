"""Fetch event tags from the public Gamma API for all universe events (batched by id, cached, resumable)."""
import sys, os, json, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor
import duckdb
sys.path.insert(0, "ingest")
from universe import universe_sql

CACHE = "data/cache/gamma_events"
BATCH = 50


def fetch(ids, tries=5):
    url = "https://gamma-api.polymarket.com/events?limit=%d&%s" % (len(ids), "&".join(f"id={i}" for i in ids))
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "research-pipeline"})
            data = json.load(urllib.request.urlopen(req, timeout=60))
            return [{"event_id": str(e["id"]), "event_slug": e.get("slug"),
                     "tags": [t.get("slug") for t in e.get("tags") or []]} for e in data]
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as ex:
            time.sleep(2 ** k)
    raise RuntimeError(f"failed batch starting {ids[0]}")


def run_batch(args):
    n, ids = args
    path = f"{CACHE}/{n:05d}.jsonl"
    if os.path.exists(path):
        return 0
    rows = fetch(ids)
    with open(path + ".tmp", "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)
    os.rename(path + ".tmp", path)
    return len(rows)


if __name__ == "__main__":
    os.makedirs(CACHE, exist_ok=True)
    ids = [r[0] for r in duckdb.sql(f"select distinct event_id from ({universe_sql(start='2025-07-01', end='2026-10-05')}) order by 1").fetchall()]
    batches = list(enumerate(ids[i:i + BATCH] for i in range(0, len(ids), BATCH)))
    got = 0
    with ThreadPoolExecutor(4) as ex:
        for i, n in enumerate(ex.map(run_batch, batches)):
            got += n
            if i % 200 == 0:
                print(f"batch {i}/{len(batches)} fetched={got}", flush=True)
    print("done", len(ids), "events requested")


def fetch_gap_events():
    """Tags for events of gap-window markets (from data_api_gap.py), cached as gap_*.jsonl."""
    gap = json.load(open("data/cache/data_api/gap_markets.json"))
    have = set(duckdb.sql("select distinct event_id from read_json('data/cache/gamma_events/*.jsonl', format='newline_delimited')").fetchall())
    ids = sorted({m["event_id"] for m in gap if m["event_id"] not in ("None", None)} - {r[0] for r in have})
    for n, i in enumerate(range(0, len(ids), BATCH)):
        path = f"{CACHE}/gap_{n:05d}.jsonl"
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.writelines(json.dumps(r) + "\n" for r in fetch(ids[i:i + BATCH]))
    print("gap events:", len(ids))
