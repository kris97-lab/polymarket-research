"""Match start time (gameStartTime) and actual close time for esports markets from the public Gamma API (batched, cached).
Output: data/processed/esports_game_start.parquet with market_id, game_start_ts, closed_ts, clear_book_on_start."""
import os, json, time, datetime as dt, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor
import duckdb, polars as pl

CACHE = "data/cache/gamma_markets_esports"
BATCH = 50


def ts(s):
    if not s:
        return None
    s = s.replace(" ", "T").replace("+00", "+00:00") if s.endswith("+00") else s.replace(" ", "T").replace("Z", "+00:00")
    return int(dt.datetime.fromisoformat(s).timestamp())


def fetch(ids, tries=5):
    url = "https://gamma-api.polymarket.com/markets?closed=true&limit=%d&%s" % (len(ids), "&".join(f"id={i}" for i in ids))
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "research-pipeline"})
            return [{"market_id": str(m["id"]), "game_start": m.get("gameStartTime"), "closed": m.get("closedTime"),
                     "clear_book": m.get("clearBookOnStart")} for m in json.load(urllib.request.urlopen(req, timeout=60))]
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            time.sleep(2 ** k)
    raise RuntimeError(f"failed batch starting {ids[0]}")


def run_batch(args):
    n, ids = args
    path = f"{CACHE}/{n:05d}.jsonl"
    if not os.path.exists(path):
        rows = fetch(ids)
        with open(path + ".tmp", "w") as f:
            f.writelines(json.dumps(r) + "\n" for r in rows)
        os.rename(path + ".tmp", path)


if __name__ == "__main__":
    os.makedirs(CACHE, exist_ok=True)
    ids = [r[0] for r in duckdb.connect("data/polymarket.duckdb", read_only=True).sql(
        "select market_id from markets where category = 'esports' and source = 'hf' order by 1").fetchall()]
    with ThreadPoolExecutor(4) as ex:
        list(ex.map(run_batch, enumerate(ids[i:i + BATCH] for i in range(0, len(ids), BATCH))))
    rows = [json.loads(l) for f in sorted(os.listdir(CACHE)) if f.endswith(".jsonl") for l in open(f"{CACHE}/{f}")]
    out = pl.DataFrame([dict(market_id=r["market_id"], game_start_ts=ts(r["game_start"]), closed_ts=ts(r["closed"]),
                             clear_book_on_start=r["clear_book"]) for r in rows],
                       schema=dict(market_id=pl.Utf8, game_start_ts=pl.Int64, closed_ts=pl.Int64, clear_book_on_start=pl.Boolean))
    out.unique("market_id").write_parquet("data/processed/esports_game_start.parquet")
    print(len(ids), "requested,", out.height, "returned,", out["game_start_ts"].null_count(), "without gameStartTime")
