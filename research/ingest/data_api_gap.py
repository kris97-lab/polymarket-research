"""Fill the gap after the HF snapshot (2026-10-05 .. now) from public Polymarket APIs, cached locally.

1) Gamma /markets: markets resolved in the gap window (split by day so offsets stay small).
2) Data API v2 /trades?condition=..&takerOnly=false: cursor pagination newest->oldest, stop at the cutoff.
   Rows are per wallet-side (maker fills + taker summary), like users.parquet.
"""
import os, sys, json, time, datetime as dt, urllib.request, urllib.parse, urllib.error
from concurrent.futures import ThreadPoolExecutor
import duckdb

GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com/v2"
CACHE = "data/cache/data_api"
CUTOFF = int(dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc).timestamp())


def get(url, tries=6):
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "research-pipeline"})
            return json.load(urllib.request.urlopen(req, timeout=60))
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504):
                raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            pass
        time.sleep(min(60, 2 ** k))
    raise RuntimeError(url)


def gap_markets(min_volume):
    out, day = [], dt.date(2026, 10, 5)
    while day <= dt.date.today():
        nxt = day + dt.timedelta(days=1)
        off = 0
        while True:
            q = urllib.parse.urlencode({"closed": "true", "end_date_min": f"{day}T00:00:00Z", "end_date_max": f"{nxt}T00:00:00Z",
                                        "volume_num_min": min_volume, "limit": 500, "offset": off})
            page = get(f"{GAMMA}/markets?{q}")
            out += [{"market_id": str(m["id"]), "condition_id": m.get("conditionId"), "slug": m.get("slug"),
                     "question": m.get("question"), "outcomes": m.get("outcomes"), "outcome_prices": m.get("outcomePrices"),
                     "clob_token_ids": m.get("clobTokenIds"), "volume": float(m.get("volumeNum") or 0),
                     "end_date": m.get("endDate"), "created_at": m.get("createdAt"), "neg_risk": m.get("negRisk"),
                     "event_id": str((m.get("events") or [{}])[0].get("id")), "event_slug": (m.get("events") or [{}])[0].get("slug")}
                    for m in page]
            if len(page) < 500:
                break
            off += 500
        day = nxt
    return out


def market_trades(cond):
    path = f"{CACHE}/trades/{cond}.jsonl"
    if os.path.exists(path):
        return 0
    rows, cursor = [], None
    while True:
        q = {"condition": cond, "takerOnly": "false", "limit": 1000}
        if cursor:
            q["cursor"] = cursor
        d = get(f"{DATA}/trades?{urllib.parse.urlencode(q)}")
        page = d.get("data", [])
        rows += [{k: r.get(k) for k in ("proxy_wallet", "side", "token_id", "condition_id", "size", "price",
                                         "timestamp", "outcome", "outcome_index", "transaction_hash")}
                 for r in page if r["timestamp"] >= CUTOFF]
        cursor = d.get("pagination", {}).get("next_cursor")
        if not page or page[-1]["timestamp"] < CUTOFF or not d.get("pagination", {}).get("has_more"):
            break
    with open(path + ".tmp", "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)
    os.rename(path + ".tmp", path)
    return len(rows)


if __name__ == "__main__":
    min_volume = float(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    os.makedirs(f"{CACHE}/trades", exist_ok=True)
    mpath = f"{CACHE}/gap_markets.json"
    if not os.path.exists(mpath):
        json.dump(gap_markets(min_volume), open(mpath, "w"))
    markets = json.load(open(mpath))
    print(f"gap markets (vol>={min_volume:,.0f}): {len(markets)}", flush=True)
    conds = sorted({m["condition_id"] for m in markets if m["condition_id"]})
    done = 0
    with ThreadPoolExecutor(3) as ex:
        for i, n in enumerate(ex.map(market_trades, conds)):
            done += n
            if i % 100 == 0:
                print(f"{i}/{len(conds)} markets, {done:,} rows", flush=True)
    print("done", done)
