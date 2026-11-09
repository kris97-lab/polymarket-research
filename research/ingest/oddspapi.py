"""OddsPapi v4 client for the Pinnacle-vs-Polymarket esports pilot (key read from ODDSPAPI_KEY or the repo's .env, never logged).

Quota (free plan, 250/month): /v4/fixtures counts 1 per call; /v4/historical-odds and /v4/account are free.
Every response is cached on disk, so re-runs cost nothing.
  python ingest/oddspapi.py account                  # check the key and remaining quota
  python ingest/oddspapi.py fixtures 2026-01-01 2026-10-04   # esports fixtures in 9-day windows (billable)
  python ingest/oddspapi.py history                  # Pinnacle + Polymarket odds history for cached fixtures (free, 5 s each)"""
import json, os, sys, time, urllib.error, urllib.parse, urllib.request
import datetime as dt

BASE = "https://api.oddspapi.io/v4"
CACHE = "data/cache/oddspapi"
SPORTS = {16: "dota2", 17: "cs2", 18: "lol", 61: "valorant"}
COOLDOWN = {"fixtures": 2.1, "historical-odds": 5.1, "account": 1.0}


ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")


def key():
    if os.environ.get("ODDSPAPI_KEY"):
        return os.environ["ODDSPAPI_KEY"]
    if os.path.exists(ENV):
        for line in open(ENV):
            if line.startswith("ODDSPAPI_KEY="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("ODDSPAPI_KEY missing: set it in the environment or in .env at the repo root")


def call(endpoint, **params):
    q = urllib.parse.urlencode({**params, "apiKey": key()})
    for k in range(4):
        try:
            req = urllib.request.Request(f"{BASE}/{endpoint}?{q}", headers={"User-Agent": "research-pipeline"})
            out = json.load(urllib.request.urlopen(req, timeout=60))
            time.sleep(COOLDOWN.get(endpoint, 1.0))
            return out
        except urllib.error.HTTPError as e:
            body = e.read()[:300].decode(errors="replace")
            if e.code == 404:
                time.sleep(COOLDOWN.get(endpoint, 1.0))
                return None
            if e.code == 429 and "COOLDOWN" in body.upper():
                time.sleep(6)
                continue
            if e.code >= 500:                                       # transient server errors: back off and retry
                time.sleep(30 * (k + 1))
                continue
            raise SystemExit(f"{endpoint}: HTTP {e.code} {body}")      # URL (with key) is never printed
        except (urllib.error.URLError, TimeoutError):
            time.sleep(5 * (k + 1))
    raise SystemExit(f"{endpoint}: failed")


PAGE_CAP = 250                    # /v4/fixtures returns at most 250 rows: a full page means the window was truncated
WINDOW_DAYS = {17: 4, 16: 9, 18: 9}
MAX_CALLS = 170                   # leave part of the monthly quota untouched


def fixtures(start, end):
    os.makedirs(f"{CACHE}/fixtures", exist_ok=True)
    n = 0

    def window(sport, a, b):
        nonlocal n
        path = f"{CACHE}/fixtures/{sport}_{a}_{b}.json"
        if os.path.exists(path):
            out = json.load(open(path))
        else:
            if n >= MAX_CALLS:
                raise SystemExit(f"stopped at the {MAX_CALLS}-call budget")
            out = call("fixtures", sportId=sport, **{"from": a.isoformat(), "to": b.isoformat()}) or []
            json.dump(out, open(path, "w"))
            n += 1
        if len(out) >= PAGE_CAP and (b - a).days > 1:          # truncated: split the window in two
            mid = a + dt.timedelta(days=(b - a).days // 2)
            window(sport, a, mid)
            window(sport, mid, b)

    d0, d1 = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    for sport, days in WINDOW_DAYS.items():
        a = d0
        while a < d1:
            b = min(a + dt.timedelta(days=days), d1)
            window(sport, a, b)
            a = b
        print(SPORTS[sport], "done; billable calls so far:", n, flush=True)


def history(limit=None):
    os.makedirs(f"{CACHE}/history", exist_ok=True)
    fx = [f for f in load_fixtures() if (f.get("externalProviders") or {}).get("pinnacleId")]
    if os.path.exists("data/processed/oddspapi_map.parquet"):     # only fixtures matched to a Polymarket market
        import polars as pl
        mapped = set(pl.read_parquet("data/processed/oddspapi_map.parquet")["fixtureId"].to_list())
        fx = [f for f in fx if f["fixtureId"] in mapped]
    import random
    random.Random(1).shuffle(fx)                                   # random order: a partial download is a fair sample
    todo = [f for f in fx if not os.path.exists(f"{CACHE}/history/{f['fixtureId']}.json")][:limit]
    print(len(fx), "fixtures,", len(todo), "to fetch (~%.0f min)" % (len(todo) * 5.2 / 60), flush=True)
    for i, f in enumerate(todo):
        out = call("historical-odds", fixtureId=f["fixtureId"], bookmakers="pinnacle")
        json.dump(out, open(f"{CACHE}/history/{f['fixtureId']}.json", "w"))
        if i % 100 == 0:
            print(i, flush=True)


def load_fixtures():
    seen, out = set(), []
    for p in sorted(os.listdir(f"{CACHE}/fixtures")):
        data = json.load(open(f"{CACHE}/fixtures/{p}"))
        for f in data if isinstance(data, list) else data.get("data", []):
            if f["fixtureId"] not in seen and f.get("statusId") == 2:
                seen.add(f["fixtureId"])
                out.append(f)
    return out


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "account":
        a = call("account")
        print({k: v for k, v in a.items() if "key" not in k.lower()} if isinstance(a, dict) else a)
    elif cmd == "fixtures":
        fixtures(sys.argv[2], sys.argv[3])
    elif cmd == "history":
        history(int(sys.argv[2]) if len(sys.argv) > 2 else None)
