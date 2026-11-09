"""Crypto up/down wallet (mo-money style): where does its PnL come from, and can its rule be copied?

Input: data/cache/wallets/{W}_activity.json (Data API v2 /activity: TRADE, MERGE, REDEEM).
1) own PnL per resolved market = sells + merges + payout of held shares - buys; split into
   "pair" PnL (min(Up, Down) shares are worth $1 whatever happens) and "one side" PnL (the unpaired rest);
2) calibration of the public tape: for prints at price p, how often does that side win (sample of markets);
3) copy: every buy of the wallet -> our bid at its price + off, placed D s later, alive until the market ends,
   filled by later prints at <= limit; stake = min($30, 50% of that volume); held to resolution, no fee (maker).
Usage: python backtest/updown_wallet.py <wallet> [sample_markets]"""
import json, os, random, sys, time, urllib.parse, urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import polars as pl

W = sys.argv[1].lower()
SAMPLE = int(sys.argv[2]) if len(sys.argv) > 2 else 400
CACHE = "data/cache/wallets"
DATA, GAMMA = "https://data-api.polymarket.com/v2", "https://gamma-api.polymarket.com"
STAKE, LIQ, MIN_STAKE = 30.0, 0.5, 2.0
EXEC = [(1, 0.0), (5, 0.0), (15, 0.0), (60, 0.0), (5, 0.01), (15, 0.02)]


def get(url, tries=5):
    for k in range(tries):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "research-pipeline"}), timeout=60))
        except Exception:
            time.sleep(2 ** k)
    return None


act = json.load(open(f"{CACHE}/{W}_activity.json"))
t0 = min(r["timestamp"] for r in act)
by_c = defaultdict(list)
for r in act:
    by_c[r["condition_id"]].append(r)
# markets fully inside the window: first activity at least 4 h after the window start
conds = [c for c, rs in by_c.items() if min(r["timestamp"] for r in rs) > t0 + 4 * 3600]

rpath = f"{CACHE}/{W}_markets.json"
if not os.path.exists(rpath):
    res = []
    for i in range(0, len(conds), 50):
        q = "&".join(f"condition_ids={c}" for c in conds[i:i + 50])
        res += get(f"{GAMMA}/markets?limit=100&closed=true&{q}") or []
    json.dump(res, open(rpath, "w"))
mk = {}
for m in json.load(open(rpath)):
    toks, prices = json.loads(m.get("clobTokenIds") or "[]"), json.loads(m.get("outcomePrices") or "[]")
    if m.get("closed") and prices and max(float(p) for p in prices) >= 0.99:
        mk[m["conditionId"]] = dict(tokens=toks, pay={tok: float(p) for tok, p in zip(toks, prices)},
                                    end=int(pl.Series([m["endDate"]]).str.to_datetime(time_zone="UTC").dt.epoch("s")[0]), slug=m.get("slug"))
print("markets in window", len(conds), "resolved", len(mk), flush=True)

# 1) own PnL, pair vs one side
own = []
for c in conds:
    m = mk.get(c)
    if not m:
        continue
    sh, cost, cash = defaultdict(float), defaultdict(float), 0.0
    for r in by_c[c]:
        if r["type"] == "TRADE":
            tok = str(r["token_id"])
            q = r["size"] if r["side"] == "BUY" else -r["size"]
            if q > 0:
                cost[tok] += r["usdc_size"]
            sh[tok] += q
            cash -= q * r["price"]
    up, dn = m["tokens"]
    bu, bd = sum(r["size"] for r in by_c[c] if r["type"] == "TRADE" and r["side"] == "BUY" and str(r["token_id"]) == up), \
             sum(r["size"] for r in by_c[c] if r["type"] == "TRADE" and r["side"] == "BUY" and str(r["token_id"]) == dn)
    pnl = cash + sum(q * m["pay"].get(t, 0.0) for t, q in sh.items())     # merging pairs = $1 per pair = same as holding
    pairs = min(bu, bd)
    au, ad = (cost[up] / bu if bu else 0), (cost[dn] / bd if bd else 0)
    pair_pnl = pairs * (1 - au - ad)
    own.append(dict(condition_id=c, kind=m["slug"].rsplit("-", 1)[0], bought=cost[up] + cost[dn], pnl=pnl,
                    pairs=pairs, pair_pnl=pair_pnl, side_pnl=pnl - pair_pnl, locked=pairs > 0,
                    n_buys=sum(1 for r in by_c[c] if r["type"] == "TRADE" and r["side"] == "BUY")))
own = pl.DataFrame(own)
own.write_parquet(f"data/processed/backtest/updown_own_{W[:10]}.parquet")
print(own.group_by("kind").agg(pl.len().alias("markets"), pl.col("bought").sum(), pl.col("pnl").sum(),
                               pl.col("pair_pnl").sum(), pl.col("side_pnl").sum(), pl.col("locked").mean().alias("share_locked"),
                               (pl.col("pnl") > 0).mean().alias("markets_won"))
      .with_columns((pl.col("pnl") / pl.col("bought")).alias("roi")).sort("bought", descending=True))
tot = own.select(pl.col("bought").sum(), pl.col("pnl").sum(), pl.col("pair_pnl").sum(), pl.col("side_pnl").sum())
print("TOTAL", tot.to_dicts()[0], "roi %.1f%%" % (100 * tot["pnl"][0] / tot["bought"][0]), flush=True)

# 2) public tape for a random sample of the wallet's resolved BTC markets
os.makedirs(f"{CACHE}/tape", exist_ok=True)


def tape(c):
    p = f"{CACHE}/tape/{c}.json"
    if os.path.exists(p):
        return
    out, cur = [], None
    for _ in range(60):
        q = {"condition": c, "takerOnly": "true", "limit": 1000}
        if cur:
            q["cursor"] = cur
        d = get(f"{DATA}/trades?{urllib.parse.urlencode(q)}")
        if not d:
            break
        out += [(r["timestamp"], str(r["token_id"]), r["price"], r["size"]) for r in d.get("data") or []]
        pg = d.get("pagination") or {}
        if not pg.get("has_more"):
            break
        cur = pg.get("next_cursor")
    json.dump(out, open(p, "w"))


btc = [c for c in mk if mk[c]["slug"].startswith("btc-updown-5m") or mk[c]["slug"].startswith("btc-updown-15m")]
random.Random(1).shuffle(btc)
sample = btc[:SAMPLE]
with ThreadPoolExecutor(6) as ex:
    for i, _ in enumerate(ex.map(tape, sample)):
        if i % 100 == 0:
            print("tape", i, "/", len(sample), flush=True)

cal, rows = [], []
for c in sample:
    m = mk[c]
    tp = json.load(open(f"{CACHE}/tape/{c}.json"))
    up = m["tokens"][0]
    # Up-price series from every print (Down prints at 1 - p)
    arr = sorted((ts, p if tok == up else 1 - p, size * p) for ts, tok, p, size in tp if tok in m["tokens"])
    if not arr:
        continue
    ts = np.array([a[0] for a in arr], dtype=np.int64)
    pu = np.array([a[1] for a in arr])
    usd = np.array([a[2] for a in arr])
    for ts_, tok, p, size in tp:
        cal.append((m["slug"].rsplit("-", 1)[0], p, m["pay"].get(tok, 0.0), size * p))
    for r in by_c[c]:
        if r["type"] != "TRADE" or r["side"] != "BUY":
            continue
        tok = str(r["token_id"])
        po = pu if tok == up else 1 - pu
        pay = m["pay"].get(tok, 0.0)
        base = dict(condition_id=c, kind=m["slug"].rsplit("-", 1)[0], ts=r["timestamp"], price=r["price"], pay=pay)
        rows.append(dict(base, mode="instant (its own fill)", stake=min(STAKE, r["usdc_size"]), limit=r["price"]))
        for d, off in EXEC:
            a, b = np.searchsorted(ts, r["timestamp"] + d, "right"), np.searchsorted(ts, m["end"], "right")
            limit = min(r["price"] + off, 0.99)
            vol = float(usd[a:b][po[a:b] <= limit + 1e-9].sum())
            stake = min(STAKE, LIQ * vol)
            rows.append(dict(base, mode=f"D{d}s +{off * 100:.0f}c", stake=stake if stake >= MIN_STAKE else 0.0, limit=limit))

cal = (pl.DataFrame(cal, schema=["kind", "price", "pay", "usd"], orient="row")
       .with_columns(pl.col("price").cut([0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98]).alias("bucket"))
       .group_by("bucket").agg(pl.len().alias("prints"), pl.col("price").mean().alias("avg_price"),
                               pl.col("pay").mean().alias("win_rate"), ((pl.col("pay") * pl.col("usd")).sum() / pl.col("usd").sum()).alias("win_rate_usd"))
       .with_columns((pl.col("win_rate") - pl.col("avg_price")).alias("edge")).sort("avg_price"))
cal.write_csv(f"reports/updown_calibration_{W[:10]}.csv")
cp = pl.DataFrame(rows).with_columns((pl.col("stake") * (pl.col("pay") / pl.col("limit") - 1)).alias("pnl"))
cp.write_parquet(f"data/processed/backtest/updown_copy_{W[:10]}.parquet")
summ = (cp.group_by("mode").agg(pl.len().alias("its_buys"), (pl.col("stake") > 0).mean().alias("filled_share"),
                                pl.col("stake").sum().alias("turnover"), pl.col("pnl").sum().alias("pnl"),
                                (pl.col("pnl").sum() / pl.col("stake").sum()).alias("roi"),
                                (pl.col("pay") - pl.col("price")).filter(pl.col("stake") > 0).mean().alias("edge_filled"),
                                (pl.col("pay") - pl.col("price")).filter(pl.col("stake") == 0).mean().alias("edge_unfilled"))
        .sort("mode"))
summ.write_csv(f"reports/updown_copy_{W[:10]}.csv")
with pl.Config(tbl_cols=20, tbl_width_chars=250, float_precision=3, tbl_rows=30):
    print(cal)
    print(summ)
