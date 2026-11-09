"""What would copying one wallet have earned? Public Data API v2 + Gamma only (works for wallets newer than the HF data).

1) the wallet's own result: cash flows of its trades + payout of what it still held at resolution (resolved markets only);
2) copying: every BUY >= MIN_USD of the wallet -> our limit at its price + offset, placed D s later, alive TTL,
   filled only by later public prints at <= limit; stake = min($100, 50% of that volume); fee 1%; held to resolution;
   also an optimistic "instant" copy at the wallet's own price.
One copy per (market, outcome) per day at most: repeated adds by the wallet are not copied again.
Usage: python backtest/copy_wallet.py <wallet>"""
import json, os, sys, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import polars as pl

W = sys.argv[1].lower()
CACHE = "data/cache/wallets"
DATA, GAMMA = "https://data-api.polymarket.com/v2", "https://gamma-api.polymarket.com"
FEE, STAKE, LIQ, MIN_STAKE, MIN_USD = 0.01, 100.0, 0.5, 5.0, 10.0
EXEC = [(60, 0.0, 300), (60, 0.01, 300), (60, 0.01, 3600), (300, 0.02, 3600), (5, 0.0, 300)]


def get(url, tries=5):
    for k in range(tries):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "research-pipeline"}), timeout=60))
        except Exception:
            time.sleep(2 ** k)
    return None


trades = json.load(open(f"{CACHE}/{W}.json"))
t = pl.DataFrame([{k: r.get(k) for k in ("condition_id", "token_id", "side", "size", "price", "timestamp", "outcome_index", "title")} for r in trades])
t = t.with_columns(pl.col("token_id").cast(pl.Utf8), (pl.col("size") * pl.col("price")).alias("usd"))
conds = t["condition_id"].unique().to_list()
print(W, "trades", t.height, "markets", len(conds), flush=True)

# resolutions (Gamma, by condition id, batched)
rpath = f"{CACHE}/{W}_markets.json"
if not os.path.exists(rpath):
    res = []
    for i in range(0, len(conds), 50):
        q = "&".join(f"condition_ids={c}" for c in conds[i:i + 50])
        for closed in ("true", "false"):
            res += get(f"{GAMMA}/markets?limit=100&closed={closed}&{q}") or []
    json.dump(res, open(rpath, "w"))
mk = {}
for m in json.load(open(rpath)):
    toks, prices = json.loads(m.get("clobTokenIds") or "[]"), json.loads(m.get("outcomePrices") or "[]")
    resolved = bool(m.get("closed")) and prices and max(float(p) for p in prices) >= 0.99
    mk[m["conditionId"]] = dict(tokens=toks, pay={tok: float(p) for tok, p in zip(toks, prices)} if resolved else None,
                                end=m.get("endDate"), question=m.get("question"))
print("resolved markets:", sum(1 for v in mk.values() if v["pay"]), "of", len(conds), flush=True)

# 1) own result on resolved markets
own = []
for (c,), g in t.group_by("condition_id"):
    m = mk.get(c)
    if not m or not m["pay"]:
        continue
    cash, cost = 0.0, 0.0
    held = {}
    for r in g.sort("timestamp").iter_rows(named=True):
        q = r["size"] if r["side"] == "BUY" else -r["size"]
        held[r["token_id"]] = held.get(r["token_id"], 0.0) + q
        cash -= q * r["price"]
        cost += r["usd"] if r["side"] == "BUY" else 0.0
    pnl = cash + sum(q * m["pay"].get(tok, 0.0) for tok, q in held.items())
    own.append(dict(condition_id=c, pnl=pnl, bought=cost))
own = pl.DataFrame(own)
print(f"OWN: markets {own.height}, bought ${own['bought'].sum():,.0f}, pnl ${own['pnl'].sum():,.0f}, "
      f"roi on buys {own['pnl'].sum() / own['bought'].sum():+.1%}, markets in profit {(own['pnl'] > 0).mean():.0%}", flush=True)

# 2) public tape for every resolved market the wallet bought (takerOnly prints), cached
os.makedirs(f"{CACHE}/tape", exist_ok=True)


def tape(c):
    p = f"{CACHE}/tape/{c}.json"
    if os.path.exists(p):
        return
    out, cur = [], None
    for _ in range(40):
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


buys = t.filter(pl.col("side") == "BUY", pl.col("usd") >= MIN_USD, pl.col("condition_id").is_in([c for c, v in mk.items() if v["pay"]]))
need = buys["condition_id"].unique().to_list()
with ThreadPoolExecutor(6) as ex:
    for i, _ in enumerate(ex.map(tape, need)):
        if i % 200 == 0:
            print("tape", i, "/", len(need), flush=True)

# 3) copy simulation
buys = (buys.with_columns((pl.col("timestamp") // 86400).alias("day")).sort("timestamp")
        .unique(["condition_id", "token_id", "day"], keep="first", maintain_order=True))
rows = []
for r in buys.iter_rows(named=True):
    tp = json.load(open(f"{CACHE}/tape/{r['condition_id']}.json"))
    m = mk[r["condition_id"]]
    pay = m["pay"].get(r["token_id"], 0.0)
    toks = m["tokens"]
    # price of our token from every print (the complementary token prints at 1 - p)
    arr = sorted((ts, p if tok == r["token_id"] else 1 - p, size * p) for ts, tok, p, size in tp if tok in toks)
    ts = np.array([a[0] for a in arr], dtype=np.int64)
    po = np.array([a[1] for a in arr])
    usd = np.array([a[2] for a in arr])
    base = dict(condition_id=r["condition_id"], ts=r["timestamp"], price=r["price"], pay=pay, wallet_usd=r["usd"])
    rows.append(dict(base, mode="instant", stake=min(STAKE, r["usd"]), limit=r["price"]))
    for d, off, ttl in EXEC:
        a, b = np.searchsorted(ts, r["timestamp"] + d, "right"), np.searchsorted(ts, r["timestamp"] + d + ttl, "right")
        limit = min(r["price"] + off, 0.99)
        vol = float(usd[a:b][po[a:b] <= limit + 1e-9].sum())
        stake = min(STAKE, LIQ * vol)
        rows.append(dict(base, mode=f"D{d}s +{off * 100:.0f}c TTL{ttl // 60}m", stake=stake if stake >= MIN_STAKE else 0.0, limit=limit))
cp = (pl.DataFrame(rows)
      .with_columns((pl.col("stake") * (pl.col("pay") / pl.col("limit") - 1 - FEE)).alias("pnl"),
                    pl.from_epoch("ts").dt.strftime("%m-%d").alias("day"),
                    (pl.col("ts") >= pl.col("ts").min() + (pl.col("ts").max() - pl.col("ts").min()) // 2).alias("second_half")))
cp.write_parquet(f"data/processed/backtest/copy_{W}.parquet")
summ = (cp.group_by("mode").agg(pl.len().alias("signals"), (pl.col("stake") > 0).sum().alias("filled"),
                                pl.col("stake").sum().alias("turnover"), pl.col("pnl").sum().alias("pnl"),
                                (pl.col("pnl").sum() / pl.col("stake").sum()).alias("roi"),
                                (pl.col("pnl").filter(~pl.col("second_half")).sum() / pl.col("stake").filter(~pl.col("second_half")).sum()).alias("roi_1st_half"),
                                (pl.col("pnl").filter(pl.col("second_half")).sum() / pl.col("stake").filter(pl.col("second_half")).sum()).alias("roi_2nd_half"),
                                (pl.col("pay").filter(pl.col("stake") > 0) > 0.5).mean().alias("win_filled"),
                                (pl.col("pay") - pl.col("price")).mean().alias("edge_at_wallet_price"),
                                (pl.col("pay") - pl.col("price")).filter(pl.col("stake") > 0).mean().alias("edge_filled"),
                                (pl.col("pay") - pl.col("price")).filter(pl.col("stake") == 0).mean().alias("edge_unfilled"))
        .sort("mode"))
summ.write_csv(f"reports/copy_{W[:10]}.csv")
with pl.Config(tbl_cols=20, tbl_width_chars=250, float_precision=3):
    print(summ)
