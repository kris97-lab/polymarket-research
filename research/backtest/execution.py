"""Conservative limit-order execution for every unique signal x (delay D, price offset, TTL), plus matched random entries.

Order: limit = ref_price (last smart entry) + offset, placed D seconds after the signal, alive for TTL.
 - skipped if the last trade before placement is already above ref + MAX_MOVE (price ran away);
 - filled only if a later trade printed at <= limit within TTL; fill price = our limit (never the better print);
 - fill_vol = USD volume printed at <= limit within TTL (capacity: the sim takes only a share of it).
Random baseline: for each signal, RANDOM_SEEDS entries at the same moment, same category, in a random other market
that traded within the previous hour and is scheduled to end within the horizon; random outcome, ref = its last price."""
import datetime as dt, glob, os
import numpy as np
import polars as pl
import duckdb

DS = [5, 60, 300]
OFFSETS = [0.0, 0.01, 0.02]
TTLS = [300, 3600, 21600]
MAX_MOVE = 0.05
RANDOM_SEEDS = 3
HORIZON_S = 30 * 86400
OUT = "data/processed/backtest/executions"

markets = duckdb.connect("data/polymarket.duckdb", read_only=True).sql(
    """select market_id, category, epoch(end_date)::bigint end_ts,
              case winner_idx when 0 then 1.0 when 1 then 0.0 else payout1 end payout_yes
       from markets where source = 'hf'""").pl()
mid_of = {m: i for i, m in enumerate(markets["market_id"].to_list())}
m_cat = markets["category"].to_numpy()
m_end = markets["end_ts"].to_numpy()
m_pay = markets["payout_yes"].to_numpy()

sig = (pl.read_parquet("data/processed/backtest/signals.parquet")
       .select("market_id", "outcome", "t_sig", "ref_price", "category").unique()
       .with_columns(pl.from_epoch("t_sig").dt.strftime("%Y-%m").alias("month")).sort("t_sig"))
rng = np.random.default_rng(7)
months = sorted(glob.glob("data/processed/fills/*.parquet"))


def load_tape(month):
    """Fills of this month and the next (signals near month end need the following hours), sorted by (market, ts)."""
    files = [f for f in months if os.path.basename(f)[:7] >= month][:2]
    t = (pl.scan_parquet(files).select("market_id", "ts", "price", "usd", "is_token1").collect()
         .with_columns(pl.col("market_id").replace_strict(mid_of, default=-1).alias("mid"),
                       pl.when(pl.col("is_token1")).then(pl.col("price")).otherwise(1 - pl.col("price")).alias("py"))
         .filter(pl.col("mid") >= 0).sort("mid", "ts"))
    mid, ts, py, usd = (t[c].to_numpy() for c in ("mid", "ts", "py", "usd"))
    starts = np.searchsorted(mid, np.arange(len(m_cat) + 1))
    by_time = np.argsort(ts, kind="stable")
    return dict(mid=mid, ts=ts.astype(np.int64), py=py.astype(np.float64), usd=usd.astype(np.float64),
                starts=starts, t_sorted=ts[by_time].astype(np.int64), mid_by_time=mid[by_time])


def execute(tp, m, outcome, t_sig, ref):
    a, b = tp["starts"][m], tp["starts"][m + 1]
    ts, py, usd = tp["ts"][a:b], tp["py"][a:b], tp["usd"][a:b]
    po = py if outcome == 0 else 1 - py
    rows = []
    for D in DS:
        start = t_sig + D
        k = np.searchsorted(ts, start, "right") - 1
        last = po[k] if k >= 0 and ts[k] >= t_sig else ref
        moved = last > ref + MAX_MOVE
        j0 = k + 1
        for ttl in TTLS:
            j1 = np.searchsorted(ts, start + ttl, "right")
            w_ts, w_po, w_usd = ts[j0:j1], po[j0:j1], usd[j0:j1]
            for off in OFFSETS:
                limit = min(ref + off, 0.99)
                ok = w_po <= limit + 1e-9
                vol = float(w_usd[ok].sum()) if not moved else 0.0
                first = int(w_ts[ok][0]) if (vol > 0) else -1
                rows.append((D, off, ttl, moved, vol, first, limit, last))
    return rows


def random_counterpart(tp, cat, t_sig, exclude):
    lo, hi = np.searchsorted(tp["t_sorted"], [t_sig - 3600, t_sig], side="right")
    cand = np.unique(tp["mid_by_time"][lo:hi])
    cand = cand[(m_cat[cand] == cat) & (m_end[cand] > t_sig) & (m_end[cand] - t_sig <= HORIZON_S) & (cand != exclude)]
    if len(cand) == 0:
        return None
    m = int(rng.choice(cand))
    outcome = int(rng.integers(2))
    a, b = tp["starts"][m], tp["starts"][m + 1]
    k = np.searchsorted(tp["ts"][a:b], t_sig, "right") - 1
    if k < 0:
        return None
    py = tp["py"][a + k]
    ref = py if outcome == 0 else 1 - py
    if not 0.02 <= ref <= 0.95:
        return None
    return m, outcome, ref


cols = ["market_id", "outcome", "t_sig", "seed", "exec_market_id", "exec_outcome", "ref_price", "payout_o", "end_ts", "category",
        "D", "offset", "TTL", "moved", "fill_vol", "fill_ts", "limit", "last_price"]
os.makedirs(OUT, exist_ok=True)
for month, g in sig.group_by("month", maintain_order=True):
    path = f"{OUT}/{month[0]}.parquet"
    if os.path.exists(path):
        continue
    out = []
    tp = load_tape(month[0])
    for market_id, outcome, t_sig, ref, cat in g.select("market_id", "outcome", "t_sig", "ref_price", "category").iter_rows():
        m = mid_of[market_id]
        pay = m_pay[m] if outcome == 0 else 1 - m_pay[m]
        for r in execute(tp, m, outcome, t_sig, ref):
            out.append((market_id, outcome, t_sig, -1, market_id, outcome, ref, pay, int(m_end[m]), cat) + r)
        for seed in range(RANDOM_SEEDS):
            rc = random_counterpart(tp, cat, t_sig, m)
            if rc is None:
                continue
            rm, ro, rref = rc
            rpay = m_pay[rm] if ro == 0 else 1 - m_pay[rm]
            for r in execute(tp, rm, ro, t_sig, rref):
                out.append((market_id, outcome, t_sig, seed, markets["market_id"][rm], ro, rref, rpay, int(m_end[rm]), cat) + r)
    pl.DataFrame(out, schema=cols, orient="row").write_parquet(path)
    del tp, out
    print(month[0], g.height, "signals", flush=True)
