"""Portfolio simulation and grid search with an out-of-sample split.

Per config (scope, N, K, W, D, offset, TTL): signals -> executions -> event-driven portfolio:
 - stake = PCT of equity (cash + open positions at cost), capped per market and per category, by free cash,
   and by LIQ_SHARE of the USD volume that printed at <= our limit within TTL (we only get part of the liquidity);
 - fee FEE_BPS on notional at entry; position held to resolution (scheduled end), payout 1/0 (0.5 on 50/50);
 - daily equity -> ROI, Sharpe (annualized, sqrt 365), max drawdown.
Parameters are selected on TRAIN signals only and then evaluated on TEST."""
import datetime as dt, heapq, sys
import numpy as np
import polars as pl

BANK, PCT, MAX_MARKET_PCT, MAX_CAT_PCT = 10_000.0, 0.02, 0.02, 0.25
LIQ_SHARE, MIN_STAKE = 0.5, 5.0
FEE_BPS = float(sys.argv[1]) if len(sys.argv) > 1 else 100.0
TRAIN = (dt.datetime(2025, 10, 1), dt.datetime(2026, 5, 1))
TEST = (dt.datetime(2026, 5, 1), dt.datetime(2026, 9, 4))
MIN_TRADES = 30
EXEC = ["D", "offset", "TTL"]
CFG = ["scope", "N", "K", "W_h"]
epoch = lambda d: int(d.replace(tzinfo=dt.timezone.utc).timestamp())


def simulate(fill_ts, end_ts, limit, payout, fill_vol, cats, period, fee_bps=FEE_BPS, curve=False):
    """Event-driven sim over fills sorted by time. Returns metrics dict and per-category PnL
    (metrics also carry the daily equity curve as `curve` = (first_day_epoch, equity array) when asked)."""
    order = np.argsort(fill_ts, kind="stable")
    cash, open_cost, cat_cost = BANK, 0.0, {}
    heap, trades = [], 0
    day0, day1 = period[0] // 86400, period[1] // 86400 + 45
    daily = np.zeros(day1 - day0 + 1)          # realized pnl by resolution day
    cat_pnl = {}

    def settle(until):
        nonlocal cash, open_cost
        while heap and heap[0][0] <= until:
            t_end, stake, shares, pay, cat = heapq.heappop(heap)
            proceeds = shares * pay
            cash += proceeds
            open_cost -= stake
            cat_cost[cat] -= stake
            pnl = proceeds - stake * (1 + fee_bps / 1e4)
            daily[min(max(t_end // 86400 - day0, 0), len(daily) - 1)] += pnl
            cat_pnl[cat] = cat_pnl.get(cat, 0.0) + pnl

    for i in order:
        if end_ts[i] <= fill_ts[i]:
            continue
        settle(fill_ts[i])
        equity = cash + open_cost
        cat = cats[i]
        stake = min(PCT * equity, MAX_MARKET_PCT * equity, MAX_CAT_PCT * equity - cat_cost.get(cat, 0.0),
                    cash / (1 + fee_bps / 1e4), LIQ_SHARE * fill_vol[i])
        if stake < MIN_STAKE:
            continue
        cash -= stake * (1 + fee_bps / 1e4)
        open_cost += stake
        cat_cost[cat] = cat_cost.get(cat, 0.0) + stake
        heapq.heappush(heap, (int(end_ts[i]), stake, stake / limit[i], float(payout[i]), cat))
        trades += 1
    settle(1 << 62)
    equity = BANK + np.cumsum(daily)
    rets = np.diff(np.concatenate([[BANK], equity])) / np.concatenate([[BANK], equity[:-1]])
    sharpe = float(rets.mean() / rets.std() * np.sqrt(365)) if rets.std() > 0 else 0.0
    dd = float((equity / np.maximum.accumulate(equity) - 1).min())
    m = dict(pnl=float(equity[-1] - BANK), roi=float(equity[-1] / BANK - 1), sharpe=sharpe, max_dd=dd, trades=trades)
    if curve:
        m["curve"] = (day0 * 86400, equity)
    return m, cat_pnl


def run_period(sig, ex, period, seeds=(-1,), fee_bps=FEE_BPS, curve=False):
    """sig: signals of one (scope,N,K,W); ex: executions; returns rows per exec combo (and seed)."""
    lo, hi = epoch(period[0]), epoch(period[1])
    s = sig.filter(pl.col("t_sig") >= lo, pl.col("t_sig") < hi)
    if s.is_empty():
        return []
    j = s.select("market_id", "outcome", "t_sig").join(ex.filter(pl.col("seed").is_in(list(seeds))),
                                                       on=["market_id", "outcome", "t_sig"])
    rows = []
    for key, g in j.group_by(EXEC + ["seed"]):
        n_sig = g.select("market_id", "outcome", "t_sig").unique().height
        f = g.filter(pl.col("fill_vol") > 0, ~pl.col("moved"))
        ret = (g["payout_o"] / g["limit"] - 1)
        filled = (g["fill_vol"] > 0) & ~g["moved"]
        m, cat_pnl = simulate(f["fill_ts"].to_numpy(), f["end_ts"].to_numpy(), f["limit"].to_numpy(), f["payout_o"].to_numpy(),
                              f["fill_vol"].to_numpy(), f["category"].to_list(), (lo, hi), fee_bps, curve)
        m.update(dict(zip(EXEC + ["seed"], key)), signals=n_sig, fill_rate=f.height / max(g.height, 1),
                 ret_filled=float(ret.filter(filled).mean() or 0), ret_unfilled=float(ret.filter(~filled & ~g["moved"]).mean() or 0),
                 cat_pnl=cat_pnl)
        rows.append(m)
    return rows


if __name__ == "__main__":
    sig = pl.read_parquet("data/processed/backtest/signals.parquet")
    ex = pl.read_parquet("data/processed/backtest/executions/*.parquet").with_columns(pl.col("moved").cast(pl.Boolean))
    ex = ex.unique(subset=["market_id", "outcome", "t_sig", "seed"] + EXEC, keep="first")
    strat = ex.filter(pl.col("seed") == -1)
    out = []
    for cfg, g in sig.group_by(CFG):
        for name, period in (("train", TRAIN), ("test", TEST)):
            for r in run_period(g, strat, period):
                r.update(dict(zip(CFG, cfg)), period=name)
                r["cat_pnl"] = str({k: round(v) for k, v in r["cat_pnl"].items()})
                out.append(r)
    res = pl.DataFrame(out)
    res.write_parquet(f"data/processed/backtest/grid_fee{int(FEE_BPS)}.parquet")
    print(res.height, "config-period rows")
