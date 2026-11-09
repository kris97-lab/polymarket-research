"""Pilot: Polymarket esports series-winner prices vs Pinnacle no-vig fair value, pre-match only.

1) calibration: last pre-match Polymarket price vs Pinnacle closing fair (Brier score, both vs actual results);
2) strategy: buy the outcome whose Polymarket print is >= THR below Pinnacle fair (one entry per market, >= 5 min before
   the start). Two execution models:
   - optimistic ("first"): we buy at that print's price, stake = min($100, 50% of the print);
   - conservative (as the paper bot): limit at that price + 1c placed 60 s later, alive 5 min, filled only by later
     prints <= limit, stake = min($100, 50% of that volume);
   fee 1% of stake; held to resolution;
3) top esports wallets: do their pre-match entries happen at prices below Pinnacle fair more often than the market's?
Outputs reports/pinnacle_*.csv."""
import glob, json, os
import numpy as np
import polars as pl
import duckdb

FEE, STAKE, LIQ, MIN_STAKE = 0.01, 100.0, 0.5, 5.0
THRS = [0.02, 0.03, 0.05, 0.08]
STALE_S = 12 * 3600               # Pinnacle snapshot older than this is not used
MIN_LEAD_S = 300                  # entry at least 5 min before the start
D, TTL, OFF = 60, 300, 0.01
SPLIT = int(pl.Series(["2026-06-01"]).str.to_datetime(time_zone="UTC").dt.epoch("s")[0])

mp = pl.read_parquet("data/processed/oddspapi_map.parquet")
mk = duckdb.connect("data/polymarket.duckdb", read_only=True).sql(
    "select market_id, case winner_idx when 0 then 1.0 when 1 then 0.0 else payout1 end payout_yes from markets where source = 'hf'").pl()
mp = mp.join(mk, on="market_id")


def fair_series(fid, sport, swapped):
    """Pinnacle no-vig probability of Polymarket answer1 over time: (ts array, p array)."""
    p = f"data/cache/oddspapi/history/{fid}.json"
    if not os.path.exists(p):
        return None
    h = json.load(open(p))
    try:
        outs = h["bookmakers"]["pinnacle"]["markets"][str(sport * 10 + 1)]["outcomes"]
        s1, s2 = outs[str(sport * 10 + 1)]["players"]["0"], outs[str(sport * 10 + 2)]["players"]["0"]
    except (KeyError, TypeError):
        return None
    ev = sorted([(x["createdAt"], 1, x["price"], x.get("active", True)) for x in s1] +
                [(x["createdAt"], 2, x["price"], x.get("active", True)) for x in s2])
    o1 = o2 = None
    ts, pr = [], []
    for c, side, price, active in ev:
        if side == 1:
            o1 = price if active and price and price > 1 else None
        else:
            o2 = price if active and price and price > 1 else None
        if o1 and o2:
            q1 = (1 / o1) / (1 / o1 + 1 / o2)
            ts.append(c)
            pr.append(1 - q1 if swapped else q1)
    if not ts:
        return None
    t = pl.Series(ts).str.to_datetime(time_zone="UTC").dt.epoch("s").to_numpy()
    return t, np.array(pr)


fair = {}
for r in mp.iter_rows(named=True):
    s = fair_series(r["fixtureId"], r["sportId"], r["swapped"])
    if s is not None:
        fair[r["market_id"]] = s
mp = mp.filter(pl.col("market_id").is_in(list(fair)))
print("markets with Pinnacle history:", mp.height, flush=True)

ids = mp["market_id"].to_list()
tape = (pl.scan_parquet(sorted(glob.glob("data/processed/fills/2026-*.parquet")) + sorted(glob.glob("data/processed/fills/2025-12.parquet")))
        .select("market_id", "ts", "price", "usd", "is_token1", "maker_buy").filter(pl.col("market_id").is_in(ids)).collect()
        .with_columns(pl.when(pl.col("is_token1")).then(pl.col("price")).otherwise(1 - pl.col("price")).alias("py"))
        .join(mp.select("market_id", "game_start_ts", "payout_yes"), on="market_id")
        .filter(pl.col("ts") < pl.col("game_start_ts")).sort("market_id", "ts"))
print("pre-match prints:", tape.height, flush=True)

# attach Pinnacle fair (as of each print) to every pre-match print
parts = []
for (mid,), g in tape.group_by("market_id"):
    t, p = fair[mid]
    k = np.searchsorted(t, g["ts"].to_numpy(), "right") - 1
    ok = k >= 0
    f = np.where(ok, p[np.clip(k, 0, None)], np.nan)
    age = np.where(ok, g["ts"].to_numpy() - t[np.clip(k, 0, None)], 1e12)
    parts.append(g.with_columns(pl.Series("fair_yes", f), pl.Series("fair_age", age)))
tp = pl.concat(parts).filter(pl.col("fair_age") <= STALE_S, pl.col("fair_yes").is_not_nan())

# 1) calibration: last pre-match print vs Pinnacle fair at that moment, against the result
last = tp.sort("ts").group_by("market_id").last()
cal = last.select(pl.len().alias("markets"),
                  ((pl.col("py") - pl.col("payout_yes")) ** 2).mean().alias("brier_polymarket"),
                  ((pl.col("fair_yes") - pl.col("payout_yes")) ** 2).mean().alias("brier_pinnacle"),
                  (pl.col("py") - pl.col("fair_yes")).abs().mean().alias("mean_abs_gap"),
                  (pl.col("py") - pl.col("fair_yes")).abs().quantile(0.9).alias("p90_abs_gap"))
cal.write_csv("reports/pinnacle_calibration.csv")
print(cal)

# 2) strategy: first print per market where one outcome trades >= THR below fair
tp = tp.with_columns(
    pl.when(pl.col("fair_yes") - pl.col("py") >= 0).then(0).otherwise(1).alias("o"),
    (pl.col("fair_yes") - pl.col("py")).abs().alias("gap"))
tp = tp.with_columns(pl.when(pl.col("o") == 0).then(pl.col("py")).otherwise(1 - pl.col("py")).alias("p_o"),
                     pl.when(pl.col("o") == 0).then(pl.col("payout_yes")).otherwise(1 - pl.col("payout_yes")).alias("pay_o"))
by_m = {mid[0]: (g["ts"].to_numpy(), g["py"].to_numpy(), g["usd"].to_numpy()) for mid, g in tape.group_by("market_id")}
rows = []
for thr in THRS:
    sig = (tp.filter(pl.col("gap") >= thr, pl.col("p_o").is_between(0.02, 0.98), pl.col("game_start_ts") - pl.col("ts") >= MIN_LEAD_S + D + TTL)
           .sort("ts").group_by("market_id", maintain_order=True).first())
    for s in sig.iter_rows(named=True):
        opt_stake = min(STAKE, LIQ * s["usd"])
        ts, py, usd = by_m[s["market_id"]]
        po = py if s["o"] == 0 else 1 - py
        a, b = np.searchsorted(ts, s["ts"] + D, "right"), np.searchsorted(ts, s["ts"] + D + TTL, "right")
        limit = min(s["p_o"] + OFF, 0.99)
        ok = po[a:b] <= limit + 1e-9
        cons_stake = min(STAKE, LIQ * float(usd[a:b][ok].sum()))
        rows.append(dict(thr=thr, market_id=s["market_id"], ts=s["ts"], o=s["o"], price=s["p_o"], fair_o=s["p_o"] + s["gap"], gap=s["gap"],
                         pay=s["pay_o"], period="train" if s["ts"] < SPLIT else "test",
                         opt_stake=opt_stake if opt_stake >= MIN_STAKE else 0.0, cons_stake=cons_stake if cons_stake >= MIN_STAKE else 0.0,
                         limit=limit))
tr = pl.DataFrame(rows).with_columns(
    (pl.col("opt_stake") * (pl.col("pay") / pl.col("price") - 1 - FEE)).alias("opt_pnl"),
    (pl.col("cons_stake") * (pl.col("pay") / pl.col("limit") - 1 - FEE)).alias("cons_pnl"),
    pl.from_epoch("ts").dt.strftime("%Y-%m").alias("month"))
tr.write_parquet("data/processed/backtest/pinnacle_value_trades.parquet")
roi = lambda p, s, per=None: (pl.col(p).filter(pl.col("period") == per).sum() / pl.col(s).filter(pl.col("period") == per).sum()) if per \
    else (pl.col(p).sum() / pl.col(s).sum())
summ = (tr.group_by("thr").agg(pl.len().alias("signals"), pl.col("gap").mean().alias("avg_gap"),
                              (pl.col("pay") - pl.col("price")).mean().alias("edge_vs_result"),
                              (pl.col("fair_o") - pl.col("pay")).mean().alias("fair_minus_result"),
                              (pl.col("opt_stake") > 0).sum().alias("opt_trades"), pl.col("opt_pnl").sum().alias("opt_pnl"),
                              roi("opt_pnl", "opt_stake").alias("opt_roi"), roi("opt_pnl", "opt_stake", "train").alias("opt_roi_train"),
                              roi("opt_pnl", "opt_stake", "test").alias("opt_roi_test"),
                              (pl.col("cons_stake") > 0).sum().alias("cons_trades"), pl.col("cons_pnl").sum().alias("cons_pnl"),
                              roi("cons_pnl", "cons_stake").alias("cons_roi"), roi("cons_pnl", "cons_stake", "train").alias("cons_roi_train"),
                              roi("cons_pnl", "cons_stake", "test").alias("cons_roi_test"))
        .sort("thr"))
months = tr.group_by("thr", "month").agg(pl.col("opt_pnl").sum(), pl.col("cons_pnl").sum()).group_by("thr").agg(
    (pl.col("opt_pnl") > 0).sum().alias("opt_months_pos"), (pl.col("cons_pnl") > 0).sum().alias("cons_months_pos"), pl.len().alias("months"))
summ = summ.join(months, on="thr").sort("thr")
summ.write_csv("reports/pinnacle_value.csv")
with pl.Config(tbl_cols=30, tbl_width_chars=300, float_precision=3):
    print(summ)

# 3) top esports wallets: share of their pre-match entries bought >= 2c below Pinnacle fair, vs all pre-match prints
ranks = (pl.read_parquet("data/processed/scores/scores.parquet", columns=["rebalance", "scope", "wallet", "rank"])
         .filter(pl.col("scope") == "esports", pl.col("rank") <= 50).select("rebalance", "wallet"))
ent = (pl.scan_parquet("data/processed/backtest/entries/*.parquet").select("wallet", "market_id", "ts", "outcome", "price_o", "usd")
       .filter(pl.col("market_id").is_in(ids)).collect()
       .with_columns(pl.from_epoch("ts").dt.truncate("1mo").dt.date().alias("rebalance")).join(ranks, on=["rebalance", "wallet"])
       .join(mp.select("market_id", "game_start_ts"), on="market_id").filter(pl.col("ts") < pl.col("game_start_ts")))
parts = []
for (mid,), g in ent.group_by("market_id"):
    t, p = fair[mid]
    k = np.searchsorted(t, g["ts"].to_numpy(), "right") - 1
    f = np.where(k >= 0, p[np.clip(k, 0, None)], np.nan)
    parts.append(g.with_columns(pl.Series("fair_yes", f)))
if parts:
    ent = pl.concat(parts).drop_nans("fair_yes").with_columns(
        (pl.when(pl.col("outcome") == 0).then(pl.col("fair_yes")).otherwise(1 - pl.col("fair_yes")) - pl.col("price_o")).alias("gap_vs_fair"))
    # every pre-match fill seen from the taker's side (the outcome the taker effectively bought)
    tk = tp.with_columns((pl.col("is_token1") != pl.col("maker_buy")).alias("taker_yes")).with_columns(
        (pl.when(pl.col("taker_yes")).then(pl.col("fair_yes") - pl.col("py")).otherwise(pl.col("py") - pl.col("fair_yes"))).alias("g"))
    w = pl.DataFrame(dict(
        group=["top-50 esports wallets, pre-match entries", "all pre-match fills, taker side"],
        n=[ent.height, tk.height],
        mean_gap=[float(ent["gap_vs_fair"].mean()), float(tk["g"].mean())],
        share_ge_2c=[float((ent["gap_vs_fair"] >= 0.02).mean()), float((tk["g"] >= 0.02).mean())],
        share_le_minus_2c=[float((ent["gap_vs_fair"] <= -0.02).mean()), float((tk["g"] <= -0.02).mean())]))
    w.write_csv("reports/pinnacle_top_wallets.csv")
    print(w)
