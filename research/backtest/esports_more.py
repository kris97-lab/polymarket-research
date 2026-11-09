"""Esports pre-match: rule variants that trade more often (K=1, deeper top-N, entry-size thresholds).

Same frozen execution as the paper bot: limit = ref + 1c, placed 60 s after the signal, alive 5 min, whole window before
the match start; fill only on prints <= limit; stake = min($100, 50% of that volume); fee 1%. One signal per market.
Output: reports/esports_more.csv (per variant: signals, fills, PnL, ROI on turnover, train/test, months positive)."""
import datetime as dt, glob, os
import numpy as np
import polars as pl
import duckdb

D, TTL, OFF, MAX_MOVE, W_S = 60, 300, 0.01, 0.05, 6 * 3600
STAKE, LIQ, MIN_STAKE, FEE = 100.0, 0.5, 5.0, 0.01
START, SPLIT, END = (int(dt.datetime(*d, tzinfo=dt.timezone.utc).timestamp()) for d in ((2025, 10, 1), (2026, 5, 1), (2026, 9, 4)))

mk = duckdb.connect("data/polymarket.duckdb", read_only=True).sql(
    """select market_id, epoch(end_date)::bigint end_ts, case winner_idx when 0 then 1.0 when 1 then 0.0 else payout1 end payout_yes
       from markets where source = 'hf' and category = 'esports'""").pl()
gs = pl.read_parquet("data/processed/esports_game_start.parquet").select("market_id", "game_start_ts").drop_nulls()
mk = mk.join(gs, on="market_id")
ranks = (pl.read_parquet("data/processed/scores/scores.parquet", columns=["rebalance", "scope", "wallet", "rank"])
         .filter(pl.col("scope") == "esports", pl.col("rank") <= 100).drop("scope"))
ent = (pl.scan_parquet("data/processed/backtest/entries/*.parquet").select("wallet", "market_id", "ts", "outcome", "price_o", "usd")
       .filter(pl.col("ts") >= START, pl.col("ts") < END).collect()
       .join(mk, on="market_id")
       .filter(pl.col("ts") < pl.col("game_start_ts"))
       .with_columns(pl.from_epoch("ts").dt.truncate("1mo").dt.date().alias("rebalance"))
       .join(ranks, on=["rebalance", "wallet"]))
print("pre-match entries of esports top-100:", ent.height, flush=True)

variants = []
for N in (10, 25, 50, 100):
    for K in (1, 2):
        for min_usd in (10, 100, 500):
            e = ent.filter(pl.col("rank") <= N, pl.col("usd") >= min_usd).sort("market_id", "outcome", "ts")
            if e.is_empty():
                continue
            nw = (e.with_columns(pl.from_epoch("ts").alias("t"))
                  .rolling(index_column="t", period=f"{W_S}s", group_by=["market_id", "outcome"])
                  .agg(pl.col("wallet").n_unique().alias("n"), pl.col("price_o").last().alias("ref"),
                       pl.col("game_start_ts").last(), pl.col("end_ts").last(), pl.col("payout_yes").last()))
            sig = (nw.filter(pl.col("n") >= K).with_columns(pl.col("t").dt.epoch("s").alias("t_sig"))
                   .filter(pl.col("ref").is_between(0.02, 0.95), pl.col("end_ts") - pl.col("t_sig") <= 30 * 86400,
                           pl.col("t_sig") + D + TTL <= pl.col("game_start_ts"))
                   .sort("t_sig").group_by("market_id", maintain_order=True).first()
                   .with_columns(pl.lit(N).alias("N"), pl.lit(K).alias("K"), pl.lit(min_usd).alias("min_usd")))
            variants.append(sig.select("N", "K", "min_usd", "market_id", "outcome", "t_sig", "ref", "payout_yes"))
sig = pl.concat(variants)
u = sig.select("market_id", "outcome", "t_sig", "ref").unique().with_columns(pl.from_epoch("t_sig").dt.strftime("%Y-%m").alias("month"))
print("signals:", sig.height, "unique:", u.height, flush=True)

# execution on the public tape, one month (+ next) of fills at a time, esports markets only
mid_of = {m: i for i, m in enumerate(mk["market_id"].to_list())}
files = sorted(glob.glob("data/processed/fills/*.parquet"))
res = []
for (month,), g in u.group_by("month", maintain_order=True):
    fs = [f for f in files if os.path.basename(f)[:7] >= month][:2]
    t = (pl.scan_parquet(fs).select("market_id", "ts", "price", "usd", "is_token1")
         .filter(pl.col("market_id").is_in(list(mid_of))).collect()
         .with_columns(pl.when(pl.col("is_token1")).then(pl.col("price")).otherwise(1 - pl.col("price")).alias("py"))
         .sort("market_id", "ts"))
    by_m = {k[0]: (d["ts"].to_numpy(), d["py"].to_numpy(), d["usd"].to_numpy()) for k, d in t.group_by("market_id")}
    for market_id, outcome, t_sig, ref, _ in g.iter_rows():
        ts, py, usd = by_m.get(market_id, (np.array([], dtype=np.int64), np.array([]), np.array([])))
        po = py if outcome == 0 else 1 - py
        start = t_sig + D
        k = np.searchsorted(ts, start, "right") - 1
        last = po[k] if k >= 0 and ts[k] >= t_sig else ref
        limit = min(ref + OFF, 0.99)
        if last > ref + MAX_MOVE:
            res.append((market_id, outcome, t_sig, True, 0.0, limit))
            continue
        a, b = k + 1, np.searchsorted(ts, start + TTL, "right")
        ok = po[a:b] <= limit + 1e-9
        res.append((market_id, outcome, t_sig, False, float(usd[a:b][ok].sum()), limit))
    print(month, g.height, flush=True)
ex = pl.DataFrame(res, schema=["market_id", "outcome", "t_sig", "moved", "fill_vol", "limit"], orient="row")

r = (sig.join(ex, on=["market_id", "outcome", "t_sig"])
     .with_columns(pl.when(pl.col("outcome") == 0).then(pl.col("payout_yes")).otherwise(1 - pl.col("payout_yes")).alias("pay"),
                   pl.min_horizontal(STAKE, LIQ * pl.col("fill_vol")).alias("stake"),
                   pl.when(pl.col("t_sig") < SPLIT).then(pl.lit("train")).otherwise(pl.lit("test")).alias("period"),
                   pl.from_epoch("t_sig").dt.strftime("%Y-%m").alias("month"))
     .with_columns((pl.col("stake") >= MIN_STAKE).alias("filled"))
     .with_columns(pl.when(pl.col("filled")).then(pl.col("stake") * (pl.col("pay") / pl.col("limit") - 1 - FEE)).otherwise(0.0).alias("pnl"),
                   pl.when(pl.col("filled")).then(pl.col("stake")).otherwise(0.0).alias("turn")))
r.write_parquet("data/processed/backtest/esports_more_trades.parquet")
V = ["N", "K", "min_usd"]
per = lambda p: [pl.col("filled").filter(pl.col("period") == p).sum().alias(f"fills_{p}"),
                 (pl.col("pnl").filter(pl.col("period") == p).sum() / pl.col("turn").filter(pl.col("period") == p).sum()).alias(f"roi_{p}")]
months = r.group_by(V + ["month"]).agg(pl.col("pnl").sum()).group_by(V).agg((pl.col("pnl") > 0).sum().alias("months_pos"), pl.len().alias("months"))
out = (r.group_by(V).agg(pl.len().alias("signals"), pl.col("filled").sum().alias("fills"), pl.col("turn").sum().alias("turnover"),
                         pl.col("pnl").sum().alias("pnl"), *per("train"), *per("test"),
                         (pl.col("pay").filter(pl.col("filled")) > 0.5).mean().alias("win"))
       .join(months, on=V)
       .with_columns((pl.col("pnl") / pl.col("turnover")).alias("roi"), (pl.col("fills") / 11).alias("fills_per_month"))
       .sort(V))
out.write_csv("reports/esports_more.csv")
with pl.Config(tbl_rows=40, tbl_cols=20, float_precision=3, tbl_width_chars=250):
    print(out.select(*V, "signals", "fills", "fills_per_month", "pnl", "roi", "roi_train", "roi_test", "fills_test", "months_pos", "months", "win"))
