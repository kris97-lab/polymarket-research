"""Esports: consensus signals before the match vs during it (in-play), split by Gamma gameStartTime.

Polymarket clears esports order books at match start (clearBookOnStart), so a limit order placed before the start
cannot fill after it: such fills are dropped. fill_vol of a pre-match order whose window crosses the start may still
include in-play prints (only the aggregate is stored), so `clean` marks orders whose whole TTL ends before the start.
Outputs reports/esports_*.csv."""
import numpy as np
import polars as pl
from sim import run_period, TRAIN, TEST, EXEC, CFG

gs = pl.read_parquet("data/processed/esports_game_start.parquet").select("market_id", "game_start_ts")
sig = (pl.read_parquet("data/processed/backtest/signals.parquet").filter(pl.col("scope") == "esports")
       .join(gs, on="market_id", how="inner")
       .with_columns(pl.when(pl.col("t_sig") < pl.col("game_start_ts")).then(pl.lit("pre")).otherwise(pl.lit("live")).alias("phase"),
                     ((pl.col("game_start_ts") - pl.col("t_sig")) / 3600).alias("h_to_start")))
keys = sig.select("market_id", "outcome", "t_sig").unique()
ex = (pl.read_parquet("data/processed/backtest/executions/*.parquet").with_columns(pl.col("moved").cast(pl.Boolean))
      .unique(subset=["market_id", "outcome", "t_sig", "seed"] + EXEC, keep="first")
      .join(keys, on=["market_id", "outcome", "t_sig"])
      .join(gs.rename({"market_id": "exec_market_id", "game_start_ts": "exec_start"}), on="exec_market_id", how="left")
      # book is cleared at the start: a pre-match order gets nothing at/after it
      .with_columns(pl.when((pl.col("t_sig") < pl.col("exec_start")) & (pl.col("fill_ts") >= pl.col("exec_start")))
                    .then(0.0).otherwise(pl.col("fill_vol")).alias("fill_vol"),
                    (pl.col("t_sig") + pl.col("D") + pl.col("TTL") <= pl.col("exec_start")).fill_null(False).alias("clean")))
period_of = lambda t: pl.when(t < int(TEST[0].timestamp())).then(pl.lit("train")).otherwise(pl.lit("test"))
lo, hi = int(TRAIN[0].replace(tzinfo=__import__("datetime").timezone.utc).timestamp()), int(TEST[1].replace(tzinfo=__import__("datetime").timezone.utc).timestamp())

# 1) signal-level edge (one row per unique signal), strategy vs random, reference execution
u = sig.select("market_id", "outcome", "t_sig", "phase", "h_to_start").unique(["market_id", "outcome", "t_sig"])
lvl = (ex.filter(pl.col("D") == 60, pl.col("offset") == 0.01, pl.col("TTL") == 3600, pl.col("t_sig") >= lo, pl.col("t_sig") < hi)
       .join(u, on=["market_id", "outcome", "t_sig"])
       .with_columns(period_of(pl.col("t_sig")).alias("period"), pl.when(pl.col("seed") == -1).then(pl.lit("consensus")).otherwise(pl.lit("random")).alias("who"),
                     ((pl.col("fill_vol") > 0) & ~pl.col("moved")).alias("filled"),
                     pl.col("h_to_start").cut([0, 1, 6], labels=["live", "pre <1h", "pre 1-6h", "pre >6h"]).cast(pl.Utf8).alias("bucket")))
edge = lambda by: (lvl.group_by(by).agg(pl.len().alias("n"), (pl.col("payout_o") - pl.col("ref_price")).mean().alias("edge_ideal"),
                                        pl.col("filled").mean().alias("fill_rate"),
                                        (pl.col("payout_o") - pl.col("limit")).filter(pl.col("filled")).mean().alias("edge_filled"),
                                        (pl.col("payout_o") - pl.col("limit")).filter(~pl.col("filled") & ~pl.col("moved")).mean().alias("edge_unfilled"))
                   .sort(by))
e1 = edge(["who", "phase", "period"])
e2 = edge(["who", "bucket", "period"]).filter(pl.col("who") == "consensus")
e1.write_csv("reports/esports_signal_edge.csv")
e2.write_csv("reports/esports_signal_edge_by_hours.csv")
with pl.Config(tbl_rows=40, float_precision=3, tbl_width_chars=200):
    print(e1); print(e2)

# 2) portfolio grid per phase (pre-match also restricted to clean windows)
rows = []
for phase, flt in (("live", None), ("pre", None), ("pre_clean", "clean")):
    s_ph = sig.filter(pl.col("phase") == ("live" if phase == "live" else "pre"))
    e_ph = ex.filter(pl.col("clean")) if flt else ex
    for cfg, g in s_ph.group_by(CFG):
        for name, period in (("train", TRAIN), ("test", TEST)):
            for seeds, who in (((-1,), "consensus"), ((0, 1, 2), "random")):
                for r in run_period(g, e_ph, period, seeds=seeds):
                    r.pop("cat_pnl")
                    r.update(dict(zip(CFG, cfg)), period=name, phase=phase, who=who)
                    rows.append(r)
grid = pl.DataFrame(rows)
grid.write_parquet("data/processed/backtest/esports_split_grid.parquet")
key = ["phase", "N", "K", "W_h"] + EXEC
agg = (grid.group_by(key + ["period", "who"]).agg(pl.col("roi").mean(), pl.col("trades").mean(), pl.col("pnl").mean(), pl.col("sharpe").mean())
       .pivot(on=["who", "period"], index=key, values=["roi", "trades", "pnl", "sharpe"]))
agg.write_parquet("data/processed/backtest/esports_split_agg.parquet")
ok = agg.filter(pl.col('trades_{"consensus","train"}') >= 10, pl.col('trades_{"consensus","test"}') >= 10)
rob = (ok.group_by("phase").agg(pl.len().alias("configs"),
                                (pl.col('roi_{"consensus","test"}') > 0).mean().alias("share_pos_test"),
                                pl.col('roi_{"consensus","train"}').median().alias("med_roi_train"),
                                pl.col('roi_{"consensus","test"}').median().alias("med_roi_test"),
                                pl.col('roi_{"random","test"}').median().alias("med_roi_test_random"),
                                pl.corr(pl.col('roi_{"consensus","train"}'), pl.col('roi_{"consensus","test"}'), method="spearman").alias("spearman"))
       .sort("phase"))
best = (ok.sort('sharpe_{"consensus","train"}', descending=True).group_by("phase", maintain_order=True).head(3)
        .select(*key, pl.col('trades_{"consensus","train"}').alias("tr_n"), pl.col('roi_{"consensus","train"}').alias("tr_roi"),
                pl.col('trades_{"consensus","test"}').alias("te_n"), pl.col('roi_{"consensus","test"}').alias("te_roi"),
                pl.col('roi_{"random","test"}').alias("te_roi_rand")))
rob.write_csv("reports/esports_grid_robustness.csv")
best.write_csv("reports/esports_best_on_train.csv")
with pl.Config(tbl_rows=40, float_precision=3, tbl_width_chars=250, tbl_cols=20):
    print(rob); print(best)
