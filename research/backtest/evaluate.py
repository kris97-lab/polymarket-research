"""Out-of-sample evaluation of configs selected on TRAIN: strategy vs random-entry baseline (same timestamps,
same category, same execution params), fee sensitivity, adverse selection, PnL by category."""
import sys
import polars as pl
sys.path.insert(0, "backtest")
from sim import run_period, TRAIN, TEST, CFG, EXEC, MIN_TRADES

pl.Config.set_tbl_rows(60); pl.Config.set_tbl_width_chars(250); pl.Config.set_fmt_str_lengths(80)
KEY = CFG + EXEC
g = pl.read_parquet("data/processed/backtest/grid_fee100.parquet")
w = g.filter(pl.col("period") == "train").join(g.filter(pl.col("period") == "test"), on=KEY, suffix="_test", how="left")
ok = w.filter(pl.col("trades") >= MIN_TRADES, pl.col("N") > 1)
picks = pl.concat([ok.sort("sharpe", descending=True).head(1).with_columns(pl.lit("best_overall").alias("pick")),
                   ok.sort("sharpe", descending=True).group_by("scope", maintain_order=True).first().with_columns(pl.lit("best_in_scope").alias("pick"))], how="diagonal")
# best single wallet baseline: same selection rule among N=1 configs
b1 = w.filter(pl.col("N") == 1, pl.col("trades") >= 10).sort("sharpe", descending=True).head(1).with_columns(pl.lit("best_single_wallet").alias("pick"))
picks = pl.concat([picks, b1], how="diagonal")

sig = pl.read_parquet("data/processed/backtest/signals.parquet")
ex = (pl.read_parquet("data/processed/backtest/executions/*.parquet").with_columns(pl.col("moved").cast(pl.Boolean))
      .unique(subset=["market_id", "outcome", "t_sig", "seed"] + EXEC, keep="first"))
rows = []
for p in picks.iter_rows(named=True):
    s = sig.filter(*[pl.col(c) == p[c] for c in CFG])
    e = ex.filter(*[pl.col(c) == p[c] for c in EXEC])
    for period_name, period in (("train", TRAIN), ("test", TEST)):
        for fee in (0, 50, 100, 200):
            for r in run_period(s, e, period, seeds=(-1, 0, 1, 2), fee_bps=fee):
                r.update({c: p[c] for c in KEY}, pick=p["pick"], period=period_name, fee_bps=fee,
                         kind="strategy" if r["seed"] == -1 else "random")
                r["cat_pnl"] = str({k: round(v) for k, v in r["cat_pnl"].items()})
                rows.append(r)
res = pl.DataFrame(rows)
res.write_parquet("data/processed/backtest/evaluation.parquet")
agg = (res.group_by("pick", *KEY, "period", "fee_bps", "kind")
       .agg(pl.col("trades").mean(), pl.col("fill_rate").mean(), pl.col("roi").mean(), pl.col("sharpe").mean(), pl.col("max_dd").mean(),
            pl.col("ret_filled").mean(), pl.col("ret_unfilled").mean()))
show = agg.filter(pl.col("fee_bps") == 100).pivot(on=["period", "kind"], index=["pick"] + KEY,
                                                  values=["roi", "sharpe", "trades"]).with_columns(pl.col(pl.Float64).round(3))
print(show)
fee = agg.filter(pl.col("kind") == "strategy", pl.col("period") == "test").pivot(on="fee_bps", index=["pick", "scope"], values="roi").with_columns(pl.col(pl.Float64).round(3))
print("test ROI by fee (bps):"); print(fee)
