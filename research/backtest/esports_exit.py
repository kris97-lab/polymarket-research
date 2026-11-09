"""Esports pre-match: hold to resolution vs sell shortly before the match starts.

Uses the filled trades of backtest/esports_more.py (same entry model). Exit = last public print of our outcome at or
before (game start - X min), minus a HAIRCUT for crossing the spread to the bid, fee 1% on the exit notional too.
If there is no print in the 6 h before the exit time, the position is held to resolution.
Output: reports/esports_exit.csv."""
import glob, os
import numpy as np
import polars as pl

FEE, HAIRCUT, LOOKBACK = 0.01, 0.01, 6 * 3600
EXITS_MIN = [1, 5, 15, 60]

tr = (pl.read_parquet("data/processed/backtest/esports_more_trades.parquet").filter(pl.col("filled"))
      .join(pl.read_parquet("data/processed/esports_game_start.parquet").select("market_id", "game_start_ts"), on="market_id"))
u = tr.select("market_id", "outcome", "t_sig", "game_start_ts").unique().with_columns(pl.from_epoch("t_sig").dt.strftime("%Y-%m").alias("month"))
files = sorted(glob.glob("data/processed/fills/*.parquet"))
rows = []
for (month,), g in u.group_by("month", maintain_order=True):
    fs = [f for f in files if os.path.basename(f)[:7] >= month][:2]
    ids = g["market_id"].unique().to_list()
    t = (pl.scan_parquet(fs).select("market_id", "ts", "price", "is_token1").filter(pl.col("market_id").is_in(ids)).collect()
         .with_columns(pl.when(pl.col("is_token1")).then(pl.col("price")).otherwise(1 - pl.col("price")).alias("py")).sort("market_id", "ts"))
    by_m = {k[0]: (d["ts"].to_numpy(), d["py"].to_numpy()) for k, d in t.group_by("market_id")}
    for market_id, outcome, t_sig, gs, _ in g.iter_rows():
        ts, py = by_m.get(market_id, (np.array([], dtype=np.int64), np.array([])))
        po = py if outcome == 0 else 1 - py
        out = [market_id, outcome, t_sig]
        for x in EXITS_MIN:
            te = gs - 60 * x
            k = np.searchsorted(ts, te, "right") - 1
            out.append(float(po[k]) if k >= 0 and ts[k] >= max(te - LOOKBACK, t_sig) else None)
        rows.append(out)
    print(month, g.height, flush=True)
px = pl.DataFrame(rows, schema=["market_id", "outcome", "t_sig"] + [f"px_{x}" for x in EXITS_MIN], orient="row")
tr = tr.join(px, on=["market_id", "outcome", "t_sig"])

shares = pl.col("stake") / pl.col("limit")
for x in EXITS_MIN:
    sell = (pl.col(f"px_{x}") - HAIRCUT).clip(0.0, 1.0)
    tr = tr.with_columns(pl.when(pl.col(f"px_{x}").is_null()).then(pl.col("pnl"))
                         .otherwise(shares * sell * (1 - FEE) - pl.col("stake") * (1 + FEE)).alias(f"pnl_exit{x}"),
                         (pl.col(f"px_{x}") - pl.col("limit")).alias(f"clv_{x}"))
tr.write_parquet("data/processed/backtest/esports_exit_trades.parquet")

V = ["N", "K", "min_usd"]
sel = tr.filter(((pl.col("N") == 50) & (pl.col("min_usd") == 10)) | ((pl.col("N") == 10) & (pl.col("K") == 1) & (pl.col("min_usd") == 500))
                | ((pl.col("N") == 100) & (pl.col("K") == 2) & (pl.col("min_usd") == 10)))
out = []
for key, g in sel.group_by(V, maintain_order=True):
    turn = g["stake"].sum()
    for name, col in [("hold", "pnl")] + [(f"exit {x} min", f"pnl_exit{x}") for x in EXITS_MIN]:
        m = g.group_by("month").agg(pl.col(col).sum())
        r = (g[col] / g["stake"]).to_numpy()
        out.append(dict(zip(V, key), exit=name, trades=g.height, pnl=float(g[col].sum()), roi=float(g[col].sum() / turn),
                        roi_train=float(g.filter(pl.col("period") == "train")[col].sum() / g.filter(pl.col("period") == "train")["stake"].sum()),
                        roi_test=float(g.filter(pl.col("period") == "test")[col].sum() / g.filter(pl.col("period") == "test")["stake"].sum()),
                        sd_per_trade=float(r.std()), t_stat=float(r.mean() / r.std() * np.sqrt(len(r))),
                        months_pos=int((m[col] > 0).sum()), months=m.height,
                        no_exit_price=int(g[f"px_{name.split()[1]}"].null_count()) if name != "hold" else 0,
                        clv=float(g[f"clv_{name.split()[1]}"].mean()) if name != "hold" else None))
res = pl.DataFrame(out)
res.write_csv("reports/esports_exit.csv")
with pl.Config(tbl_rows=40, tbl_cols=20, float_precision=3, tbl_width_chars=250):
    print(res)
