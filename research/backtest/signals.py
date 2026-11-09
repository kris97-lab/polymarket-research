"""Consensus signals: >= K distinct top-N wallets (ranked at the start of the month) entered the same outcome
of the same market within W hours. One signal per (config, market): the first one.

Ex-ante eligibility (no look-ahead): market's scheduled end within HORIZON days after the signal,
trigger price inside [MIN_REF, MAX_REF]. Also emits the 'best single wallet' baseline (N=1, K=1)."""
import datetime as dt
import polars as pl

SCOPES = ["all", "sports", "esports", "weather", "crypto", "finance", "geopolitics", "politics", "culture", "tech"]
NS = [1, 10, 25, 50]
KS = [2, 3, 4]
WS_H = [1, 6, 24]
MIN_ENTRY_USD = 10
HORIZON_D = 30
MIN_REF, MAX_REF = 0.02, 0.95
FIRST_R, LAST_R = dt.date(2025, 10, 1), dt.date(2026, 9, 1)
SIGNAL_END = dt.datetime(2026, 9, 4, tzinfo=dt.timezone.utc)  # HF data end (2026-10-04) minus horizon

ts_of = lambda d: int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())

import duckdb
markets = duckdb.connect("data/polymarket.duckdb", read_only=True).sql(
    "select market_id, category, epoch(end_date)::bigint end_ts, winner_idx, payout1, payout2 from markets where source = 'hf'").pl()
scores = (pl.scan_parquet("data/processed/scores/scores.parquet")
          .filter(pl.col("rank") <= max(NS), pl.col("scope").is_in(SCOPES))
          .select("rebalance", "scope", "wallet", "rank").collect())
entries = (pl.scan_parquet("data/processed/backtest/entries/*.parquet")
           .filter(pl.col("usd") >= MIN_ENTRY_USD, pl.col("ts") >= ts_of(FIRST_R), pl.col("ts") < int(SIGNAL_END.timestamp()))
           .select("wallet", "market_id", "ts", "outcome", "price_o", "usd")
           .join(markets.lazy().select("market_id", "category", "end_ts"), on="market_id")
           .with_columns(pl.from_epoch("ts").dt.truncate("1mo").dt.date().alias("rebalance"))
           .collect())
print("entries in signal period:", entries.height, flush=True)

out = []
for scope in SCOPES:
    e = entries if scope == "all" else entries.filter(pl.col("category") == scope)
    sc = scores.filter(pl.col("scope") == scope)
    for N in NS:
        top = e.join(sc.filter(pl.col("rank") <= N).select("rebalance", "wallet"), on=["rebalance", "wallet"])
        if top.is_empty():
            continue
        top = top.with_columns(pl.from_epoch("ts").alias("t")).sort("market_id", "outcome", "t")
        for W in (WS_H if N > 1 else [1]):
            nw = (top.rolling(index_column="t", period=f"{W}h", group_by=["market_id", "outcome"])
                  .agg(pl.col("wallet").n_unique().alias("n_wallets"), pl.col("price_o").last().alias("ref_price"),
                       pl.col("rebalance").last(), pl.col("end_ts").last(), pl.col("category").last()))
            for K in (KS if N > 1 else [1]):
                sig = (nw.filter(pl.col("n_wallets") >= K)
                       .with_columns(pl.col("t").dt.epoch("s").alias("t_sig"))
                       .filter(pl.col("end_ts") > pl.col("t_sig"), pl.col("end_ts") - pl.col("t_sig") <= HORIZON_D * 86400,
                               pl.col("ref_price").is_between(MIN_REF, MAX_REF))
                       .sort("t_sig").group_by("market_id", maintain_order=True).first()   # one position per market
                       .with_columns(pl.lit(scope).alias("scope"), pl.lit(N).alias("N"), pl.lit(K).alias("K"), pl.lit(W).alias("W_h")))
                out.append(sig.select("scope", "N", "K", "W_h", "rebalance", "market_id", "outcome", "t_sig", "ref_price",
                                      "n_wallets", "category", "end_ts"))
        print(scope, N, "done", flush=True)

sig = pl.concat(out).join(markets.select("market_id", "winner_idx", "payout1", "payout2"), on="market_id")
sig = sig.with_columns(pl.when(pl.col("outcome") == 0).then(pl.col("payout1")).otherwise(pl.col("payout2")).alias("payout_o"))
sig = sig.with_columns(pl.when(pl.col("winner_idx").is_null()).then(pl.col("payout_o"))
                       .when(pl.col("winner_idx") == pl.col("outcome")).then(1.0).otherwise(0.0).alias("payout_o"))
sig.write_parquet("data/processed/backtest/signals.parquet")
print(sig.group_by("scope", "N", "K", "W_h").len().sort("scope", "N", "K", "W_h").filter(pl.col("scope").is_in(["all", "weather"])))
print("total signal rows:", sig.height, " unique (market,outcome,t_sig):", sig.select("market_id", "outcome", "t_sig").unique().height)
