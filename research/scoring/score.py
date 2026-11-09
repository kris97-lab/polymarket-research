"""Walk-forward wallet scoring. For each monthly rebalance date R only markets that resolved before R are used
(end_date in [R - LOOKBACK, R)), so nothing after R leaks into the score.

Metrics per wallet (overall and per category): realized PnL, ROI = pnl/cost, closed markets, win rate,
edge = pnl / gross tokens (avg of (payout - entry price) signed by direction, token-weighted),
mean per-market edge and its lower confidence bound (ranking key).
Flags: market maker (excluded), hedge/arb suspect (marked; excluded from top-N by default)."""
import os, datetime as dt
import duckdb

CFG = dict(
    first_rebalance="2025-10-01", last_rebalance="2026-11-01",
    lookback_months=6,            # trailing window of resolved markets
    data_start="2025-07-01",      # earliest end_date in the market universe
    min_markets=100,              # closed markets overall in the window
    min_markets_category=30,      # closed markets inside the category for category scopes
    min_cost=5_000,               # capital deployed in the window: below that signals are noise, not copyable
    z=1.0,                        # rank by mean_edge - z * stderr
    mm_rt=0.5, mm_maker_share=0.7, mm_tpm=10, mm_avg_trade=50,
    hedge_abs_edge=0.005, hedge_min_cost=100_000,
)
WM = "data/processed/wallet_market/*.parquet"
OUT = "data/processed/scores"


def rebalance_dates():
    d = dt.date.fromisoformat(CFG["first_rebalance"])
    while d <= dt.date.fromisoformat(CFG["last_rebalance"]):
        yield d
        d = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)


def metrics_sql(group_cols):
    return f"""
      select {group_cols}, count(*) n_markets, sum(n_trades) n_trades, sum(usd) usd, sum(cost) cost, sum(pnl) pnl,
             sum(pnl) / nullif(sum(cost), 0) roi,
             avg((pnl > 0)::int) win_rate,
             sum(pnl) / nullif(sum(pos_qty + neg_qty), 0) edge,
             avg(pnl / nullif(pos_qty + neg_qty, 0)) mean_edge,
             stddev_samp(pnl / nullif(pos_qty + neg_qty, 0)) / sqrt(count(*)) se_edge,
             sum(maker_usd) / nullif(sum(usd), 0) maker_share,
             sum(least(pos_qty, neg_qty)) / nullif(sum(greatest(pos_qty, neg_qty)), 0) two_sided,
             sum(n_trades) / count(*) trades_per_market,
             sum(usd) / sum(n_trades) avg_trade_usd,
             max(pnl) / nullif(sum(pnl) filter (where pnl > 0), 0) top1_pnl_share
      from win group by {group_cols}"""


def main():
    os.makedirs(OUT, exist_ok=True)
    con = duckdb.connect(config={"memory_limit": "3GB", "threads": 4, "preserve_insertion_order": False,
                                 "temp_directory": "data/tmp", "max_temp_directory_size": "6GiB"})
    c = CFG
    parts = []
    for R in rebalance_dates():
        lo = max(dt.date.fromisoformat(c["data_start"]),
                 (R.replace(day=1) - dt.timedelta(days=31 * c["lookback_months"] - 15)).replace(day=1))
        con.sql(f"create or replace temp table win as select * from '{WM}' where end_date >= '{lo}' and end_date < '{R}'")
        con.sql(f"create or replace temp table overall as {metrics_sql('wallet')}")
        con.sql(f"create or replace temp table bycat as {metrics_sql('wallet, category')}")
        con.sql(f"""create or replace temp table s as
          with flags as (
            select wallet, n_markets n_markets_all,
                   two_sided >= {c['mm_rt']} and (maker_share >= {c['mm_maker_share']}
                       or (trades_per_market >= {c['mm_tpm']} and avg_trade_usd < {c['mm_avg_trade']})) is_mm,
                   abs(edge) < {c['hedge_abs_edge']} and cost >= {c['hedge_min_cost']} is_hedge
            from overall)
          select 'all' scope, o.*, f.n_markets_all, f.is_mm, f.is_hedge from overall o join flags f using (wallet)
          where o.n_markets >= {c['min_markets']}
          union all by name
          select b.category scope, b.* exclude (category), f.n_markets_all, f.is_mm, f.is_hedge from bycat b join flags f using (wallet)
          where f.n_markets_all >= {c['min_markets']} and b.n_markets >= {c['min_markets_category']}""")
        con.sql(f"""create or replace temp table r{R:%Y%m} as
          select '{R}'::date rebalance, '{lo}'::date window_start, *, mean_edge - {c['z']} * coalesce(se_edge, 1) score,
                 case when not is_mm and not is_hedge and pnl > 0 and cost >= {c['min_cost']} then
                      row_number() over (partition by scope, (not is_mm and not is_hedge and pnl > 0 and cost >= {c['min_cost']})
                                         order by mean_edge - {c['z']} * coalesce(se_edge, 1) desc) end rank
          from s""")
        parts.append(f"r{R:%Y%m}")
        n = con.sql(f"select count(*) filter (where scope='all'), count(*) filter (where scope='all' and rank is not null), count(*) filter (where is_mm and scope='all') from r{R:%Y%m}").fetchall()[0]
        print(f"{R}: window {lo}..{R}, qualified {n[0]:,}, rankable {n[1]:,}, mm {n[2]:,}", flush=True)
    con.sql(f"copy ({' union all by name '.join(f'select * from {p}' for p in parts)}) to '{OUT}/scores.parquet' (format parquet)")


if __name__ == "__main__":
    main()
