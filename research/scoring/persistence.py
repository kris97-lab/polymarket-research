"""Out-of-sample check of wallet skill: wallets ranked at rebalance R are evaluated on markets they ENTERED
during [R, next R) (first trade after R), compared with all other qualified wallets of the same scope."""
import sys
import duckdb

TOP_N = [10, 25, 50, 100]

con = duckdb.connect(config={"memory_limit": "3GB", "threads": 4, "temp_directory": "data/tmp", "max_temp_directory_size": "6GiB"})
con.sql("create view sc as select * from 'data/processed/scores/scores.parquet'")
con.sql("create view wm as select * from 'data/processed/wallet_market/*.parquet'")
con.sql("""create temp table fwd as
  select s.rebalance, s.scope, s.wallet, s.rank, s.is_mm, s.is_hedge, w.pnl, w.cost, w.pos_qty + w.neg_qty qty
  from sc s join wm w on w.wallet = s.wallet
   and to_timestamp(w.first_ts) >= s.rebalance and to_timestamp(w.first_ts) < s.rebalance + interval 1 month
   and (s.scope = 'all' or w.category = s.scope)""")
buckets = " ".join(f"when rank <= {n} then 'top{n:03d}'" for n in TOP_N)
con.sql(f"""create temp table res as
  select scope, case {buckets} when rank is not null then 'rest_rankable' when is_mm then 'market_makers' else 'hedge_or_losing' end grp,
         count(distinct wallet || rebalance) wallet_months, count(*) markets, sum(pnl) pnl, sum(pnl)/sum(cost) roi, sum(pnl)/sum(qty) edge,
         count(distinct rebalance) n_months
  from fwd group by all""")
# cumulative top-N buckets (top25 includes top10, etc.)
con.sql(f"""create temp table cum as
  select scope, 'top' || lpad(n::varchar, 3, '0') grp, count(distinct wallet || rebalance) wallet_months, count(*) markets,
         sum(pnl) pnl, sum(pnl)/sum(cost) roi, sum(pnl)/sum(qty) edge,
         count(distinct rebalance) filter (where pnl is not null) n_months
  from fwd, (select unnest({TOP_N}) n) where rank <= n group by all
  union all
  select scope, grp, wallet_months, markets, pnl, roi, edge, n_months from res where grp in ('rest_rankable','market_makers','hedge_or_losing')""")
con.sql("copy cum to 'data/processed/scores/persistence.parquet' (format parquet)")
print(con.sql("select scope, grp, wallet_months, markets, round(pnl/1e3) pnl_k, round(roi,4) roi, round(edge,4) edge from cum order by scope = 'all' desc, scope, grp").df().to_string())
# month-by-month for top50 overall: is the edge stable or driven by one month?
con.sql("""create temp table bymonth as
  select rebalance, sum(pnl) filter (where rank <= 50) / sum(qty) filter (where rank <= 50) top50_edge,
         sum(pnl) filter (where rank <= 50) / sum(cost) filter (where rank <= 50) top50_roi,
         sum(pnl) filter (where rank > 50) / sum(qty) filter (where rank > 50) rest_edge,
         sum(pnl) filter (where rank <= 50) top50_pnl
  from fwd where scope = 'all' group by 1 order by 1""")
con.sql("copy bymonth to 'data/processed/scores/persistence_by_month.parquet' (format parquet)")
print(con.sql("select rebalance, round(top50_edge,4) top50_edge, round(top50_roi,4) top50_roi, round(rest_edge,4) rest_edge, round(top50_pnl/1e3) top50_pnl_k from bymonth").df().to_string())
