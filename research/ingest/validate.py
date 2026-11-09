"""Sanity checks for the unified trades view."""
import duckdb

con = duckdb.connect("data/polymarket.duckdb", read_only=True, config={"memory_limit": "3GB", "threads": 4, "preserve_insertion_order": False, "temp_directory": "data/tmp", "max_temp_directory_size": "5GiB"})
q = lambda s: print(con.sql(s))

# coverage by month and source
q("""select strftime(to_timestamp(ts), '%Y-%m') mo, source, count(*) rows, approx_count_distinct(wallet) wallets,
            approx_count_distinct(market_id) markets, round(sum(size_usd)/2e6, 1) fill_vol_musd
     from trades group by all order by 1, 2""")
# zero-sum: YES-equivalent qty must net to ~0 across both sides of every HF fill
q("select round(sum(yes_qty), 3) net_yes_qty, round(sum(abs(yes_qty))/1e9, 3) gross_bn from trades where source = 'hf'")
# prices within (0,1), no nulls in key fields
q("""select count(*) filter (where price <= 0 or price >= 1) bad_price, count(*) filter (where wallet is null) null_wallet,
            count(*) filter (where outcome is null) null_outcome from trades""")
# traded USD vs Gamma market volume (HF volume = fills counted once)
q("""with v as (select market_id, sum(size_usd)/2 v from trades where source='hf' group by 1)
     select category, count(*) markets, round(median(v.v / nullif(m.volume, 0)), 3) median_vol_ratio
     from v join markets m using (market_id) group by 1 order by markets desc""")
q("select category, count(*) markets, count(*) filter (where winner_idx is null) no_clear_winner from markets group by 1 order by 2 desc")
q("select * from (select * from fills limit 2) f, lateral (select answer1, answer2, category from markets m where m.market_id = f.market_id)")
