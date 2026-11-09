"""Build data/polymarket.duckdb: markets (with category & payouts), fills (HF, compact), api_trades (gap),
and the unified per-wallet view `trades`:
  ts, wallet, market_id, outcome, outcome_idx, side, price, size_usd, tx_hash, block, log_index, role, source,
  yes_price, yes_qty (signed YES-equivalent tokens; buying NO == selling YES)."""
import sys, json, glob
import duckdb
sys.path.insert(0, "ingest")
from universe import universe_sql
from categories import categorize, sports_prefixes

DB = "data/polymarket.duckdb"
con = duckdb.connect(DB, config={"memory_limit": "3GB"})
prefixes = sports_prefixes()

# --- event categories (Gamma tags) ---
tags = con.sql("select event_id, any_value(event_slug) event_slug, any_value(tags) tags from read_json('data/cache/gamma_events/*.jsonl', format='newline_delimited') group by 1").fetchall()
gap_path = "data/cache/data_api/gap_markets.json"
gap = json.load(open(gap_path)) if glob.glob(gap_path) else []
cat_rows = [(e, categorize(t, s, prefixes)) for e, s, t in tags]
con.sql("create or replace table event_category (event_id varchar, category varchar)")
con.executemany("insert into event_category values (?, ?)", cat_rows)

# --- markets: HF universe + gap markets from Gamma ---
con.sql(f"""create or replace table markets as
  select u.*, coalesce(c.category, case when split_part(u.event_slug,'-',1) in (select unnest(?::varchar[])) then 'sports' else 'other' end) category,
         'hf' source
  from ({universe_sql(start='2025-07-01', end='2026-10-05')}) u left join event_category c using (event_id)""", params=[sorted(prefixes)])
if gap:
    rows = []
    for m in gap:
        try:
            outs, prices = json.loads(m["outcomes"]), [float(x) for x in json.loads(m["outcome_prices"])]
        except (TypeError, ValueError):
            continue
        if len(outs) != 2 or m["market_id"] in ():
            continue
        rows.append((m["market_id"], m["slug"], m["event_id"], m["event_slug"], m["question"], outs[0], outs[1],
                     int(bool(m["neg_risk"])), m["volume"], m["created_at"], m["end_date"], prices[0], prices[1],
                     categorize([], m["event_slug"], prefixes), "api", m["condition_id"], json.loads(m["clob_token_ids"] or "[]")))
    con.sql("""create or replace temp table gm (market_id varchar, slug varchar, event_id varchar, event_slug varchar, question varchar,
               answer1 varchar, answer2 varchar, neg_risk int, volume double, created_at timestamptz, end_date timestamptz,
               payout1 double, payout2 double, category varchar, source varchar, condition_id varchar, tokens varchar[])""")
    con.executemany("insert into gm values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.sql("""insert into markets select market_id, slug, event_id, event_slug, question, answer1, answer2, neg_risk, volume, created_at, end_date,
               payout1, payout2, coalesce(c.category, gm.category), source
               from gm left join event_category c using (event_id) where market_id not in (select market_id from markets)""")
# winner index: 0/1 when one payout clearly dominates, null for 50/50 etc.
con.sql("alter table markets add column winner_idx int")
con.sql("update markets set winner_idx = case when payout1 >= 0.98 then 0 when payout2 >= 0.98 then 1 end")

# --- fills (HF) and api_trades (gap) ---
con.sql("create or replace view fills as select * from read_parquet('data/processed/fills/*.parquet')")
api_files = glob.glob("data/cache/data_api/trades/*.jsonl")
if api_files and gap:
    con.sql("""create or replace table api_trades as
      select t.timestamp::uint32 ts, t.proxy_wallet wallet, g.market_id, t.outcome_index::int outcome_idx,
             t.side, t.price::float price, (t.size * t.price)::float usd, t.transaction_hash tx_hash
      from read_json('data/cache/data_api/trades/*.jsonl', format='newline_delimited',
                     columns={proxy_wallet:'varchar', side:'varchar', token_id:'varchar', condition_id:'varchar', size:'double',
                              price:'double', timestamp:'bigint', outcome:'varchar', outcome_index:'int', transaction_hash:'varchar'}) t
      join gm g on t.condition_id = g.condition_id
      where t.size * t.price >= 1""")
else:
    con.sql("create or replace table api_trades (ts uint32, wallet varchar, market_id varchar, outcome_idx int, side varchar, price float, usd float, tx_hash varchar)")

con.sql("""create or replace view trades as
  with sides as (
    select ts, maker wallet, market_id, case when is_token1 then 0 else 1 end outcome_idx,
           case when maker_buy then 'BUY' else 'SELL' end side, price, usd size_usd,
           null::varchar tx_hash, block, log_index, 'maker' role, 'hf' source from fills
    union all
    select ts, taker, market_id, case when is_token1 then 0 else 1 end,
           case when maker_buy then 'SELL' else 'BUY' end, price, usd,
           null, block, log_index, 'taker', 'hf' from fills
    union all
    select ts, wallet, market_id, outcome_idx, side, price, usd, tx_hash, null, null, null, 'api' from api_trades
  )
  select s.ts, s.wallet, s.market_id, case when s.outcome_idx = 0 then m.answer1 else m.answer2 end outcome, s.outcome_idx,
         s.side, s.price, s.size_usd, s.tx_hash, s.block, s.log_index, s.role, s.source,
         case when s.outcome_idx = 0 then s.price else 1 - s.price end yes_price,
         (s.size_usd / nullif(s.price, 0)) * case when (s.outcome_idx = 0) = (s.side = 'BUY') then 1 else -1 end yes_qty
  from sides s join markets m using (market_id)""")
print(con.sql("select source, category, count(*) n, round(sum(volume)/1e9,2) vol_bn from markets group by all order by 1, n desc"))
