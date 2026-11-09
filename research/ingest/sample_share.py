"""Estimate what share of trade rows/volume fall into the universe, by sampling remote row groups."""
import sys, duckdb, pyarrow.parquet as pq
from huggingface_hub import HfFileSystem
sys.path.insert(0, "ingest")
from universe import universe_sql

fs = HfFileSystem()
pf = pq.ParquetFile(fs.open("datasets/SII-WANGZJ/Polymarket_data/trades.parquet"))
con = duckdb.connect()
con.sql(f"create table u as {universe_sql()}")
for g in [int(x) for x in sys.argv[1:]]:
    t = pf.read_row_group(g, columns=["timestamp", "market_id", "usd_amount"])
    con.register("t", t)
    print(g, con.sql("""select to_timestamp(min(timestamp))::date d, count(*) n,
        round(avg((u.market_id is not null)::int),3) row_share,
        round(sum(usd_amount) filter (where u.market_id is not null)/sum(usd_amount),3) vol_share,
        round(avg((usd_amount<1)::int),3) tiny_share
        from t left join u using(market_id)""").fetchall())
