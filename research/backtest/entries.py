"""All trades of wallets that ever appear in a top-100, with running position per (wallet, market),
so that only ENTRIES (trades that grow exposure toward an outcome) can count toward consensus signals.
Exits (selling YES you hold) are not "buying NO".

Two explicit steps keep memory and temp disk bounded: (1) Polars, one month file at a time -> sides of top wallets,
(2) the running-position window per wallet bucket on that much smaller file."""
import os, glob
import duckdb
import polars as pl

OUT = "data/processed/backtest"
BUCKETS = 8

con = duckdb.connect(config={"memory_limit": "3GB", "threads": 4, "preserve_insertion_order": False,
                             "temp_directory": "data/tmp", "max_temp_directory_size": "4GiB"})

sides = f"{OUT}/top_wallet_sides"
os.makedirs(sides, exist_ok=True)
tw = pl.read_parquet("data/processed/scores/scores.parquet", columns=["wallet", "rank"]).filter(pl.col("rank") <= 100)["wallet"].unique()
for f in sorted(glob.glob("data/processed/fills/*.parquet")):
    path = f"{sides}/{os.path.basename(f)}"
    if os.path.exists(path):
        continue
    lf = pl.scan_parquet(f)
    cols = ["market_id", "ts", "block", "log_index", "usd", "price", "is_token1"]
    m = lf.filter(pl.col("maker").is_in(tw.implode())).select(pl.col("maker").alias("wallet"), *cols, pl.col("maker_buy").alias("is_buy"))
    t = lf.filter(pl.col("taker").is_in(tw.implode())).select(pl.col("taker").alias("wallet"), *cols, (~pl.col("maker_buy")).alias("is_buy"))
    pl.concat([m, t]).sink_parquet(path + ".tmp")
    os.rename(path + ".tmp", path)
sides = f"{sides}/*.parquet"
print("sides:", con.sql(f"select count(*) from '{sides}'").fetchall()[0][0], flush=True)

os.makedirs(f"{OUT}/entries", exist_ok=True)
for b in range(BUCKETS):
    path = f"{OUT}/entries/b{b}.parquet"
    if os.path.exists(path):
        continue
    con.sql(f"""create or replace temp table q as
      select *, usd / price * case when is_token1 = is_buy then 1 else -1 end yes_qty,
             case when is_token1 then price else 1 - price end yes_price
      from '{sides}' where hash(wallet) % {BUCKETS} = {b}""")
    con.sql(f"""copy (
      select wallet, market_id, ts, block, log_index, usd,
             case when yes_qty > 0 then 0 else 1 end outcome,                     -- 0 = answer1 (YES), 1 = answer2 (NO)
             case when yes_qty > 0 then yes_price else 1 - yes_price end price_o, -- price paid for that outcome
             pos_after - yes_qty pos_before, pos_after
      from (select *, sum(yes_qty) over (partition by wallet, market_id order by ts, block, log_index
                                          rows unbounded preceding) pos_after from q)
      where (yes_qty > 0 and pos_after > 0) or (yes_qty < 0 and pos_after < 0)   -- exposure grows toward the traded outcome
    ) to '{path}.tmp' (format parquet, compression zstd)""")
    os.rename(f"{path}.tmp", path)
    print("bucket", b, flush=True)

print(con.sql(f"""select count(*) entries, count(distinct wallet) wallets, count(distinct market_id) markets,
                 round(sum(usd)/1e6) usd_m from '{OUT}/entries/*.parquet'""").fetchall())
