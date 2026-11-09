"""Stream trades.parquet from HuggingFace month by month (column projection + row-group pruning),
keep only universe markets and fills >= $1, write compact monthly Parquet to data/processed/fills/."""
import sys, os, time, datetime as dt
import duckdb, requests
sys.path.insert(0, "ingest")
from universe import universe_sql

SRC = "hf://datasets/SII-WANGZJ/Polymarket_data/trades.parquet"
OUT = "data/processed/fills"
MIN_USD = 1.0


def cdn_url():
    """Resolve the HF file once to its signed CDN URL: range reads then bypass the rate-limited resolver."""
    for k in range(8):
        r = requests.head(SRC.replace("hf://datasets/", "https://huggingface.co/datasets/").replace("/trades.parquet", "/resolve/main/trades.parquet"),
                          allow_redirects=False, timeout=30)
        if r.status_code in (301, 302, 307) and r.headers.get("location"):
            return r.headers["location"].replace("*", "%2A")  # DuckDB treats * as a glob
        time.sleep(int(r.headers.get("retry-after", 30)) + 1)
    raise RuntimeError(f"cannot resolve CDN url: {r.status_code}")


def months(start, end):
    d = dt.date.fromisoformat(start)
    while d < dt.date.fromisoformat(end):
        nxt = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        yield d, nxt
        d = nxt


def main(start="2024-01-01", end="2026-10-05"):
    os.makedirs(OUT, exist_ok=True)
    con = duckdb.connect(config={"memory_limit": "3GB", "threads": 6})
    con.sql(f"create table u as select market_id, end_date from ({universe_sql(start='2025-07-01', end='2026-10-05')})")
    for lo, hi in months(start, end):
        path = f"{OUT}/{lo:%Y-%m}.parquet"
        if os.path.exists(path):
            continue
        t0 = time.time()
        a, b = int(dt.datetime(lo.year, lo.month, 1, tzinfo=dt.timezone.utc).timestamp()), int(dt.datetime(hi.year, hi.month, 1, tzinfo=dt.timezone.utc).timestamp())
        for attempt in range(5):
            try:
                copy_month(con, cdn_url(), a, b, path)
                break
            except duckdb.Error as ex:
                print(f"{lo:%Y-%m}: attempt {attempt} failed: {str(ex)[:150]}", flush=True)
                time.sleep(60 * (attempt + 1))
        else:
            raise RuntimeError(f"{lo:%Y-%m} failed")
        os.rename(f"{path}.tmp", path)
        n = con.sql(f"select count(*) from '{path}'").fetchall()[0][0]
        print(f"{lo:%Y-%m}: {n:,} rows, {os.path.getsize(path)/1e6:.0f} MB, {time.time()-t0:.0f}s", flush=True)


def copy_month(con, url, a, b, path):
    con.sql(f"""copy (
            select timestamp::uint32 ts, block_number::uint32 block, log_index, t.market_id,
                   maker, taker, price::float price, usd_amount::float usd,
                   maker_direction = 'BUY' maker_buy, nonusdc_side = 'token1' is_token1
            from read_parquet('{url}') t semi join u on t.market_id = u.market_id
            where timestamp >= {a} and timestamp < {b} and usd_amount >= {MIN_USD}
            order by ts, block, log_index
        ) to '{path}.tmp' (format parquet, compression zstd, row_group_size 500000)""")


if __name__ == "__main__":
    main(*sys.argv[1:])
