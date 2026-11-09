"""Inspect remote parquet metadata (schema, row groups, timestamp ranges) without downloading data."""
import sys, datetime as dt
import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem

fs = HfFileSystem()
for name in sys.argv[1:]:
    path = f"datasets/SII-WANGZJ/Polymarket_data/{name}"
    md = pq.ParquetFile(fs.open(path)).metadata
    print(f"== {name}: rows={md.num_rows:,} row_groups={md.num_row_groups}")
    print(md.schema.to_arrow_schema())
    names = [md.schema.column(i).name for i in range(md.num_columns)]
    if "timestamp" not in names:
        continue
    ti = names.index("timestamp")
    by_month = {}
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        st = rg.column(ti).statistics
        lo = dt.datetime.utcfromtimestamp(st.min); hi = dt.datetime.utcfromtimestamp(st.max)
        k = lo.strftime("%Y-%m")
        r, b = by_month.get(k, (0, 0))
        by_month[k] = (r + rg.num_rows, b + rg.total_byte_size)
        if g < 2 or g == md.num_row_groups - 1:
            print(f"  rg{g}: {lo} .. {hi} rows={rg.num_rows:,}")
    print("  month  rows  uncompressed_GB")
    for k, (r, b) in sorted(by_month.items()):
        print(f"  {k} {r:>13,} {b/1e9:7.2f}")
    # per-column compressed size share (first row group)
    rg = md.row_group(md.num_row_groups // 2)
    tot = sum(rg.column(i).total_compressed_size for i in range(md.num_columns))
    print("  col share (compressed):", {names[i]: round(rg.column(i).total_compressed_size / tot, 3) for i in range(md.num_columns)})
