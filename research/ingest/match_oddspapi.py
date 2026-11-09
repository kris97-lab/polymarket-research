"""Map OddsPapi esports fixtures (Pinnacle) to Polymarket series-winner markets by team names and start time.
Output: data/processed/oddspapi_map.parquet (fixtureId, market_id, swapped = OddsPapi participant1 is Polymarket answer2)."""
import re, sys
import duckdb, polars as pl
sys.path.insert(0, "ingest")
import oddspapi as op

STOP = {"team", "esports", "esport", "gaming", "club", "gg", "the", "e", "sports"}
PREFIX = {17: "Counter-Strike:", 16: "Dota 2:", 18: "LoL:"}


def norm(s):
    words = [w for w in re.sub(r"[^a-z0-9 ]", " ", (s or "").lower()).split() if w not in STOP]
    return "".join(words)


def same(a, b):
    a, b = norm(a), norm(b)
    return bool(a) and bool(b) and (a == b or (min(len(a), len(b)) >= 3 and (a in b or b in a)))


pm = (duckdb.connect("data/polymarket.duckdb", read_only=True).sql(
    """select market_id, question, answer1, answer2 from markets
       where source = 'hf' and category = 'esports' and question like '%(BO%' and question like '% vs %'""").pl()
      .join(pl.read_parquet("data/processed/esports_game_start.parquet").select("market_id", "game_start_ts").drop_nulls(), on="market_id"))
fx = [f for f in op.load_fixtures() if (f.get("externalProviders") or {}).get("pinnacleId")]
by_prefix = {k: pm.filter(pl.col("question").str.starts_with(v)) for k, v in PREFIX.items()}
rows = []
for f in fx:
    t = int(pl.Series([f["startTime"]]).str.to_datetime(time_zone="UTC").dt.epoch("s")[0])
    c = by_prefix[f["sportId"]].filter((pl.col("game_start_ts") - t).abs() <= 4 * 3600)
    for m in c.iter_rows(named=True):
        if same(f["participant1Name"], m["answer1"]) and same(f["participant2Name"], m["answer2"]):
            rows.append((f["fixtureId"], m["market_id"], False, f["sportId"], t, m["game_start_ts"]))
            break
        if same(f["participant1Name"], m["answer2"]) and same(f["participant2Name"], m["answer1"]):
            rows.append((f["fixtureId"], m["market_id"], True, f["sportId"], t, m["game_start_ts"]))
            break
out = pl.DataFrame(rows, schema=["fixtureId", "market_id", "swapped", "sportId", "start_ts", "game_start_ts"], orient="row").unique("market_id")
out.write_parquet("data/processed/oddspapi_map.parquet")
print("pinnacle fixtures", len(fx), "| polymarket series markets", pm.height, "| matched", out.height)
print(out.group_by("sportId").len())
