"""Market universe: resolved binary markets with meaningful volume, excluding intraday crypto/bot markets."""
import duckdb

MARKETS = "data/raw/hf/markets.parquet"
MIN_VOLUME = 10_000
MIN_LIFE_H = 6
# intraday crypto (5m/15m/hourly/4h up-down) — signal windows of 1-24h make no sense there
INTRADAY_RE = r"(-updown-(5m|15m|30m|1h|4h)-|up-or-down-.*-[0-9]{1,2}(am|pm)-et$|-above-.*-[0-9]{1,2}(am|pm)-et$)"


def universe_sql(start="2025-10-01", end="2026-11-05"):
    return f"""
    select id as market_id, slug, event_id, event_slug, question, answer1, answer2, neg_risk, volume,
           created_at, end_date,
           try_cast(json_extract_string(replace(outcome_prices, '''', '"'), '$[0]') as double) as payout1,
           try_cast(json_extract_string(replace(outcome_prices, '''', '"'), '$[1]') as double) as payout2
    from '{MARKETS}'
    where closed = 1 and volume >= {MIN_VOLUME}
      and end_date >= '{start}' and end_date < '{end}'
      and epoch(end_date) - epoch(created_at) >= {MIN_LIFE_H} * 3600
      and not regexp_matches(slug, '{INTRADAY_RE}')
    """


if __name__ == "__main__":
    con = duckdb.connect()
    con.sql(f"create table u as {universe_sql()}")
    print(con.sql("select count(*) n, round(sum(volume)/1e9,2) vol_bn, count(*) filter (where payout1 is null) bad_payout from u"))
    print(con.sql("select payout1, count(*) n from u group by 1 order by n desc limit 6"))
