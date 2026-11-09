"""Aggregate every wallet-side trade into one row per (wallet, market): the building block for walk-forward scoring.

All quantities are in YES-equivalent terms (buying NO == selling YES), so PnL is unambiguous for mint/merge matches:
  cash  = sum(-yes_qty * yes_price)
  pnl   = cash + net_yes_qty * payout_yes          (held to resolution; exits before resolution are in cash)
  cost  = sum(capital spent per trade: q*p for the side actually bought)
  edge  = pnl / gross_qty   -> average (payout - entry price) per token, signed by trade direction
Wallets are split into hash buckets so each group-by fits in memory."""
import os, sys, time
import duckdb

DB = "data/polymarket.duckdb"
OUT = "data/processed/wallet_market"
BUCKETS = 16
# exchange contracts and the neg-risk adapter are not traders
NON_TRADERS = ("0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", "0xc5d563a36ae78145c45a50134d48a1215220f80a",
               "0xe111180000d2663c0091e4f400237545b87b996b", "0xe2222d279d744050d28e00520010520000310f59",
               "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296")

SIDES = """
  select maker wallet, market_id, ts, usd, price, is_token1, maker_buy is_buy, true is_maker from fills
  union all
  select taker, market_id, ts, usd, price, is_token1, not maker_buy, false from fills
  union all
  select wallet, market_id, ts, usd, price, outcome_idx = 0, side = 'BUY', null from api_trades
"""


def main():
    os.makedirs(OUT, exist_ok=True)
    con = duckdb.connect(DB, read_only=True, config={"memory_limit": "3GB", "threads": 4, "preserve_insertion_order": False,
                                                       "temp_directory": "data/tmp", "max_temp_directory_size": "6GiB"})
    for b in range(BUCKETS):
        path = f"{OUT}/b{b:02d}.parquet"
        if os.path.exists(path):
            continue
        t0 = time.time()
        con.sql(f"""copy (
          with s as (
            select wallet, market_id, ts, usd, is_maker,
                   usd / price * case when is_token1 = is_buy then 1 else -1 end yes_qty,
                   case when is_token1 then price else 1 - price end yes_price
            from ({SIDES}) where hash(wallet) % {BUCKETS} = {b} and wallet not in {NON_TRADERS}
          )
          select s.wallet, s.market_id, any_value(m.category) category, any_value(m.end_date) end_date,
                 any_value(m.winner_idx) winner_idx,
                 count(*)::uint32 n_trades, min(s.ts) first_ts, max(s.ts) last_ts,
                 sum(usd) usd, coalesce(sum(usd) filter (where is_maker), 0) maker_usd,
                 sum(case when yes_qty > 0 then yes_qty * yes_price else -yes_qty * (1 - yes_price) end) cost,
                 sum(greatest(yes_qty, 0)) pos_qty, sum(greatest(-yes_qty, 0)) neg_qty,
                 sum(-yes_qty * yes_price) cash,
                 sum(-yes_qty * yes_price) + sum(yes_qty) * any_value(case m.winner_idx when 0 then 1.0 when 1 then 0.0 else m.payout1 end) pnl
          from s join markets m using (market_id)
          group by 1, 2
        ) to '{path}.tmp' (format parquet, compression zstd)""")
        os.rename(f"{path}.tmp", path)
        n = con.sql(f"select count(*) from '{path}'").fetchall()[0][0]
        print(f"bucket {b}: {n:,} wallet-markets, {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
