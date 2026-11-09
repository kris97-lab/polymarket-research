"""Paper trading (no real orders, no wallet, no keys): esports pre-match consensus of top esports wallets.
Rules: k2 = 2 of the top 50 (main), k1 = 1 of the top 50, k2t100 = 2 of the top 100 (added 2026-11-08).

Rule frozen from the backtest (research/reports/esports.md):
 - signal: >= 2 distinct top-50 esports wallets ENTER the same outcome of a market within 6 h, before the match starts
   (entry = a trade that grows the wallet's exposure toward that outcome, >= $10);
 - virtual limit buy at (last smart entry price + 1c), placed 60 s after the signal, alive 5 min,
   and the whole order must expire before the match starts (Polymarket clears esports books at the start);
 - skipped if the last print is already above ref + 5c;
 - filled only if the public tape printed at <= our limit within those 5 min; stake = min($100, 50% of that volume);
 - fee 1%; held to resolution.
Also records the order book at placement: what an immediate (taker) buy of up to $100 at <= ref + 2c would have got.
Public APIs only: Gamma (markets), Data API v2 (trades), CLOB /book (order book). Stdlib only."""
import json, os, sqlite3, sys, time, traceback, urllib.error, urllib.parse, urllib.request
import datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import report
GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com/v2"
CLOB = "https://clob.polymarket.com"

STRATEGIES = {"k2": (2, 50), "k1": (1, 50), "k2t100": (2, 100)}   # rule -> (min distinct wallets, list size); "k2" is the main rule
TOP_N = max(n for _, n in STRATEGIES.values())                       # wallets polled
W_S = 6 * 3600
DELAY_S, TTL_S, OFFSET, MAX_MOVE, TAKER_SLIP = 60, 300, 0.01, 0.05, 0.02
MIN_ENTRY_USD, MIN_REF, MAX_REF, HORIZON_S = 10.0, 0.02, 0.95, 30 * 86400
STAKE_MAX, LIQ_SHARE, MIN_STAKE, FEE = 100.0, 0.5, 5.0, 0.01
STALE_S = 15 * 60                 # signals found later than this (restart, API lag) are recorded but not traded
TAPE_LAG_S = 120                  # wait for the public tape to catch up before judging a fill
LOOP_S, MARKETS_EVERY, RESOLVE_EVERY, REPORT_EVERY = 60, 600, 600, 120
BACKFILL_S = 3 * 86400


def log(*a):
    print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def get(url, tries=5):
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "paper-research"})
            return json.load(urllib.request.urlopen(req, timeout=30))
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504):
                log("HTTP", e.code, url[:120])
                return None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ConnectionError, OSError):
            pass
        time.sleep(min(30, 2 ** k))
    log("giving up", url[:120])
    return None


def ts(s):
    if not s:
        return None
    s = s.replace(" ", "T")
    s = s[:-3] + "+00:00" if s.endswith("+00") else s.replace("Z", "+00:00")
    try:
        return int(dt.datetime.fromisoformat(s).timestamp())
    except ValueError:
        return None


def connect():
    db = sqlite3.connect(os.path.join(HERE, "paper.db"), isolation_level=None, timeout=30)
    db.row_factory = sqlite3.Row
    cols = [r[1] for r in db.execute("pragma table_info(signals)")]
    if cols and "strategy" not in cols:          # pre-strategy schema: keep its rows as the main rule
        db.execute("alter table signals rename to signals_old")
    db.executescript("""
    pragma journal_mode = wal;
    create table if not exists markets (market_id text primary key, condition_id text, question text, event_slug text,
        outcome0 text, outcome1 text, token0 text, token1 text, game_start integer, end_ts integer, market_type text);
    create index if not exists markets_cond on markets(condition_id);
    create table if not exists wallet_trades (tx_hash text, wallet text, condition_id text, token_id text, side text,
        size real, price real, ts integer, primary key (tx_hash, wallet, token_id, side, size, price));
    create index if not exists wt_cond on wallet_trades(condition_id, wallet);
    create table if not exists wallets (month text, rank integer, wallet text, primary key (month, wallet));
    create table if not exists signals (strategy text, market_id text, condition_id text, outcome integer, token text,
        t_sig integer, ref real, wallets text, detected_at integer, game_start integer, status text, note text,
        t_place integer, last_price real, limit_price real,
        fill_vol real, fill_ts integer, stake real,
        taker_usd real, taker_shares real, book_json text,
        payout real, resolved_at integer, pnl real, taker_pnl real, primary key (strategy, market_id));
    create table if not exists status (key text primary key, value text);
    """)
    if db.execute("select 1 from sqlite_master where name = 'signals_old'").fetchone():
        db.execute("insert into signals select 'k2', * from signals_old")
        db.execute("drop table signals_old")
    return db


def set_status(db, key, value):
    db.execute("insert or replace into status values (?, ?)", (key, str(value)))


def load_wallets(db):
    for f in sorted(os.listdir(os.path.join(HERE, "wallets"))):
        if f.endswith(".csv"):
            month = f[:7]
            with open(os.path.join(HERE, "wallets", f)) as fh:
                rows = [l.strip().split(",") for l in fh.readlines()[1:] if l.strip()]
            db.executemany("insert or ignore into wallets values (?, ?, ?)", [(month, int(r[0]), r[1].lower()) for r in rows])


def wallets_for(db, t, n=50):
    """Top-n list of the month of t (or the latest earlier one if that month's list is missing)."""
    month = dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m")
    m = db.execute("select max(month) from wallets where month <= ?", (month,)).fetchone()[0]
    return m, {r[0] for r in db.execute("select wallet from wallets where month = ? and rank <= ?", (m, n))}


def refresh_markets(db):
    n, off = 0, 0
    while off < 10_000:
        ev = get(f"{GAMMA}/events?tag_slug=esports&closed=false&limit=100&offset={off}")
        if not ev:
            break
        for e in ev:
            for m in e.get("markets") or []:
                gs = ts(m.get("gameStartTime"))
                try:
                    toks, outs = json.loads(m.get("clobTokenIds") or "[]"), json.loads(m.get("outcomes") or "[]")
                except json.JSONDecodeError:
                    continue
                if not gs or not m.get("conditionId") or len(toks) != 2:
                    continue
                db.execute("insert or replace into markets values (?,?,?,?,?,?,?,?,?,?,?)",
                           (str(m["id"]), m["conditionId"], m.get("question"), e.get("slug"), outs[0] if outs else "",
                            outs[1] if len(outs) > 1 else "", toks[0], toks[1], gs, ts(m.get("endDate")), m.get("sportsMarketType")))
                n += 1
        off += 100
    set_status(db, "markets_refreshed", int(time.time()))
    set_status(db, "markets_open", n)


def poll_wallet(db, w, since, limit=100, max_pages=3):
    cursor = None
    for _ in range(max_pages):
        q = {"user": w, "takerOnly": "false", "limit": limit}
        if cursor:
            q["cursor"] = cursor
        d = get(f"{DATA}/trades?{urllib.parse.urlencode(q)}")
        if not d:
            return False
        page, new = d.get("data") or [], 0
        for r in page:
            new += db.execute("insert or ignore into wallet_trades values (?,?,?,?,?,?,?,?)",
                              (r.get("transaction_hash"), w, r.get("condition_id"), str(r.get("token_id")), r.get("side"),
                               float(r.get("size") or 0), float(r.get("price") or 0), int(r.get("timestamp") or 0))).rowcount
        pag = d.get("pagination") or {}
        if not page or page[-1].get("timestamp", 0) < since or new == 0 or not pag.get("has_more"):
            return True
        cursor = pag.get("next_cursor")
    return True


def entries(db, m, wallet):
    """Replay the wallet's trades in this market -> entries (ts, outcome, price of that outcome, usd)."""
    pos, out = 0.0, []
    for r in db.execute("select token_id, side, size, price, ts from wallet_trades where condition_id = ? and wallet = ? "
                        "order by ts, rowid", (m["condition_id"], wallet)):
        i = 0 if r["token_id"] == m["token0"] else 1 if r["token_id"] == m["token1"] else None
        if i is None:
            continue
        q = r["size"] if (i == 0) == (r["side"] == "BUY") else -r["size"]      # signed qty of outcome 0
        pos += q
        if (q > 0 and pos > 0) or (q < 0 and pos < 0):
            o = 0 if q > 0 else 1
            p_o = r["price"] if (i == o) else 1 - r["price"]
            usd = r["size"] * r["price"]
            if usd >= MIN_ENTRY_USD:
                out.append((r["ts"], o, p_o, wallet))
    return out


def detect(db, now):
    st = dict(db.execute("select key, value from status").fetchall())
    for name, (k, n) in STRATEGIES.items():
        month, top = wallets_for(db, now, n)
        if top:      # signals from before the rule was added count as history, not as missed trades
            detect_rule(db, now, name, k, top, int(st.get(f"rule_started_{name}") or st["first_started"]))


def detect_rule(db, now, strategy, K, top, first_started):
    marks = ",".join("?" * len(top))
    cands = db.execute(f"""select m.* from markets m where m.game_start > ?
        and not exists (select 1 from signals s where s.strategy = ? and s.market_id = m.market_id)
        and (select count(distinct wallet) from wallet_trades t where t.condition_id = m.condition_id and t.wallet in ({marks})) >= {K}""",
                       (now, strategy, *top)).fetchall()
    for m in cands:
        es = sorted(e for w in top for e in entries(db, m, w) if e[0] < m["game_start"])
        best = None
        for o in (0, 1):
            eo = [e for e in es if e[1] == o]
            for j, e in enumerate(eo):
                ws = {x[3] for x in eo[:j + 1] if x[0] > e[0] - W_S}
                if len(ws) >= K and MIN_REF <= e[2] <= MAX_REF and not (m["end_ts"] and m["end_ts"] - e[0] > HORIZON_S):
                    if best is None or e[0] < best[0]:
                        best = (e[0], o, e[2], sorted(ws))
                    break
        if best is None:
            continue
        t_sig, o, ref, ws = best
        t_place = max(t_sig + DELAY_S, now)
        status, note = "pending", ""
        if t_sig < first_started:
            status, note = "history", "до запуска бота"
        elif now - t_sig > STALE_S:
            status, note = "stale", f"найден через {(now - t_sig) // 60} мин"
        elif t_place + TTL_S > m["game_start"]:
            status, note = "too_close", "ордер не успевает до старта матча"
        db.execute("""insert into signals (strategy, market_id, condition_id, outcome, token, t_sig, ref, wallets, detected_at,
                      game_start, status, note, t_place, limit_price) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                   (strategy, m["market_id"], m["condition_id"], o, m["token0"] if o == 0 else m["token1"], t_sig, ref, json.dumps(ws),
                    now, m["game_start"], status, note, t_place, min(ref + OFFSET, 0.99)))
        log("signal", strategy, status, m["question"], "->", m["outcome0"] if o == 0 else m["outcome1"], f"ref={ref:.3f}", note)


def tape(cond, token, since):
    """Public taker prints of a market since `since`, as (ts, price of `token`, usd)."""
    out, cursor = [], None
    for _ in range(20):
        q = {"condition": cond, "takerOnly": "true", "limit": 500}
        if cursor:
            q["cursor"] = cursor
        d = get(f"{DATA}/trades?{urllib.parse.urlencode(q)}")
        if d is None:
            return None
        page = d.get("data") or []
        for r in page:
            t, p, size = int(r.get("timestamp") or 0), float(r.get("price") or 0), float(r.get("size") or 0)
            if t >= since:
                out.append((t, p if str(r.get("token_id")) == token else 1 - p, size * p))
        pag = d.get("pagination") or {}
        if not page or page[-1].get("timestamp", 0) < since or not pag.get("has_more"):
            break
        cursor = pag.get("next_cursor")
    return sorted(out)


def place(db, now):
    for s in db.execute("select * from signals where status = 'pending' and t_place <= ?", (now,)).fetchall():
        tp = tape(s["condition_id"], s["token"], s["t_sig"])
        if tp is None:
            continue
        before = [x for x in tp if x[0] <= now]
        last = before[-1][1] if before else s["ref"]
        if last > s["ref"] + MAX_MOVE:
            db.execute("update signals set status = 'moved', last_price = ?, t_place = ?, note = ? where strategy = ? and market_id = ?",
                       (last, now, "цена уже ушла выше ref + 5¢", s["strategy"], s["market_id"]))
            continue
        book = get(f"{CLOB}/book?token_id={s['token']}")
        asks = sorted(((float(a["price"]), float(a["size"])) for a in (book or {}).get("asks") or []))
        cap, usd, shares = s["ref"] + TAKER_SLIP, 0.0, 0.0
        for p, size in asks:
            if p > cap + 1e-9 or usd >= STAKE_MAX:
                break
            take = min(size, (STAKE_MAX - usd) / p)
            usd, shares = usd + take * p, shares + take
        db.execute("""update signals set status = 'placed', t_place = ?, last_price = ?, taker_usd = ?, taker_shares = ?,
                      book_json = ? where strategy = ? and market_id = ?""",
                   (now, last, usd if usd >= MIN_STAKE else 0.0, shares if usd >= MIN_STAKE else 0.0,
                    json.dumps({"asks": asks[:10], "bids": sorted(((float(b["price"]), float(b["size"])) for b in (book or {}).get("bids") or []), reverse=True)[:10]}),
                    s["strategy"], s["market_id"]))


def evaluate(db, now):
    for s in db.execute("select * from signals where status = 'placed' and t_place + ? <= ?", (TTL_S + TAPE_LAG_S, now)).fetchall():
        tp = tape(s["condition_id"], s["token"], s["t_place"])
        if tp is None:
            continue
        hits = [x for x in tp if s["t_place"] < x[0] <= s["t_place"] + TTL_S and x[1] <= s["limit_price"] + 1e-9]
        vol = sum(x[2] for x in hits)
        stake = min(STAKE_MAX, LIQ_SHARE * vol)
        filled = stake >= MIN_STAKE
        db.execute("update signals set status = ?, fill_vol = ?, fill_ts = ?, stake = ? where strategy = ? and market_id = ?",
                   ("filled" if filled else "unfilled", vol, hits[0][0] if hits else None, stake if filled else 0.0, s["strategy"], s["market_id"]))


def resolve(db, now):
    seen = {}
    for s in db.execute("""select * from signals where payout is null and game_start + 3600 < ?
                           and (status = 'filled' or coalesce(taker_usd, 0) > 0)""", (now,)).fetchall():
        if s["market_id"] not in seen:
            seen[s["market_id"]] = get(f"{GAMMA}/markets?id={s['market_id']}&closed=true") or get(f"{GAMMA}/markets?id={s['market_id']}") or []
        ms = seen[s["market_id"]]
        if not ms or not ms[0].get("closed"):
            continue
        try:
            prices = [float(x) for x in json.loads(ms[0].get("outcomePrices") or "[]")]
        except (json.JSONDecodeError, ValueError):
            continue
        if len(prices) != 2 or not (max(prices) >= 0.99 or prices == [0.5, 0.5]):
            continue
        pay = prices[s["outcome"]]
        pnl = (s["stake"] or 0) * (pay / s["limit_price"] - 1 - FEE) if s["status"] == "filled" else None
        tpnl = (s["taker_shares"] * pay - s["taker_usd"] * (1 + FEE)) if (s["taker_usd"] or 0) > 0 else None
        db.execute("update signals set payout = ?, resolved_at = ?, pnl = ?, taker_pnl = ? where strategy = ? and market_id = ?",
                   (pay, now, pnl, tpnl, s["strategy"], s["market_id"]))
        log("resolved", s["strategy"], s["market_id"], "payout", pay, "pnl", pnl, "taker", tpnl)


def step(db, name, fn, *a):
    try:
        fn(db, *a)
    except Exception:
        log("ERROR in", name, traceback.format_exc())
        set_status(db, "last_error", f"{dt.datetime.now():%Y-%m-%d %H:%M} {name}: {traceback.format_exc().splitlines()[-1]}")


def main():
    db = connect()
    load_wallets(db)
    now = int(time.time())
    month, top = wallets_for(db, now, TOP_N)
    log("start; wallet list", month, len(top), "wallets")
    set_status(db, "started", now)
    db.execute("insert or ignore into status values ('first_started', ?)", (str(now),))
    first = db.execute("select value from status where key = 'first_started'").fetchone()[0]
    for name in STRATEGIES:                # k2 and k1 ran from the first start, later rules from now
        db.execute("insert or ignore into status values (?, ?)", (f"rule_started_{name}", first if name in ("k2", "k1") else str(now)))
    step(db, "markets", lambda d: refresh_markets(d))
    for w in sorted(top):                  # backfill positions so that entries are recognised correctly
        step(db, "backfill", lambda d: poll_wallet(d, w, now - BACKFILL_S, limit=500, max_pages=10))
    last_mk, last_res, last_rep = time.time(), 0.0, 0.0
    while True:
        t0 = time.time()
        if t0 - last_mk >= MARKETS_EVERY:
            step(db, "markets", lambda d: refresh_markets(d))
            last_mk = t0
        month, top = wallets_for(db, int(t0), TOP_N)
        load_wallets(db)
        for w in sorted(top):
            step(db, "poll", lambda d: poll_wallet(d, w, int(t0) - 3600))
        now = int(time.time())
        step(db, "detect", detect, now)
        step(db, "place", place, now)
        step(db, "evaluate", evaluate, now)
        if t0 - last_res >= RESOLVE_EVERY:
            step(db, "resolve", resolve, now)
            last_res = t0
        set_status(db, "heartbeat", now)
        set_status(db, "wallet_month", month)
        if t0 - last_rep >= REPORT_EVERY:
            step(db, "report", lambda d: report.write(d, os.path.join(HERE, "report.html")))
            last_rep = t0
        time.sleep(max(1, LOOP_S - (time.time() - t0)))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        report.write(connect(), os.path.join(HERE, "report.html"))
    else:
        main()
