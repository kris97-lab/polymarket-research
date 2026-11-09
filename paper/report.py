"""Static HTML report of the paper-trading database (rewritten by the bot every couple of minutes)."""
import datetime as dt, html, json, time

STATUS = {"pending": "ждёт выставления", "placed": "ордер выставлен", "filled": "исполнен", "unfilled": "не исполнен",
          "moved": "цена убежала", "too_close": "слишком близко к старту", "stale": "найден поздно"}
TRADABLE = ("pending", "placed", "filled", "unfilled", "moved")
RULES = {"k2": ("2 кошелька (основное)", "+6.3%", "≈ 51/мес в среднем, с авг. ≈ 0–12/мес"), "k1": ("1 кошелёк (сравнение)", "+1.7%", "≈ 206/мес"),
         "k2t100": ("2 из топ-100 (сравнение)", "+1.9%", "≈ 108/мес")}


def fmt_t(t):
    return dt.datetime.fromtimestamp(t).strftime("%d.%m %H:%M") if t else ""


def money(x, sign=True):
    if x is None:
        return "—"
    return (f"{x:+,.0f}" if sign else f"{x:,.0f}").replace(",", " ") + " $"


def wallet_link(w, label=None):
    return f'<a href="https://polymarket.com/profile/{w}" target="_blank" rel="noopener">{html.escape(label or w[:6] + "…" + w[-4:])}</a>'


def curve_svg(points, color):
    """Cumulative PnL over resolution time as a small inline SVG polyline."""
    if len(points) < 2:
        return '<p class="muted">График появится после 2+ закрытых сделок.</p>'
    xs = [p[0] for p in points]
    ys, acc = [], 0.0
    for p in points:
        acc += p[1]
        ys.append(acc)
    lo, hi = min(0.0, *ys), max(0.0, *ys)
    w, h, pad = 640, 160, 8
    sx = lambda x: pad + (x - xs[0]) / max(1, xs[-1] - xs[0]) * (w - 2 * pad)
    sy = lambda y: h - pad - (y - lo) / max(1e-9, hi - lo) * (h - 2 * pad)
    pts = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in zip(xs, ys))
    return (f'<svg viewBox="0 0 {w} {h}" class="chart" role="img" aria-label="Накопленный PnL">'
            f'<line x1="{pad}" x2="{w - pad}" y1="{sy(0):.1f}" y2="{sy(0):.1f}" class="zero"/>'
            f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"/></svg>')


def write(db, path):
    now = int(time.time())
    st = {r[0]: r[1] for r in db.execute("select key, value from status")}
    all_rows = db.execute("""select s.*, m.question, m.event_slug, m.outcome0, m.outcome1 from signals s
                             left join markets m using (market_id) where s.status != 'history' order by s.t_sig desc""").fetchall()
    rows = [r for r in all_rows if r["strategy"] == "k2"]
    tradable = [r for r in rows if r["status"] in TRADABLE]
    filled = [r for r in rows if r["status"] == "filled"]
    closed = [r for r in filled if r["pnl"] is not None]
    taker = [r for r in rows if (r["taker_usd"] or 0) > 0]
    taker_closed = [r for r in taker if r["taker_pnl"] is not None]

    def block(closed_rows, stake_key, pnl_key, n_open):
        turnover = sum(r[stake_key] for r in closed_rows)
        pnl = sum(r[pnl_key] for r in closed_rows)
        wins = sum(1 for r in closed_rows if r["payout"] and r["payout"] > 0.5)
        return dict(n=len(closed_rows), open=n_open, turnover=turnover, pnl=pnl,
                    roi=(pnl / turnover if turnover else None), win=(wins / len(closed_rows) if closed_rows else None))

    p = block(closed, "stake", "pnl", len(filled) - len(closed))
    t = block(taker_closed, "taker_usd", "taker_pnl", len(taker) - len(taker_closed))
    hb = int(st.get("heartbeat", 0) or 0)
    alive = now - hb < 600
    month_now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m")
    wallet_month = st.get("wallet_month", "?")

    def kpis(b, title, note, bt):
        roi = "—" if b["roi"] is None else f"{b['roi'] * 100:+.1f}%"
        win = "—" if b["win"] is None else f"{b['win'] * 100:.0f}%"
        return f"""<section class="card"><h2>{title}</h2><p class="muted">{note}</p><div class="kpis">
          <div><span>Закрыто сделок</span><b>{b['n']}</b><small>ещё открыто: {b['open']}</small></div>
          <div><span>PnL</span><b class="{'neg' if b['pnl'] < 0 else ''}">{money(b['pnl'])}</b><small>оборот {money(b['turnover'], False)}</small></div>
          <div><span>ROI на оборот</span><b>{roi}</b><small>{bt}</small></div>
          <div><span>Выигрышных</span><b>{win}</b></div></div></section>"""

    cmp_rows = []
    for key, (label, bt_roi, bt_n) in RULES.items():
        started = int(st.get(f"rule_started_{key}") or st.get("first_started", now) or now)
        months = max((now - started) / (30 * 86400), 1 / 30)
        rr = [r for r in all_rows if r["strategy"] == key]
        f = [r for r in rr if r["status"] == "filled"]
        pb = block([r for r in f if r["pnl"] is not None], "stake", "pnl", 0)
        tk = [r for r in rr if (r["taker_usd"] or 0) > 0 and r["taker_pnl"] is not None]
        tb = block(tk, "taker_usd", "taker_pnl", 0)
        pct = lambda b: "—" if b["roi"] is None else f"{b['roi'] * 100:+.1f}%"
        cmp_rows.append(f"""<tr><td>{label}</td><td class="num">{len(rr)}</td><td class="num">{len(f)}<br><small class="muted">≈ {len(f) / months:.0f}/мес · бэктест {bt_n}</small></td>
          <td class="num">{pb['n']}</td><td class="num">{money(pb['pnl'])}</td><td class="num">{pct(pb)}<br><small class="muted">бэктест {bt_roi}</small></td>
          <td class="num">{tb['n']}</td><td class="num">{money(tb['pnl'])}</td><td class="num">{pct(tb)}</td></tr>""")

    trs = []
    for r in all_rows[:300]:
        out = r["outcome0"] if r["outcome"] == 0 else r["outcome1"]
        res = "" if r["payout"] is None else ("выиграл" if r["payout"] > 0.5 else "проиграл" if r["payout"] < 0.5 else "50/50")
        trs.append(f"""<tr><td>{fmt_t(r['t_sig'])}</td><td>{RULES[r['strategy']][0].split(' (')[0] if r['strategy'] in RULES else r['strategy']}</td><td>{html.escape(r['question'] or r['market_id'])}<br><small class="muted">{html.escape(r['event_slug'] or '')}</small></td>
          <td>{html.escape(out or '')}<br><small>{' '.join(wallet_link(w) for w in json.loads(r['wallets'] or '[]'))}</small></td>
          <td>{(r['game_start'] - r['t_sig']) / 3600:.1f} ч</td><td>{r['ref']:.3f}</td>
          <td>{STATUS.get(r['status'], r['status'])}{'<br><small class="muted">' + html.escape(r['note']) + '</small>' if r['note'] else ''}</td>
          <td class="num">{money(r['stake'], False) if r['status'] == 'filled' else ''}</td><td>{res}</td>
          <td class="num">{money(r['pnl']) if r['pnl'] is not None else ''}</td>
          <td class="num">{money(r['taker_pnl']) if r['taker_pnl'] is not None else (money(r['taker_usd'], False) + ' откр.' if (r['taker_usd'] or 0) > 0 and r['payout'] is None else '')}</td></tr>""")

    counts = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    warn = []
    if not alive:
        warn.append(f"Бот не отвечает с {fmt_t(hb)}: проверьте, что Mac не спит и бот запущен.")
    if wallet_month < month_now:
        warn.append(f"Список топ-50 кошельков за {wallet_month}, а сейчас {month_now}: нужно обновить список (см. README).")
    if st.get("last_error"):
        warn.append("Последняя ошибка: " + html.escape(st["last_error"]))

    doc = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="120"><title>Paper trading: киберспорт</title>
<style>
:root{{--bg:#fcfcfb;--card:#ffffff;--ink:#1f1f1e;--muted:#6b6a66;--line:#e6e5e1;--accent:#2a78d6;--accent2:#eb6834;--neg:#c2312f;--ok:#1a7f55;--warn:#a35a00}}
@media (prefers-color-scheme:dark){{:root{{--bg:#161615;--card:#1f1f1e;--ink:#ecebe7;--muted:#a19f99;--line:#33322f;--accent:#5b9be6;--accent2:#f08a5d;--neg:#ef6b69;--ok:#4cc38a;--warn:#e0a24a}}}}
body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
main{{max-width:1100px;margin:0 auto;padding:24px 16px 48px}}
h1{{font-size:22px;margin:0 0 4px}} h2{{font-size:16px;margin:0 0 4px}}
.muted{{color:var(--muted)}} small{{font-size:12px}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin:16px 0}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:12px}}
.kpis div{{display:flex;flex-direction:column}} .kpis span{{color:var(--muted);font-size:13px}} .kpis b{{font-size:22px;font-variant-numeric:tabular-nums}}
.neg{{color:var(--neg)}} .status{{display:inline-block;padding:2px 8px;border-radius:99px;font-size:13px;border:1px solid var(--line)}}
.status.ok{{color:var(--ok)}} .status.bad{{color:var(--neg)}}
.warn{{color:var(--warn);margin:4px 0}}
.grid2{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px}}
.chart{{width:100%;height:auto}} .chart .zero{{stroke:var(--line);stroke-width:1}}
.tablewrap{{overflow-x:auto}} table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}} th{{color:var(--muted);font-weight:500}}
td.num{{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}}
.legend span{{display:inline-flex;align-items:center;gap:6px;margin-right:16px;font-size:13px}} .legend i{{width:14px;height:3px;display:inline-block}}
</style></head><body><main>
<h1>Paper trading: киберспорт до матча</h1>
<p class="muted">Виртуальные сделки без денег. Основное правило: ≥2 кошелька из топ-50 вошли в один исход за 6 ч до матча → лимитка по их цене +1¢ на 5 минут, ставка до $100. Для сравнения параллельно ведётся правило «достаточно 1 кошелька».
Обновлено {fmt_t(now)} · <span class="status {'ok' if alive else 'bad'}">{'бот работает' if alive else 'бот не отвечает'}</span>
· список кошельков: {wallet_month} · открытых рынков: {st.get('markets_open', '?')}</p>
{''.join(f'<p class="warn">⚠ {w}</p>' for w in warn)}
<section class="card"><h2>Сигналы</h2><div class="kpis">
  <div><span>Всего сигналов</span><b>{len(rows)}</b><small>торгуемых: {len(tradable)}</small></div>
  <div><span>Исполнено (пассивно)</span><b>{len(filled)}</b><small>≈ {len(filled) / months:.0f} в месяц · бэктест: 50–100</small></div>
  <div><span>Цель для вывода</span><b>{len(closed)} / 200</b><small>закрытых сделок</small></div>
  <div><span>По статусам</span><small>{' · '.join(f"{STATUS.get(k, k)}: {v}" for k, v in sorted(counts.items(), key=lambda x: -x[1])) or '—'}</small></div>
</div></section>
<div class="grid2">{kpis(p, "Основное правило (2 кошелька): лимитка 5 мин", "Исполнение только если по нашей цене реально прошли сделки; ставка ≤ 50% этого объёма.", "бэктест: ≈ +6%")}
{kpis(t, "Основное правило: купить сразу", "Сразу по стакану до цены смартов +2¢, до $100. В бэктесте это проверить было нельзя.", "в бэктесте не проверялось")}</div>
<section class="card"><h2>Сравнение правил</h2><p class="muted">Исполнение и ставки одинаковые, разница только в том, сколько кошельков и из какого списка нужно для сигнала. Бэктест за 11 месяцев: 1 кошелёк даёт в 4 раза больше сделок при той же прибыли в долларах. «2 из топ-100» добавлено 8 ноября, потому что «2 из топ-50» с августа почти не срабатывает.</p>
<div class="tablewrap"><table><thead><tr><th>Правило</th><th>Сигналов</th><th>Исполнено</th><th>Закрыто</th><th>PnL</th><th>ROI на оборот</th><th>Купить сразу: закрыто</th><th>PnL</th><th>ROI</th></tr></thead>
<tbody>{''.join(cmp_rows)}</tbody></table></div></section>
<section class="card"><h2>Накопленный PnL (основное правило)</h2><p class="legend"><span><i style="background:var(--accent)"></i>лимитка 5 мин</span><span><i style="background:var(--accent2)"></i>купить сразу</span></p>
<div class="grid2"><div>{curve_svg(sorted((r['resolved_at'], r['pnl']) for r in closed), 'var(--accent)')}</div>
<div>{curve_svg(sorted((r['resolved_at'], r['taker_pnl']) for r in taker_closed), 'var(--accent2)')}</div></div></section>
<section class="card"><h2>Последние сигналы</h2><div class="tablewrap"><table>
<thead><tr><th>Сигнал</th><th>Правило</th><th>Рынок</th><th>Исход</th><th>До старта</th><th>Цена смартов</th><th>Статус</th><th>Ставка</th><th>Итог</th><th>PnL</th><th>Купить сразу</th></tr></thead>
<tbody>{''.join(trs) or '<tr><td colspan="11" class="muted">Сигналов пока нет.</td></tr>'}</tbody></table></div></section>
<section class="card"><h2>Топ-100 кошельков ({wallet_month})</h2><p class="muted">Ссылка открывает профиль на Polymarket. Адрес можно вставить и в Betmoar. Сделки за последние 7 дней: всего / в киберспорте до матча.</p>
<div class="tablewrap"><table><thead><tr><th>#</th><th>Кошелёк</th><th>Сделок за 7 дней</th><th>До матча</th><th>Последняя сделка</th></tr></thead><tbody>{''.join(
    f"<tr><td>{w['rank']}</td><td>{wallet_link(w['wallet'], w['wallet'])}</td><td class='num'>{w['n7']}</td><td class='num'>{w['pre7']}</td><td>{fmt_t(w['last'])}</td></tr>"
    for w in db.execute('''select w.rank, w.wallet,
          (select count(*) from wallet_trades t where t.wallet = w.wallet and t.ts > ?) n7,
          (select count(*) from wallet_trades t join markets m on m.condition_id = t.condition_id
             where t.wallet = w.wallet and t.ts > ? and t.ts < m.game_start) pre7,
          (select max(ts) from wallet_trades t where t.wallet = w.wallet) last
        from wallets w where w.month = ? order by w.rank''', (now - 7 * 86400, now - 7 * 86400, wallet_month)).fetchall())}</tbody></table></div></section>
</main></body></html>"""
    with open(path + ".tmp", "w") as f:
        f.write(doc)
    import os
    os.replace(path + ".tmp", path)
