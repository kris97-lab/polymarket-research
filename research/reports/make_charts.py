"""Figures for reports/summary.md (static PNG, light theme, reference data-viz palette)."""
import datetime as dt, os, sys
import numpy as np
import polars as pl
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

sys.path.insert(0, "backtest")
from sim import run_period, TRAIN, TEST, CFG, EXEC, epoch

OUT = "reports/fig"
os.makedirs(OUT, exist_ok=True)
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"
BLUE, ORANGE, AQUA, GRAY, RED = "#2a78d6", "#eb6834", "#1baf7a", "#8a8984", "#e34948"
plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                     "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
                     "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
                     "axes.spines.top": False, "axes.spines.right": False, "font.size": 10, "axes.titlesize": 11,
                     "axes.titleweight": "bold", "legend.frameon": False})


def style_ax(ax, bars=False):
    ax.set_axisbelow(True)
    ax.tick_params(length=0)
    if bars:
        ax.grid(axis="x", visible=False)


def headroom(ax, values, frac=0.18):
    lo, hi = min(0, min(values)), max(0, max(values))
    pad = (hi - lo) * frac
    ax.set_ylim(lo - (pad if lo < 0 else 0), hi + pad)


sig = pl.read_parquet("data/processed/backtest/signals.parquet")
ex = (pl.read_parquet("data/processed/backtest/executions/*.parquet").with_columns(pl.col("moved").cast(pl.Boolean))
      .unique(subset=["market_id", "outcome", "t_sig", "seed"] + EXEC, keep="first"))


def curve(cfg, exec_, period, seed=-1):
    s = sig.filter(*[pl.col(k) == v for k, v in cfg.items()])
    e = ex.filter(*[pl.col(k) == v for k, v in exec_.items()])
    r = [x for x in run_period(s, e, period, seeds=(seed,), curve=True) if x["seed"] == seed]
    if not r:
        return None
    t0, eq = r[0]["curve"]
    days = [dt.datetime.fromtimestamp(t0, dt.timezone.utc) + dt.timedelta(days=i) for i in range(len(eq))]
    return days, eq, r[0]


# ---------- 1. equity curves: train | test ----------
MID = dict(D=60, offset=0.01, TTL=3600)
series = [
    ("Консенсус, все категории (N50 K2 W6ч)", "все", dict(scope="all", N=50, K=2, W_h=6), MID, BLUE, "-"),
    ("Консенсус, politics (N50 K2 W6ч)", "politics", dict(scope="politics", N=50, K=2, W_h=6), MID, AQUA, "-"),
    ("Лучшая на train: crypto (N50 K2 W1ч, TTL 5м)", "crypto", dict(scope="crypto", N=50, K=2, W_h=1), dict(D=60, offset=0.0, TTL=300), ORANGE, "-"),
]
fig, axes = plt.subplots(1, 2, figsize=(13, 5.2), sharey=True)
for ax, (pname, period) in zip(axes, (("Train: окт 2025 – апр 2026", TRAIN), ("Test (out-of-sample): май – сен 2026", TEST))):
    style_ax(ax)
    ends = []
    for label, short, cfg, exec_, color, ls in series:
        c = curve(cfg, exec_, period)
        if c is None:
            continue
        days, eq, m = c
        ax.plot(days, eq / 1e3, color=color, lw=2, ls=ls, label=label)
        ends.append((eq[-1] / 1e3, f"  {short} {m['roi']:+.0%}", color, days[-1]))
    # random baseline: mean of 3 seeds for the 'all' mid config
    rc = [curve(dict(scope="all", N=50, K=2, W_h=6), MID, period, seed=s) for s in range(3)]
    rc = [c for c in rc if c]
    if rc:
        n = min(len(c[1]) for c in rc)
        eq = np.mean([c[1][:n] for c in rc], axis=0)
        ax.plot(rc[0][0][:n], eq / 1e3, color=GRAY, lw=2, ls="--", label="Случайный вход (тот же конфиг, 3 сида)")
        ends.append((eq[-1] / 1e3, f"  случайный {eq[-1] / 1e4 - 1:+.0%}", GRAY, rc[0][0][n - 1]))
    ax.axhline(10, color=INK2, lw=0.8)
    ends.sort(key=lambda e: e[0])
    placed = []
    for y, txt, color, x in ends:   # direct labels at line ends, nudged apart so they never collide
        ly = max(y, placed[-1] + 1.4) if placed else y
        placed.append(ly)
        ax.annotate(txt, (x, y), xytext=(x, ly), textcoords="data", va="center", fontsize=9, color=INK,
                    xycoords="data", annotation_clip=False)
        ax.plot([x], [y], "o", color=color, ms=6, mec=SURFACE, mew=2)
    ax.set_title(pname, loc="left")
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
axes[0].set_ylabel("Equity, тыс. $ (старт 10)")
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="lower center", ncol=2, fontsize=9, bbox_to_anchor=(0.5, 0.0))
fig.suptitle("Equity curve стратегии консенсуса (D=60с, оффсет +1¢, TTL 1ч, комиссия 1%; PnL фиксируется при резолве)",
             x=0.01, ha="left", fontsize=11, color=INK)
fig.tight_layout(rect=(0, 0.1, 0.97, 1), w_pad=6)
fig.savefig(f"{OUT}/equity_curves.png", dpi=150)
plt.close(fig)

# ---------- 2. signal edge: ideal vs filled vs unfilled ----------
t = pl.read_csv("reports/stage3_signal_edge.csv").filter(pl.col("period") == "test")
groups = ["Идеальный вход\nпо цене смарта", "Исполненные\nлимитки", "Неисполненные\nлимитки"]
vals = {k: [r["edge_ideal_c"] * 100, r["edge_filled_c"] * 100, r["edge_unfilled_c"] * 100]
        for k, r in ((row["kind"], row) for row in t.iter_rows(named=True))}
fig, ax = plt.subplots(figsize=(8, 4.2))
style_ax(ax, bars=True)
headroom(ax, vals["signal"] + vals["random"])
x = np.arange(len(groups))
w = 0.36
for i, (kind, label, color) in enumerate((("signal", "Сигнал консенсуса", BLUE), ("random", "Случайный вход", GRAY))):
    bars = ax.bar(x + (i - 0.5) * (w + 0.02), vals[kind], w, color=color, label=label, edgecolor=SURFACE, linewidth=2)
    for b, v in zip(bars, vals[kind]):
        ax.annotate(f"{v:+.1f}¢", (b.get_x() + b.get_width() / 2, v), xytext=(0, 3 if v >= 0 else -12),
                    textcoords="offset points", ha="center", fontsize=9, color=INK)
ax.axhline(0, color=INK2, lw=0.8)
ax.set_xticks(x, groups)
ax.set_ylabel("Edge, центов на токен (итог − цена)")
ax.set_title("Edge сигнала есть, исполненные ордера его теряют\n(test, D=60с, оффсет +1¢, TTL 1ч)", loc="left")
ax.legend(loc="upper left")
fig.tight_layout()
fig.savefig(f"{OUT}/signal_edge.png", dpi=150)
plt.close(fig)

# ---------- 3. adverse selection by printed volume (weather, test) ----------
s = sig.filter(pl.col("scope") == "weather", pl.col("N") == 50, pl.col("K") == 2, pl.col("W_h") == 6,
               pl.col("t_sig") >= epoch(TEST[0]), pl.col("t_sig") < epoch(TEST[1]))
f = (s.select("market_id", "outcome", "t_sig").join(ex.filter(pl.col("seed") == -1, *[pl.col(k) == v for k, v in MID.items()]),
                                                    on=["market_id", "outcome", "t_sig"])
     .filter(pl.col("fill_vol") > 0, ~pl.col("moved")).with_columns((pl.col("payout_o") / pl.col("limit") - 1).alias("ret")))
q = np.quantile(f["fill_vol"].to_numpy(), [0.2, 0.4, 0.6, 0.8])
f = f.with_columns(pl.col("fill_vol").map_elements(lambda v: int(np.searchsorted(q, v)), return_dtype=pl.Int64).alias("qb"))
b = f.group_by("qb").agg(pl.col("ret").mean(), pl.col("fill_vol").median().alias("vol"), (pl.col("payout_o") > 0.5).mean().alias("win")).sort("qb")
fig, ax = plt.subplots(figsize=(8, 4.4))
style_ax(ax, bars=True)
rets = b["ret"].to_numpy() * 100
headroom(ax, list(rets), frac=0.22)
bars = ax.bar(range(len(b)), rets, 0.62, color=[BLUE if r >= 0 else RED for r in rets], edgecolor=SURFACE, linewidth=2)
for i, (r, wr) in enumerate(zip(rets, b["win"].to_numpy())):
    ax.annotate(f"{r:+.0f}%\nwin {wr:.0%}", (i, r), xytext=(0, 4 if r >= 0 else -26), textcoords="offset points",
                ha="center", fontsize=9, color=INK)
ax.axhline(0, color=INK2, lw=0.8)
ax.set_xticks(range(len(b)), [f"Q{i + 1}\n~${v:,.0f}" for i, v in enumerate(b["vol"].to_numpy())])
ax.set_xlabel("Квинтиль объёма, прошедшего по цене ≤ нашего лимита (медиана)")
ax.set_ylabel("Доходность на $1, %")
ax.set_title("Adverse selection по объёму: крупно исполняемся,\nкогда смарты ошиблись (weather, test)", loc="left")
fig.tight_layout()
fig.savefig(f"{OUT}/adverse_selection_volume.png", dpi=150)
plt.close(fig)

# ---------- 4. stage 2: out-of-sample skill persistence ----------
p = pl.read_parquet("data/processed/scores/persistence.parquet").filter(pl.col("scope") == "all")
order = [("top010", "Топ-10"), ("top025", "Топ-25"), ("top050", "Топ-50"), ("top100", "Топ-100"),
         ("rest_rankable", "Остальные\nквалиф."), ("market_makers", "Маркет-\nмейкеры"), ("hedge_or_losing", "Хедж /\nубыточные")]
rv = [p.filter(pl.col("grp") == g)["roi"][0] * 100 for g, _ in order]
fig, ax = plt.subplots(figsize=(8, 4.2))
style_ax(ax, bars=True)
headroom(ax, rv)
bars = ax.bar(range(len(order)), rv, 0.62, color=[BLUE if v >= 0 else RED for v in rv], edgecolor=SURFACE, linewidth=2)
for i, v in enumerate(rv):
    ax.annotate(f"{v:+.1f}%", (i, v), xytext=(0, 3 if v >= 0 else -12), textcoords="offset points", ha="center", fontsize=9)
ax.axhline(0, color=INK2, lw=0.8)
ax.set_xticks(range(len(order)), [l for _, l in order])
ax.set_ylabel("ROI в следующем месяце, %")
ax.set_title("Скилл кошельков устойчив вне выборки\n(ROI их собственных сделок в следующем месяце, не копирование)", loc="left")
fig.tight_layout()
fig.savefig(f"{OUT}/skill_persistence.png", dpi=150)
plt.close(fig)

# ---------- 5. grid: train vs test ROI ----------
g = pl.read_parquet("data/processed/backtest/grid_fee100.parquet")
key = CFG + EXEC
w = (g.filter(pl.col("period") == "train").join(g.filter(pl.col("period") == "test"), on=key, suffix="_test")
     .filter(pl.col("trades") >= 30, pl.col("trades_test") >= 10, pl.col("N") > 1))
fig, ax = plt.subplots(figsize=(6.4, 5.4))
style_ax(ax)
xr, yr = np.clip(w["roi"].to_numpy() * 100, -100, 250), np.clip(w["roi_test"].to_numpy() * 100, -100, 100)
ax.scatter(xr, yr, s=10, color=BLUE, alpha=0.35, linewidths=0)
ax.axhline(0, color=INK2, lw=0.8)
ax.axvline(0, color=INK2, lw=0.8)
share = float((w["roi_test"] > 0).mean())
ax.annotate(f"{w.height:,} конфигураций\nв плюсе на test: {share:.0%}", (0.98, 0.04), xycoords="axes fraction", ha="right",
            fontsize=9, color=INK)
ax.set_xlabel("ROI на train, % (обрезано до 250)")
ax.set_ylabel("ROI на test, % (обрезано до ±100)")
ax.set_title("Train-результат почти не предсказывает test", loc="left")
fig.tight_layout()
fig.savefig(f"{OUT}/train_vs_test.png", dpi=150)
plt.close(fig)
print("figures:", sorted(os.listdir(OUT)))
