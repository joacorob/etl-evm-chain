#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
timesfm_volseries — TimesFM forecasteando la SERIE DE VOLATILIDAD (uso canónico)
================================================================================

Antes le dimos a TimesFM la serie de PRECIO (≈ random walk, poco que extraer). Acá
hacemos lo correcto para un modelo de series: construimos una SERIE DE VOL REALIZADA
(rolling 24h, por hora) — que tiene clustering/memoria fuerte — y la forecasteamos
H pasos hacia adelante. De ahí sacamos la sigma esperada de la ventana.

Comparamos, sobre LA MISMA serie de vol:
  - TimesFM (forecast del path de log-RV, promedio del horizonte)
  - naive   (último valor de RV)
  - EWMA    (sobre la serie de RV)
contra la vol REALIZADA futura (y_sigma) y contra el target de decisión (tight_adv).

Si TimesFM le gana al naive/EWMA acá, entonces SÍ aporta forecasteando vol-como-serie.
Cachea data/timesfm_volseries.csv.
"""
import os, csv, math, argparse
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
DATADIR = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data"))
ETH = os.path.join(DATADIR, "eth_hourly.csv")
DS = os.path.join(DATADIR, "lp_decision_dataset.csv")
CACHE = os.path.join(DATADIR, "timesfm_volseries.csv")

_MODEL = None


def get_model(H, ctx):
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    import timesfm
    m = timesfm.TimesFM_2p5_200M_torch.from_pretrained(timesfm.TimesFM_2p5_200M_torch.DEFAULT_REPO_ID)
    m.compile(timesfm.ForecastConfig(max_context=ctx, max_horizon=H,
                                     normalize_inputs=True, use_continuous_quantile_head=False))
    _MODEL = m
    return m


def load_eth():
    ts, px = [], []
    with open(ETH) as f:
        for r in csv.DictReader(f):
            ts.append(int(r["ts"])); px.append(float(r["price"]))
    return np.array(ts), np.array(px)


def build_rv_series(px, win=24):
    """RV horaria = std de log-returns en las últimas `win` horas. Serie por hora."""
    ret = np.diff(np.log(px))  # len n-1, ret[t] = log(px[t+1]/px[t])
    rv = np.full(len(px), np.nan)
    for t in range(win, len(px)):
        rv[t] = np.std(ret[t - win:t])
    return rv, ret


def dataset_windows():
    out = []
    with open(DS) as f:
        for r in csv.DictReader(f):
            out.append(int(float(r["ts"])))
    return out


def compute(H=168, ctx=720, win=24, limit=None):
    ts, px = load_eth()
    ts_to_i = {int(t): i for i, t in enumerate(ts)}
    rv, ret = build_rv_series(px, win)
    logrv = np.log(np.clip(rv, 1e-8, None))
    windows = dataset_windows()
    if limit:
        windows = windows[:limit]
    model = get_model(H, ctx)

    rows = []
    batch_ts, batch_in, batch_naive, batch_ewma = [], [], [], []

    def ewma_on(series, lam=0.94):
        v = series[0]
        for x in series[1:]:
            v = lam * v + (1 - lam) * x
        return v

    def flush():
        if not batch_in:
            return
        point, _ = model.forecast(horizon=H, inputs=batch_in)  # point: (B, H) en log-RV
        for k in range(len(batch_in)):
            path = np.exp(np.asarray(point[k]))            # RV horaria forecasteada
            tfm_avg = float(np.mean(path))                 # vol horaria media del horizonte
            rows.append((batch_ts[k], tfm_avg * math.sqrt(H),       # window sigma TimesFM
                         batch_naive[k] * math.sqrt(H),             # naive
                         batch_ewma[k] * math.sqrt(H)))             # ewma
        batch_ts.clear(); batch_in.clear(); batch_naive.clear(); batch_ewma.clear()

    for wts in windows:
        i = ts_to_i.get(wts)
        if i is None or i < ctx + win:
            continue
        seg = logrv[i - ctx:i]
        if np.any(np.isnan(seg)):
            continue
        batch_ts.append(wts)
        batch_in.append(seg.astype(np.float32))
        batch_naive.append(float(rv[i - 1]))
        batch_ewma.append(float(ewma_on(rv[i - ctx:i])))
        if len(batch_in) >= 32:
            flush(); print(f"  {len(rows)}/{len(windows)}…", end="\r")
    flush()

    rows.sort()
    with open(CACHE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "tfm_sig", "naive_sig", "ewma_sig"])
        w.writerows(rows)
    print(f"\nescrito {CACHE}: {len(rows)} ventanas")


def evaluate():
    pred = {}
    with open(CACHE) as f:
        for r in csv.DictReader(f):
            pred[int(float(r["ts"]))] = (float(r["tfm_sig"]), float(r["naive_sig"]), float(r["ewma_sig"]))
    ts, adv, sig = [], [], []
    tfm, naive, ewma = [], [], []
    with open(DS) as f:
        for r in csv.DictReader(f):
            t = int(float(r["ts"]))
            if t in pred:
                ts.append(t); adv.append(float(r["y_tight_adv"])); sig.append(float(r["y_sigma"]))
                a, b, c = pred[t]; tfm.append(a); naive.append(b); ewma.append(c)
    adv, sig = np.array(adv), np.array(sig)
    tfm, naive, ewma = np.array(tfm), np.array(naive), np.array(ewma)
    n = len(adv); cut = int(n * 0.60)
    sl = slice(cut, None)

    def corr(a, b): return np.corrcoef(a, b)[0, 1]
    def qlike(p, r):  # sobre varianza
        vp, vr = np.clip(p, 1e-6, None) ** 2, r ** 2
        return np.mean(np.log(vp) + vr / vp)

    print("=" * 74)
    print(f"TimesFM forecasteando la SERIE DE VOL  ({n} ventanas, test OOS={n-cut})")
    print("=" * 74)
    print("¿Qué tan bien predice la VOL FUTURA realizada? (corr y QLIKE, TEST OOS)")
    print(f"{'modelo':10}{'corr↑':>10}{'QLIKE↓':>12}")
    for name, p in [("TimesFM", tfm), ("EWMA", ewma), ("naive", naive)]:
        print(f"{name:10}{corr(p[sl], sig[sl]):>10.3f}{qlike(p[sl], sig[sl]):>12.3f}")
    print("\n¿Sirve para DECIDIR tight-vs-wide? corr(-sigma_pred, tight_adv) TEST OOS:")
    for name, p in [("TimesFM", tfm), ("EWMA", ewma), ("naive", naive)]:
        print(f"  {name:10}{corr(-p[sl], adv[sl]):>+8.3f}")
    print(f"  {'oráculo':10}{corr(-sig[sl], adv[sl]):>+8.3f}   (σ realizada, no operable)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--eval-only", action="store_true")
    ap.add_argument("--ctx", type=int, default=720)
    ap.add_argument("--win", type=int, default=24)
    args = ap.parse_args()
    if not args.eval_only:
        compute(ctx=args.ctx, win=args.win, limit=args.limit)
    evaluate()
