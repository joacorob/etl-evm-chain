#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_dataset — dataset UNICO mergeado para predecir la decision tight-vs-wide
==============================================================================

Idea (pedido del usuario): mergear todo en una tabla por ventana de rebalanceo —
vol multi-horizonte, rangos de high/low (Parkinson), momentum, volumen, y
CORRELACIONES cross-asset (ETH-BTC, vol de BTC) — y dejarla lista para predecir.

Cada fila = una ventana de H horas. Solo features observables EX-ANTE (info hasta
el inicio de la ventana, sin look-ahead). Targets = lo que pasa en las H horas
siguientes (sigma realizado, y el net-vs-HODL de una posicion tight y una wide).

El target estrella es `y_tight_adv = net_tight - net_wide`: si es >0 convino tight.
Predecirlo bien = saber ex-ante cuando poner rango chico. (Techo: corr ~-0.53 con
sigma realizado; EWMA solo da -0.05. Ver REPORT_VOL.md.)

Salida: data/lp_decision_dataset.csv
"""
import os, csv, math, argparse
import numpy as np
import sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import backtest_adaptive as B
from vol_forecast import ewma_sigma

DATADIR = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data"))
ETH = os.path.join(DATADIR, "eth_hourly.csv")
BTC = os.path.join(DATADIR, "btc_hourly.csv")
OUT = os.path.join(DATADIR, "lp_decision_dataset.csv")


def load(path):
    rows = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            rows[int(r["ts"])] = (float(r["price"]), float(r.get("high", r["price"])),
                                  float(r.get("low", r["price"])), float(r.get("volume", 0.0)))
    return rows


def merge_eth_btc():
    eth, btc = load(ETH), load(BTC)
    ts_common = sorted(set(eth) & set(btc))
    px = np.array([eth[t][0] for t in ts_common])
    hi = np.array([eth[t][1] for t in ts_common])
    lo = np.array([eth[t][2] for t in ts_common])
    vol = np.array([eth[t][3] for t in ts_common])
    bpx = np.array([btc[t][0] for t in ts_common])
    ts = np.array(ts_common)
    return ts, px, hi, lo, vol, bpx


def rv(ret, i, lb):
    w = ret[max(0, i - lb):i]
    return float(np.std(w)) if len(w) > 2 else float("nan")


def parkinson(hi, lo, i, lb):
    """Vol de Parkinson (usa rango high/low, ~5x mas eficiente que close-to-close)."""
    seg_h, seg_l = hi[max(0, i - lb):i], lo[max(0, i - lb):i]
    if len(seg_h) < 3:
        return float("nan")
    hl = np.log(seg_h / seg_l) ** 2
    return math.sqrt(np.mean(hl) / (4 * math.log(2)))


def build(H=168, step=168, start_frac=0.06):
    ts, px, hi, lo, vol, bpx = merge_eth_btc()
    ret = np.diff(np.log(px))
    bret = np.diff(np.log(bpx))
    n = len(px)
    i0 = int(n * start_frac)
    idx = list(range(i0, n - H, step))

    feats = []
    for i in idx:
        # ---- features EX-ANTE (solo info <= i) ----
        f = {}
        f["ts"] = int(ts[i])
        f["price"] = float(px[i])
        # vol multi-horizonte (close-to-close)
        for lb in (24, 72, 168, 336):
            f[f"rv_{lb}"] = rv(ret, i, lb)
        f["ewma"] = ewma_sigma(ret, i)
        # Parkinson (high/low)
        f["park_168"] = parkinson(hi, lo, i, 168)
        f["park_72"] = parkinson(hi, lo, i, 72)
        # vol-of-vol: dispersion de rv_24 rolling en 168h
        rvs = [rv(ret, j, 24) for j in range(max(1, i - 168), i)]
        rvs = [x for x in rvs if not math.isnan(x)]
        f["volofvol"] = float(np.std(rvs)) if len(rvs) > 3 else float("nan")
        # momentum / trend
        for lb in (24, 72, 168):
            f[f"mom_{lb}"] = float(np.log(px[i] / px[i - lb])) if i - lb >= 0 else float("nan")
        f["abs_mom_168"] = abs(f["mom_168"])
        f["trend_snr"] = f["mom_168"] / (rv(ret, i, 168) * math.sqrt(168) + 1e-9)
        # drawdown y rango
        seg = px[max(0, i - 168):i + 1]
        f["drawdown_168"] = float(px[i] / np.max(seg) - 1.0)
        f["range_168"] = float((np.max(seg) - np.min(seg)) / np.mean(seg))
        # autocorr lag-1 de returns
        rseg = ret[max(0, i - 168):i]
        if len(rseg) > 10 and np.std(rseg) > 0:
            f["acf1"] = float(np.corrcoef(rseg[:-1], rseg[1:])[0, 1])
        else:
            f["acf1"] = 0.0
        # volumen
        vseg = vol[max(0, i - 168):i]
        vmean_long = np.mean(vol[max(0, i - 720):i]) + 1e-9
        f["vol_z"] = float((np.mean(vseg) - vmean_long) / (np.std(vol[max(0, i - 720):i]) + 1e-9))
        f["vol_trend"] = float(np.mean(vol[max(0, i - 72):i]) / vmean_long)
        # cross-asset BTC
        f["btc_rv_168"] = rv(bret, i, 168)
        a, b = ret[max(0, i - 168):i], bret[max(0, i - 168):i]
        m = min(len(a), len(b))
        f["eth_btc_corr_168"] = float(np.corrcoef(a[-m:], b[-m:])[0, 1]) if m > 10 else float("nan")
        f["btc_mom_168"] = float(np.log(bpx[i] / bpx[i - 168])) if i - 168 >= 0 else float("nan")
        # estacionalidad
        import time as _t
        tm = _t.gmtime(ts[i])
        f["hour"] = tm.tm_hour
        f["dow"] = tm.tm_wday

        # ---- targets FORWARD (i .. i+H) ----
        f["y_sigma"] = math.sqrt(np.sum(ret[i:min(i + H, len(ret))] ** 2))
        nt, ft, irt = B.window_pnl(px, ret, i, 0.05, H)
        nw, fw, irw = B.window_pnl(px, ret, i, 0.40, H)
        f["y_tight_net"] = nt
        f["y_wide_net"] = nw
        f["y_tight_adv"] = nt - nw
        f["y_tight_better"] = int(nt > nw)
        f["y_tight_inrange"] = irt
        feats.append(f)

    cols = list(feats[0].keys())
    with open(OUT, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for f in feats:
            w.writerow(f)
    span = (feats[-1]["ts"] - feats[0]["ts"]) / 86400
    print(f"escrito {OUT}")
    print(f"  filas (ventanas): {len(feats)}  | features: {len([c for c in cols if not c.startswith('y_')])-2}"
          f"  | span: {span:.0f}d ({span/30:.1f} meses)")
    tb = np.mean([f["y_tight_better"] for f in feats])
    print(f"  tight fue mejor en {tb*100:.0f}% de las ventanas")
    print(f"  columnas: {', '.join(cols)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--H", type=int, default=168)
    ap.add_argument("--step", type=int, default=168)
    args = ap.parse_args()
    build(H=args.H, step=args.step)
