#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xrange_optimizer — elegir el ancho v3 que maximiza  fees·P(in-range) − LVR
==========================================================================

Conecta el predictor calibrado de P(in-range) (xinrange) con la mecánica real del
motor (backtest_real: LP value v3 + short estático = delta-neutral). En cada ventana,
para un menú de anchos, decide el ancho óptimo EX-ANTE maximizando:

    EV(w) = fees(w)·P(in-range|w) − LVR(w)

con fundamentos reales (búsqueda web):
  - LVR rate full-range = σ²/8 por año; concentrado = ·η(w)  (Milionis et al. 2022)
  - capital efficiency η(w) = 1/(1 − ((1−w)/(1+w))^{1/4})     (math sqrt-price v3)
  - fee_apr(w) = fee_ref · η(w)/η(w_ref)  -> tighter = más fee density
P(in-range|w) sale de un GBM calibrado (features de vol + w + w/σ), walk-forward.

Después VALIDA contra el path REAL hora a hora (single position 7d, short estático,
funding): net realizado de cada estrategia OOS:
  fija (cada w) | vol-adaptive (w∝σ) | MODELO EV-óptimo | oráculo (mejor w con hindsight)

Si el modelo EV se acerca al oráculo y le gana a las fijas/heurística => la P(in-range)
calibrada SÍ sirve para dimensionar el ancho. Honesto: mide net real, no la EV.
"""
import os, sys, csv, math
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
from xtrade import feature_row
from build_dataset import merge_eth_btc, rv
from backtest_real import build_lp, lp_value, lp_amounts   # mecánica v3 real

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score

WIDTHS = [0.075, 0.10, 0.15, 0.25, 0.40]
W_REF = 0.40
H = 168
STEP = 8
FEE_REF_APR = 0.30      # fee anual realista a ±40% (in-range)
FUNDING_APR = 0.05
YR = 8760.0 / H         # ventanas por año (anualización comparativa)


def cap_eff(w):
    r = (1.0 - w) / (1.0 + w)
    return 1.0 / (1.0 - r ** 0.25)


ETA = {w: cap_eff(w) for w in WIDTHS}


def window_realized(px, i, w, hedge=True):
    """Net realizado (fracción de capital) de una posición v3 de ancho w sobre el
    path real hora a hora, single position 7d + short estático. Incluye fees in-range,
    IL, PnL del hedge y funding => esto ES fees − LVR realizado."""
    cap = 1.0; dt = 1.0 / 8760.0
    feeapr = FEE_REF_APR * ETA[w] / ETA[W_REF]
    S = px[i]
    L, sA, sB, Pa, Pb = build_lp(S, cap, w)
    eth0, _ = lp_amounts(L, sA, sB, S)
    Hh = eth0 if hedge else 0.0
    fees = fund = hpnl = 0.0; inh = 0
    end = min(i + H, len(px) - 1)
    for t in range(i + 1, end + 1):
        Sp, Sc = px[t - 1], px[t]
        if Pa <= Sp <= Pb:
            fees += lp_value(L, sA, sB, Sp) * feeapr * dt; inh += 1
        fund += abs(Hh) * Sp * FUNDING_APR * dt
        hpnl += Hh * (Sp - Sc)                       # short gana si baja
    val = lp_value(L, sA, sB, px[end])
    net = val + fees + hpnl - fund - cap
    return net, inh / max(1, end - i)


def rolling(px, i0, i1, decide, rebal_bps=5.0, perp_bps=5.0):
    """Backtest ROLLING con re-center real: una posición que se mantiene, re-decide el
    ancho cada H horas (decide(t,S)->w) y re-centra pagando costo al salir de rango o en
    cada boundary. Acá tight PAGA el churn (sale seguido) => la P(in-range) tiene con qué
    ganarse el lugar. Devuelve net total y nº de re-centers."""
    cap = 1.0; dt = 1.0 / 8760.0
    S = px[i0]; w = decide(i0, S)
    feeapr = FEE_REF_APR * ETA[w] / ETA[W_REF]
    L, sA, sB, Pa, Pb = build_lp(S, cap, w)
    Hh = lp_amounts(L, sA, sB, S)[0]
    fees = fund = hpnl = costs = 0.0; recenters = 0
    for t in range(i0 + 1, i1):
        Sp, Sc = px[t - 1], px[t]
        if Pa <= Sp <= Pb:
            fees += lp_value(L, sA, sB, Sp) * feeapr * dt
        fund += abs(Hh) * Sp * FUNDING_APR * dt
        hpnl += Hh * (Sp - Sc)
        exited = not (Pa <= Sc <= Pb)
        boundary = ((t - i0) % H == 0)
        if exited or boundary:
            posval = lp_value(L, sA, sB, Sc)
            costs += posval * (rebal_bps / 1e4)
            w = decide(t, Sc) if boundary else w        # mid-window: mismo ancho, solo re-centra
            feeapr = FEE_REF_APR * ETA[w] / ETA[W_REF]
            L, sA, sB, Pa, Pb = build_lp(Sc, posval, w)
            ethn = lp_amounts(L, sA, sB, Sc)[0]
            costs += abs(ethn - Hh) * Sc * (perp_bps / 1e4); Hh = ethn
            recenters += 1
    posval = lp_value(L, sA, sB, px[i1 - 1])
    return (posval + fees + hpnl - fund - costs) / cap - 1.0, recenters


def in_range_label(px, i, w):
    end = min(i + H, len(px) - 1)
    seg = px[i + 1:end + 1]
    S = px[i]
    return int(seg.min() >= S * (1 - w) and seg.max() <= S * (1 + w))


def build():
    ts, px, hi, lo, vol, bpx = merge_eth_btc()
    ret, bret = np.diff(np.log(px)), np.diff(np.log(bpx))
    n = len(px)
    idx = list(range(int(n * 0.06), n - H, STEP))
    rows = []   # una fila por (snapshot, width)
    snaps = []  # metadata por snapshot
    for i in idx:
        sigma_h = rv(ret, i, 168) * math.sqrt(H)
        if math.isnan(sigma_h) or sigma_h <= 0:
            continue
        f = feature_row(ts, px, hi, lo, vol, bpx, ret, bret, i)
        if any(isinstance(v, float) and math.isnan(v) for v in f.values()):
            continue
        feat = {k: v for k, v in f.items() if k not in ("ts", "price")}
        realized = {w: window_realized(px, i, w)[0] for w in WIDTHS}   # net (descarta frac)
        snaps.append({"ts": int(ts[i]), "sigma": sigma_h, "feat": feat, "realized": realized})
        for w in WIDTHS:
            r = dict(feat); r["w"] = w; r["w_over_sigma"] = w / sigma_h
            r["_ts"] = int(ts[i]); r["_y"] = in_range_label(px, i, w)
            rows.append(r)
    return snaps, rows


def main():
    snaps, rows = build()
    ns = len(snaps)
    cut = int(ns * 0.60)
    cut_ts = snaps[cut]["ts"]
    feat_cols = [c for c in rows[0] if not c.startswith("_")]

    def mat(subset):
        X = np.array([[r[c] for c in feat_cols] for r in subset])
        y = np.array([r["_y"] for r in subset])
        return X, y

    tr = [r for r in rows if r["_ts"] < cut_ts]
    te = [r for r in rows if r["_ts"] >= cut_ts]
    Xtr, ytr = mat(tr); Xte, yte = mat(te)
    sc = StandardScaler().fit(Xtr)
    clf = GradientBoostingClassifier(n_estimators=200, max_depth=3, learning_rate=0.03,
                                     subsample=0.8, random_state=0)
    cal = CalibratedClassifierCV(clf, method="isotonic", cv=3)
    cal.fit(sc.transform(Xtr), ytr)
    pte = cal.predict_proba(sc.transform(Xte))[:, 1]

    print("=" * 80)
    print(f"xRANGE OPTIMIZER — max fees·P(in-range) − LVR  | {ns} ventanas, OOS={ns-cut}")
    print("=" * 80)
    print(f"P(in-range|w) modelo: AUC OOS = {roc_auc_score(yte, pte):.3f}  "
          f"(menú anchos: {[f'{w*100:.0f}%' for w in WIDTHS]})")

    # P(in-range) por (ts,w) en OOS
    P = {}
    for r, p in zip(te, pte):
        P[(r["_ts"], r["w"])] = p

    # estrategias OOS
    oos = snaps[cut:]
    acc = {f"fija_{w}": [] for w in WIDTHS}
    acc.update({"vol_adaptive": [], "modelo_EV": [], "oraculo": []})
    pick_match = 0
    fee_lvr_dump = []
    for s in oos:
        t = s["ts"]; sig = s["sigma"]; realized = s["realized"]
        for w in WIDTHS:
            acc[f"fija_{w}"].append(realized[w])
        # vol-adaptive: ancho ∝ sigma (más vol -> más ancho), clip al menú
        w_target = min(WIDTHS, key=lambda w: abs(w - np.clip(2.0 * sig, WIDTHS[0], WIDTHS[-1])))
        acc["vol_adaptive"].append(realized[w_target])
        # modelo EV: max_w  fee_density(w)·P(in-range) − costo_romper·(1−P)
        # (con hedge, el LVR teórico lo cubre el short; el costo real de tight es
        #  ROMPER rango: dejás de cobrar + re-center. Eso es (1−P), no σ²/8·η.)
        REBAL = 0.0010          # ~10bps de re-center + IL al salir
        best_w, best_ev = None, -1e9
        for w in WIDTHS:
            p = P.get((t, w), 0.5)
            fees = FEE_REF_APR * (ETA[w] / ETA[W_REF]) / YR          # fee density de la ventana
            ev = fees * p - REBAL * (1.0 - p)
            if ev > best_ev:
                best_ev, best_w = ev, w
        acc["modelo_EV"].append(realized[best_w])
        # oráculo: mejor w realizado
        w_or = max(WIDTHS, key=lambda w: realized[w])
        acc["oraculo"].append(realized[w_or])
        if best_w == w_or:
            pick_match += 1
        fee_lvr_dump.append((best_w, w_or, w_target))

    print(f"\nnet realizado por ventana (OOS, single 7d, short estático, "
          f"anualizado ×{YR:.0f}):")
    print(f"{'estrategia':18}{'net/ventana':>13}{'APR aprox':>12}{'win-rate>0':>12}")
    def line(name, arr):
        a = np.array(arr); apr = ((1 + a.mean()) ** YR - 1) * 100
        print(f"{name:18}{a.mean()*100:>+12.3f}%{apr:>+11.1f}%{(a>0).mean()*100:>11.0f}%")
    for w in WIDTHS:
        line(f"fija ±{w*100:.0f}%", acc[f"fija_{w}"])
    line("vol-adaptive", acc["vol_adaptive"])
    line("MODELO EV", acc["modelo_EV"])
    line("oráculo", acc["oraculo"])

    me = np.array(acc["modelo_EV"]); orc = np.array(acc["oraculo"])
    best_fixed = max(WIDTHS, key=lambda w: np.mean(acc[f"fija_{w}"]))
    bf = np.array(acc[f"fija_{best_fixed}"])
    print(f"\nmodelo elige el ancho del oráculo en {pick_match/len(oos)*100:.0f}% de ventanas")
    print(f"MODELO EV {me.mean()*100:+.3f}%  vs  mejor-fija (±{best_fixed*100:.0f}%) "
          f"{bf.mean()*100:+.3f}%  vs  oráculo {orc.mean()*100:+.3f}%")
    gap_close = (me.mean() - bf.mean()) / (orc.mean() - bf.mean() + 1e-12) * 100
    print(f"=> el modelo cierra {gap_close:.0f}% del gap entre la mejor-fija y el oráculo")
    from collections import Counter
    cm = Counter(d[0] for d in fee_lvr_dump)
    print(f"anchos que elige el modelo: " +
          ", ".join(f"±{w*100:.0f}%:{cm.get(w,0)}" for w in WIDTHS))

    # ============ BACKTEST ROLLING CON RE-CENTER REAL (el test con dientes) ============
    print("\n" + "=" * 80)
    print("BACKTEST ROLLING (re-center real: tight paga el churn) — net acumulado OOS")
    print("=" * 80)
    # mapa ts->indice en px
    tsarr, px2 = _px_index()
    i_of = {int(t): k for k, t in enumerate(tsarr)}
    i0 = i_of[oos[0]["ts"]]; i1 = i_of[oos[-1]["ts"]] + H

    REBAL = 0.0010
    def decide_model(t, S):
        tt = int(tsarr[t])
        best_w, best_ev = WIDTHS[-1], -1e9
        for w in WIDTHS:
            p = P.get((tt, w))
            if p is None:
                continue
            ev = (FEE_REF_APR * (ETA[w] / ETA[W_REF]) / YR) * p - REBAL * (1.0 - p)
            if ev > best_ev:
                best_ev, best_w = ev, w
        return best_w
    def decide_voladaptive(t, S):
        i = t
        sig = rv(np.diff(np.log(px2)), i, 168) * math.sqrt(H)
        return min(WIDTHS, key=lambda w: abs(w - np.clip(2.0 * sig, WIDTHS[0], WIDTHS[-1])))

    print(f"{'estrategia':18}{'net OOS':>11}{'APR':>10}{'re-centers':>12}")
    for w in WIDTHS:
        net, rc = rolling(px2, i0, i1, lambda t, S, _w=w: _w)
        days = (i1 - i0) / 24.0
        apr = ((1 + net) ** (365.0 / days) - 1) * 100
        print(f"fija ±{w*100:<4.0f}      {net*100:>+10.1f}%{apr:>+9.0f}%{rc:>12}")
    # uso principista de la calibración: el w MÁS TIGHT con P(in-range|w) >= objetivo
    def make_target(thr):
        def d(t, S):
            tt = int(tsarr[t])
            ok = [w for w in WIDTHS if P.get((tt, w), 0.0) >= thr]
            return min(ok) if ok else WIDTHS[-1]
        return d

    strategies = [("vol-adaptive", decide_voladaptive),
                  ("MODELO EV", decide_model),
                  ("MODELO P>=0.4", make_target(0.4)),
                  ("MODELO P>=0.5", make_target(0.5)),
                  ("MODELO P>=0.6", make_target(0.6))]
    for name, fn in strategies:
        net, rc = rolling(px2, i0, i1, fn)
        days = (i1 - i0) / 24.0
        apr = ((1 + net) ** (365.0 / days) - 1) * 100
        print(f"{name:18}{net*100:>+10.1f}%{apr:>+9.0f}%{rc:>12}")


def _px_index():
    ts, px, hi, lo, vol, bpx = merge_eth_btc()
    return ts, px


if __name__ == "__main__":
    main()
