#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xinrange — foto→P(la posicion LP se queda EN RANGO) | target NO adversarial
============================================================================

Mismo motor que xtrade (clasificador calibrado, walk-forward) pero el target es
`timeout` del dataset de xtrade = el precio NO tocó ninguna barrera ±1σ_H en H horas
= la posicion LP se quedó EN RANGO todo el holding. Esto es DISPERSION, no direccion:
no hay nadie arbitrandolo y la vol tiene memoria (RV corr 0.42), asi que la
calibracion DEBERIA funcionar — al reves que el target direccional.

Features: las CLASICAS de vol/momentum (las correctas acá; el flujo es direccional).
Baseline a batir: regla por EWMA de vol (baja vol -> mas probable en rango).

Yardstick comun con funding_regime.py: ¿AUC OOS signif >0.5 (IC95 no cruza),
estable en ambas mitades, y calibracion monotona?
"""
import os, csv, math
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, brier_score_loss

HERE = os.path.dirname(os.path.abspath(__file__))
DS = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data", "xtrade_dataset.csv"))
FLOW = {"ofi_24", "ofi_72", "ofi_168", "ofi_z", "trade_size_z", "funding_now",
        "funding_mean_168", "funding_z", "funding_mom", "basis", "basis_mean_72"}
NON_FEAT = {"ts", "price", "y_long", "r_real", "exit_h", "timeout"}


def load():
    rows = []
    with open(DS) as f:
        for r in csv.DictReader(f):
            rows.append({k: float(v) for k, v in r.items()})
    return rows


def calib(p, y, bins=10):
    print("  calibracion OOS (P dicha vs frecuencia real de quedar EN RANGO):")
    edges = np.linspace(0, 1, bins + 1)
    out = []
    for a, b in zip(edges[:-1], edges[1:]):
        m = (p >= a) & (p < b) if b < 1 else (p >= a) & (p <= b)
        if m.sum() >= 5:
            print(f"    P~{(a+b)/2:.2f}  n={int(m.sum()):4d}  real={y[m].mean():.2f}")
            out.append(((a + b) / 2, y[m].mean()))
    # monotonia (Spearman entre prob-bucket y real)
    if len(out) >= 3:
        xs = np.array([o[0] for o in out]); ys = np.array([o[1] for o in out])
        rho = np.corrcoef(np.argsort(np.argsort(xs)), np.argsort(np.argsort(ys)))[0, 1]
        print(f"  monotonia calibracion (Spearman) = {rho:+.2f}  (>0.8 = bien calibrado)")


def main():
    rows = load()
    cols = [c for c in rows[0] if c not in NON_FEAT and c not in FLOW]   # solo clasicas (vol/mom)
    X, y, ewma = [], [], []
    for r in rows:
        row = [r[c] for c in cols]
        if any(math.isnan(v) for v in row):
            continue
        X.append(row); y.append(int(r["timeout"])); ewma.append(r["ewma"])
    X, y, ewma = np.array(X), np.array(y), np.array(ewma)
    n = len(X); cut = int(n * 0.60)
    base = y[cut:].mean()

    sc = StandardScaler().fit(X[:cut])
    clf = GradientBoostingClassifier(n_estimators=150, max_depth=2, learning_rate=0.03,
                                     subsample=0.8, random_state=0)
    cal = CalibratedClassifierCV(clf, method="isotonic", cv=3)
    cal.fit(sc.transform(X[:cut]), y[:cut])
    p = cal.predict_proba(sc.transform(X[cut:]))[:, 1]
    yte = y[cut:]

    print("=" * 74)
    print(f"xINRANGE — P(LP en rango) | target NO adversarial | n={n} OOS={n-cut}")
    print("=" * 74)
    print(f"base-rate en-rango (OOS) = {base*100:.1f}%")
    auc = roc_auc_score(yte, p)
    print(f"\nGBM(vol):   AUC={auc:.3f}  Brier={brier_score_loss(yte,p):.4f} (base {base*(1-base):.4f})")
    # baseline EWMA: menos vol -> mas en-rango => score = -ewma
    e = ewma[cut:]
    auc_e = roc_auc_score(yte, -e)
    print(f"EWMA(-vol): AUC={auc_e:.3f}   (baseline simple)")

    # significancia bootstrap del GBM
    rng = np.random.RandomState(0); aucs = []
    for _ in range(1000):
        idx = rng.randint(0, len(yte), len(yte))
        if len(set(yte[idx])) > 1:
            aucs.append(roc_auc_score(yte[idx], p[idx]))
    aucs = np.array(aucs)
    print(f"AUC bootstrap: media={aucs.mean():.3f}  IC95=[{np.percentile(aucs,2.5):.3f}, "
          f"{np.percentile(aucs,97.5):.3f}]  P(AUC>0.5)={(aucs>0.5).mean():.2f}")

    # estabilidad por mitades (accuracy de "apostar en-rango si p>base")
    h = len(yte) // 2
    for lbl, sl in [("1a mitad", slice(0, h)), ("2a mitad", slice(h, None))]:
        pred = (p[sl] > base).astype(int)
        acc = (pred == yte[sl]).mean()
        print(f"  {lbl}: AUC={roc_auc_score(yte[sl], p[sl]):.3f}  acc(p>base)={acc*100:.1f}%")
    print()
    calib(p, yte)


if __name__ == "__main__":
    main()
