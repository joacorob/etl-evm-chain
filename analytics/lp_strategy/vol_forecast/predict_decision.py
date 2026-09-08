#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
predict_decision — ¿un modelo sobre el dataset mergeado decide tight-vs-wide?
=============================================================================

Entrena un GBM (gradient boosting) sobre data/lp_decision_dataset.csv para predecir
`y_tight_adv` (cuanto le gana tight a wide en la ventana). Evalua OUT-OF-SAMPLE
(split temporal: entrena en el pasado, testea en el futuro — sin leakage).

Compara la skill de decision contra:
  - EWMA solo            : usar -ewma como score (el baseline pobre, corr ~-0.05)
  - oraculo              : usar -y_sigma realizado (no operable, techo ~-0.53)
  - always tight / wide  : pisos

Metrica de plata: net-vs-HODL promedio por ventana eligiendo tight cuando el
score predice ventaja. Si el GBM supera al EWMA y se acerca al oraculo => el
dataset rico + modelo SI sirven para decidir el ancho.
"""
import os, csv
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

DS = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "lp_decision_dataset.csv"))
NON_FEAT = {"ts", "price", "y_sigma", "y_tight_net", "y_wide_net", "y_tight_adv",
            "y_tight_better", "y_tight_inrange"}


def load():
    rows = []
    with open(DS) as f:
        for r in csv.DictReader(f):
            rows.append({k: float(v) for k, v in r.items()})
    cols = [c for c in rows[0] if c not in NON_FEAT]
    X = np.array([[r[c] for c in cols] for r in rows])
    y_adv = np.array([r["y_tight_adv"] for r in rows])
    y_sig = np.array([r["y_sigma"] for r in rows])
    t_net = np.array([r["y_tight_net"] for r in rows])
    w_net = np.array([r["y_wide_net"] for r in rows])
    ewma = np.array([r["ewma"] for r in rows])
    # limpiar NaN
    good = ~np.any(np.isnan(X), axis=1)
    return cols, X[good], y_adv[good], y_sig[good], t_net[good], w_net[good], ewma[good]


def decision_net(score, t_net, w_net, thr=0.0):
    """net-vs-HODL promedio eligiendo tight cuando score>thr."""
    return np.where(score > thr, t_net, w_net).mean()


def main():
    cols, X, y_adv, y_sig, t_net, w_net, ewma = load()
    n = len(X)
    cut = int(n * 0.60)
    Xtr, Xte = X[:cut], X[cut:]
    ytr, yte = y_adv[:cut], y_adv[cut:]
    tnet_te, wnet_te = t_net[cut:], w_net[cut:]
    sig_te, ewma_te = y_sig[cut:], ewma[cut:]

    sc = StandardScaler().fit(Xtr)
    gbm = GradientBoostingRegressor(n_estimators=200, max_depth=2, learning_rate=0.03,
                                    subsample=0.8, random_state=0)
    gbm.fit(sc.transform(Xtr), ytr)
    pred_te = gbm.predict(sc.transform(Xte))
    pred_tr = gbm.predict(sc.transform(Xtr))

    print("=" * 78)
    print(f"PREDICTOR DE DECISION tight-vs-wide  |  {n} ventanas, test OOS={len(Xte)}")
    print("=" * 78)
    print(f"corr(GBM, tight_adv)  train = {np.corrcoef(pred_tr, ytr)[0,1]:+.3f}")
    print(f"corr(GBM, tight_adv)  TEST  = {np.corrcoef(pred_te, yte)[0,1]:+.3f}   <- out-of-sample")
    print(f"corr(-EWMA, tight_adv) TEST = {np.corrcoef(-ewma_te, yte)[0,1]:+.3f}   (baseline pobre)")
    print(f"corr(-sigma_real, adv) TEST = {np.corrcoef(-sig_te, yte)[0,1]:+.3f}   (oraculo, no operable)")

    print(f"\nnet-vs-HODL PROMEDIO por ventana (test OOS, {len(Xte)} ventanas):")
    print(f"  always tight          : {tnet_te.mean()*100:+.2f}%")
    print(f"  always wide           : {wnet_te.mean()*100:+.2f}%")
    print(f"  decision por EWMA     : {decision_net(-ewma_te, tnet_te, wnet_te, thr=-np.median(ewma_te))*100:+.2f}%")
    print(f"  decision por GBM      : {decision_net(pred_te, tnet_te, wnet_te)*100:+.2f}%   <- operable")
    print(f"  decision por oraculo  : {decision_net(-sig_te, tnet_te, wnet_te, thr=-np.median(sig_te))*100:+.2f}%")
    print(f"  best-per-window       : {np.maximum(tnet_te, wnet_te).mean()*100:+.2f}%   (techo)")

    # feature importance
    imp = sorted(zip(cols, gbm.feature_importances_), key=lambda x: -x[1])
    print("\ntop features (importancia GBM):")
    for c, v in imp[:8]:
        print(f"  {c:18}{v:.3f}")

    gbm_net = decision_net(pred_te, tnet_te, wnet_te)
    best_fixed = max(tnet_te.mean(), wnet_te.mean())
    print(f"\nVEREDICTO: GBM decision {gbm_net*100:+.2f}% vs mejor-fija {best_fixed*100:+.2f}%"
          f"  => edge {(gbm_net-best_fixed)*100:+.2f}pp  ({'APORTA' if gbm_net>best_fixed else 'NO aporta'})")


if __name__ == "__main__":
    main()
