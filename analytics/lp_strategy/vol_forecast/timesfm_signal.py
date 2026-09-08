#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
timesfm_signal — la libreria de Google como estimador de DISPERSION (no de precio)
==================================================================================

Usa TimesFM 2.5 (200M, torch) NO para predecir la direccion del precio (imposible)
sino para estimar la DISPERSION esperada sobre el horizonte H via el ancho de su
banda de cuantiles. Para cada ventana:
  - alimenta el log-precio horario de las ultimas `ctx` horas
  - forecast de H pasos con quantile head
  - dispersion predicha = (q90 - q10) del log-precio al final del horizonte
    (proxy de sigma_window que despues comparamos contra el target tight_adv)

Cachea a data/timesfm_disp.csv (ts, tfm_disp) para no recomputar.
Eval: corr(tfm_disp, y_tight_adv) — ¿le gana al EWMA (~-0.05) o al GBM (~+0.08)?
"""
import os, csv, math, sys, argparse
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
DATADIR = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data"))
ETH = os.path.join(DATADIR, "eth_hourly.csv")
CACHE = os.path.join(DATADIR, "timesfm_disp.csv")
DS = os.path.join(DATADIR, "lp_decision_dataset.csv")

_MODEL = None


def get_model(max_horizon=168, max_context=1024):
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    import timesfm
    m = timesfm.TimesFM_2p5_200M_torch.from_pretrained(timesfm.TimesFM_2p5_200M_torch.DEFAULT_REPO_ID)
    m.compile(timesfm.ForecastConfig(
        max_context=max_context, max_horizon=max_horizon,
        normalize_inputs=True, use_continuous_quantile_head=True,
        fix_quantile_crossing=True))
    _MODEL = m
    return m


def load_eth():
    ts, px = [], []
    with open(ETH) as f:
        for r in csv.DictReader(f):
            ts.append(int(r["ts"])); px.append(float(r["price"]))
    return np.array(ts), np.array(px)


def dataset_windows():
    out = []
    with open(DS) as f:
        for r in csv.DictReader(f):
            out.append(int(float(r["ts"])))
    return out


def compute(H=168, ctx=512, limit=None):
    ts, px = load_eth()
    ts_to_i = {int(t): i for i, t in enumerate(ts)}
    windows = dataset_windows()
    if limit:
        windows = windows[:limit]
    model = get_model(max_horizon=H, max_context=ctx)
    logpx = np.log(px)

    # batch las inputs
    rows = []
    batch_ts, batch_in, batch_base = [], [], []

    def flush():
        if not batch_in:
            return
        _, q = model.forecast(horizon=H, inputs=batch_in)  # q: (B, H, n_q)
        for k in range(len(batch_in)):
            qk = np.asarray(q[k])  # (H, n_q)
            nq = qk.shape[-1]
            # cuantiles TimesFM: 10 deciles 0.1..0.9 (+ mean). tomamos extremos disponibles.
            lo = qk[-1, 1] if nq >= 9 else qk[-1, 0]
            hi = qk[-1, -2] if nq >= 9 else qk[-1, -1]
            disp = float(hi - lo)  # en log-precio (input era log)
            rows.append((batch_ts[k], abs(disp)))
        batch_ts.clear(); batch_in.clear(); batch_base.clear()

    for wts in windows:
        i = ts_to_i.get(wts)
        if i is None or i < ctx:
            continue
        seg = logpx[i - ctx:i]
        batch_ts.append(wts)
        batch_in.append(seg.astype(np.float32))
        if len(batch_in) >= 32:
            flush()
            print(f"  {len(rows)}/{len(windows)} ventanas…", end="\r")
    flush()

    rows.sort()
    with open(CACHE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "tfm_disp"])
        w.writerows(rows)
    print(f"\nescrito {CACHE}: {len(rows)} ventanas")
    return rows


def evaluate():
    """corr de la dispersion de TimesFM con el target tight_adv (OOS-aware)."""
    disp = {}
    with open(CACHE) as f:
        for r in csv.DictReader(f):
            disp[int(float(r["ts"]))] = float(r["tfm_disp"])
    adv, tfm, ewma, sig = [], [], [], []
    with open(DS) as f:
        for r in csv.DictReader(f):
            t = int(float(r["ts"]))
            if t in disp:
                adv.append(float(r["y_tight_adv"])); tfm.append(disp[t])
                ewma.append(float(r["ewma"])); sig.append(float(r["y_sigma"]))
    adv, tfm, ewma, sig = map(np.array, (adv, tfm, ewma, sig))
    n = len(adv); cut = int(n * 0.60)
    print(f"\n=== TimesFM como señal de dispersion ({n} ventanas, test OOS={n-cut}) ===")
    print(f"corr(-tfm_disp, tight_adv) FULL = {np.corrcoef(-tfm, adv)[0,1]:+.3f}")
    print(f"corr(-tfm_disp, tight_adv) TEST = {np.corrcoef(-tfm[cut:], adv[cut:])[0,1]:+.3f}   <- OOS")
    print(f"corr(-EWMA,     tight_adv) TEST = {np.corrcoef(-ewma[cut:], adv[cut:])[0,1]:+.3f}   (baseline)")
    print(f"corr(-sigma_real,tight_adv)TEST = {np.corrcoef(-sig[cut:], adv[cut:])[0,1]:+.3f}   (oraculo)")
    print(f"corr(tfm_disp, sigma_real) FULL = {np.corrcoef(tfm, sig)[0,1]:+.3f}   (¿predice la vol?)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="solo N ventanas (smoke test)")
    ap.add_argument("--eval-only", action="store_true")
    ap.add_argument("--ctx", type=int, default=512)
    args = ap.parse_args()
    if not args.eval_only:
        compute(ctx=args.ctx, limit=args.limit)
    evaluate()
