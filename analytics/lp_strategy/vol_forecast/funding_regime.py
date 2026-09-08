#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
funding_regime — la señal de FUNDING aislada, por regimen (mecanismo, no ML)
============================================================================

Hipotesis con mecanismo economico real: cuando el funding esta MUY positivo, los
longs estan crowded y pagan carry -> sesgo a reversion (bajar). Muy negativo -> al
reves. A diferencia del precio, esto NO es puro random: hay una fuerza mecanica
(el carry) y el posicionamiento es observable.

Sin GBM (que sobreajusta el tramo bueno): solo estadistica condicional.
  1. bucketear por funding_z (quintiles) y ver retorno forward H medio + hit por bucket
  2. corr(funding_z, retorno forward)  -> ¿hay relacion monotona?
  3. regla operable: signo (contrarian o momentum) FIJADO en train, aplicado en test
     OOS. pnl/trade + estabilidad por mitades.

Yardstick comun con xinrange.py.
"""
import os, sys, math
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from xtrade import load_flow, triple_barrier
from build_dataset import merge_eth_btc
from build_dataset import rv as _rv


def main(H=168, step=8, start_frac=0.06):
    ts, px, hi, lo, vol, bpx = merge_eth_btc()
    ret = np.diff(np.log(px))
    _, _, _, _, fr = load_flow(ts)                    # funding alineado al grid (forward-fill, sin look-ahead)
    n = len(px)
    idx = [i for i in range(int(n * start_frac), n - H, step)]

    fz, fwd, lab = [], [], []
    for i in idx:
        if i < 720 or math.isnan(fr[i - 1]):
            continue
        seg = fr[max(0, i - 720):i]
        seg = seg[~np.isnan(seg)]
        if len(seg) < 50 or np.std(seg) == 0:
            continue
        z = (fr[i - 1] - np.mean(seg)) / (np.std(seg) + 1e-12)
        sigma_h = _rv(ret, i, 168) * math.sqrt(H)
        if math.isnan(sigma_h) or sigma_h <= 0:
            continue
        _, r_real, _, _ = triple_barrier(px, i, H, 1.0, sigma_h)
        fz.append(z); fwd.append(r_real); lab.append(1 if r_real > 0 else 0)
    fz, fwd, lab = np.array(fz), np.array(fwd), np.array(lab)
    n = len(fz); cut = int(n * 0.60)

    print("=" * 74)
    print(f"FUNDING REGIME — señal de funding aislada | n={n}  OOS={n-cut}  H={H}h")
    print("=" * 74)

    # 1) buckets por funding_z (sobre TODO, descriptivo)
    qs = np.quantile(fz, [0, .2, .4, .6, .8, 1.0])
    print("retorno forward H por quintil de funding_z (descriptivo, full sample):")
    print(f"  {'quintil':22}{'n':>5}{'fwd_ret':>10}{'hit_up':>9}")
    for k in range(5):
        m = (fz >= qs[k]) & (fz <= qs[k + 1]) if k == 4 else (fz >= qs[k]) & (fz < qs[k + 1])
        nm = ["muy neg", "neg", "neutro", "pos", "muy pos"][k]
        print(f"  Q{k+1} funding {nm:10}{int(m.sum()):>5}{fwd[m].mean()*100:>+9.2f}%{lab[m].mean()*100:>8.1f}%")

    # 2) corr (train) — define el SIGNO de la regla sin mirar el test
    c_tr = np.corrcoef(fz[:cut], fwd[:cut])[0, 1]
    sign = -1.0 if c_tr < 0 else 1.0   # c<0 => contrarian (short cuando funding alto)
    modo = "CONTRARIAN (short si funding alto)" if sign < 0 else "MOMENTUM (long si funding alto)"
    print(f"\ncorr(funding_z, fwd_ret) train = {c_tr:+.3f}  -> regla: {modo}")
    print(f"corr(funding_z, fwd_ret) TEST  = {np.corrcoef(fz[cut:], fwd[cut:])[0,1]:+.3f}  (OOS, no usado para fijar signo)")

    # 3) regla operable OOS: posicion = sign * sign(funding_z); pnl = pos * fwd_ret
    def pnl_stats(sl):
        pos = sign * np.sign(fz[sl])
        pnl = (pos * fwd[sl])
        hit = ((pos > 0) == (fwd[sl] > 0)).mean()
        return pnl.mean() * 100, hit * 100, len(pnl)

    p_all, h_all, nn = pnl_stats(slice(cut, None))
    print(f"\nregla OOS  pnl/trade={p_all:+.3f}%  hit={h_all:.1f}%  (always-long ref={fwd[cut:].mean()*100:+.3f}%)")
    hh = (n - cut) // 2
    p1, h1, _ = pnl_stats(slice(cut, cut + hh))
    p2, h2, _ = pnl_stats(slice(cut + hh, None))
    print(f"  estabilidad: 1a mitad pnl={p1:+.3f}% hit={h1:.1f}%  |  2a mitad pnl={p2:+.3f}% hit={h2:.1f}%")

    # significancia: bootstrap del pnl/trade OOS
    rng = np.random.RandomState(0)
    pos = sign * np.sign(fz[cut:]); base_pnl = pos * fwd[cut:]
    boots = [base_pnl[rng.randint(0, len(base_pnl), len(base_pnl))].mean() * 100 for _ in range(2000)]
    boots = np.array(boots)
    print(f"  pnl/trade bootstrap IC95=[{np.percentile(boots,2.5):+.3f}%, {np.percentile(boots,97.5):+.3f}%]  "
          f"P(pnl>0)={(boots>0).mean():.2f}")


if __name__ == "__main__":
    main()
