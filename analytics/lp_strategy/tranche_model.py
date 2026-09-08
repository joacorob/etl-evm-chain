#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tranche_model — estructura junior/senior (idea de @mariorz) para forzar >15%
============================================================================

El motor delta-neutral (ETH/USDC concentrado + short HL) rinde, medido empírica-
mente (revert ~1.800 posiciones + funding real HL capeado ~11%), un NETO de
~8-14%/año pero con MUCHA dispersión (gana el "finishing", no el promedio).
Eso solo no garantiza 15%.

Mario propuso tranches (como TradFi structured products): partís el capital en
SENIOR (cupón fijo, cobra primero, bajo riesgo) y JUNIOR (uncapped, absorbe
pérdidas primero). El junior queda APALANCADO sobre el exceso del motor por
encima del cupón senior. Así fabricás un tramo >15% con riesgo explícito y
acotado, sin pedir prestado a nadie (es estructura interna del capital).

Waterfall por escenario (retorno del pool R sobre capital total C):
  PnL_pool = R * C
  senior_claim = c_senior * C_senior            (cupón objetivo)
  senior_paga  = min(max(PnL_pool,0)+0, senior_claim) acotado >=0 ... ver código
  junior_resid = PnL_pool - senior_pagado
  ret_junior   = junior_resid / C_junior
El junior pierde primero: si PnL_pool < senior_claim, el junior come la diferencia.
"""
import sys, os, argparse
import numpy as np
HERE=os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0,HERE)

def engine_samples(n, mean, std, mode="normal", seed=11):
    """Distribución de retorno anual del motor. mean/std del neto delta-neutral.
    mode=mc usa el Monte Carlo real (xyield_engine)."""
    rng=np.random.default_rng(seed)
    if mode=="mc":
        from xyield_engine import DNParams, monte_carlo_dn
        p=DNParams(n_paths=n, fee_apr_in_range=32.0, funding_annual=6.0,
                   floor_apr=3.1, vol_annual=0.39, half_width_pct=15.0)
        return monte_carlo_dn(p)
    # normal truncada suave (motor con cola izquierda real)
    s=rng.normal(mean/100, std/100, n)
    return s

def waterfall(R, s_frac, c_senior):
    """R: array retornos del pool (frac sobre capital total=1).
    Seniority correcta: el JUNIOR absorbe pérdidas primero; el senior cobra
    principal+cupón antes que nadie. Devuelve (ret_senior, ret_junior)."""
    Cs, Cj = s_frac, 1.0 - s_frac
    V = 1.0 + R                          # valor final del pool por unidad de capital
    senior_owed = Cs * (1.0 + c_senior)  # principal + cupón del senior
    senior_payout = np.clip(np.minimum(V, senior_owed), 0.0, None)
    junior_payout = np.maximum(V - senior_owed, 0.0)
    ret_senior = senior_payout / Cs - 1.0
    ret_junior = junior_payout / Cj - 1.0
    return ret_senior, ret_junior

def stats(x):
    x=np.asarray(x); q=lambda p: float(np.percentile(x,p))
    return dict(mean=float(np.mean(x))*100, med=float(np.median(x))*100,
                p5=q(5)*100, p95=q(95)*100, pgt15=float(np.mean(x>0.15))*100,
                pneg=float(np.mean(x<0))*100)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--engine-mean", type=float, default=10.0, help="media neta del motor %")
    ap.add_argument("--engine-std", type=float, default=11.0, help="desvío del motor %")
    ap.add_argument("--mode", default="normal", choices=["normal","mc"])
    ap.add_argument("--n", type=int, default=50000)
    args=ap.parse_args()

    R=engine_samples(args.n, args.engine_mean, args.engine_std, mode=args.mode)
    es=stats(R)
    print("="*78)
    print("MOTOR delta-neutral (input):")
    print(f"  media={es['mean']:+.1f}%  mediana={es['med']:+.1f}%  p5={es['p5']:+.1f}%  "
          f"p95={es['p95']:+.1f}%  P(>15%)={es['pgt15']:.0f}%  P(pérdida)={es['pneg']:.0f}%")
    print("="*78)
    print("\nTRAMOS junior/senior — el junior fabrica >15% absorbiendo riesgo:\n")
    print(f"{'senior%':>8}{'cupón':>7} | {'SENIOR realizado':>22} | {'JUNIOR realizado':>34}")
    print(f"{'':>8}{'':>7} | {'media':>7}{'p5':>7}{'Ppérd':>8} | {'media':>7}{'med':>7}{'p5':>7}{'P>15':>7}{'Ppérd':>7}")
    for s_frac in [0.5, 0.6, 0.7]:
        for c in [0.05, 0.06]:
            rs, rj = waterfall(R, s_frac, c)
            ss, sj = stats(rs), stats(rj)
            print(f"{int(s_frac*100):>7}%{c*100:>6.0f}% | "
                  f"{ss['mean']:>6.1f}%{ss['p5']:>6.1f}%{ss['pneg']:>7.0f}% | "
                  f"{sj['mean']:>6.1f}%{sj['med']:>6.1f}%{sj['p5']:>6.1f}%{sj['pgt15']:>6.0f}%{sj['pneg']:>6.0f}%")
    print("\nLectura: con motor ~10% y senior 60%@5%, el JUNIOR (40% del capital)")
    print("apalanca ~2.5x el exceso -> media >15%, pero p5 negativo = puede perder.")
    print("El senior queda casi sin riesgo (cobra ~5%, p5 cerca de 0).")
    print("\nQuién toma qué: vos ponés el JUNIOR (buscás >15%), capital 'sagrado'/")
    print("conservador va al SENIOR. O conseguís un tercero para el senior y vos")
    print("te quedás todo el junior apalancado.")

if __name__=="__main__": main()
