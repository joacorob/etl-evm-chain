#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
leverage_loop — apalancar el motor delta-neutral para forzar >15% (con ruina)
=============================================================================

Vía #3 del modelo xYield: el motor DN (ETH/USDC LP concentrado + short HL) rinde
~10% neto. Para cruzar 15% lo apalancás. Tres formas EQUIVALENTES de la misma
palanca:
  - Loop en Aave: depositás stable (collateral), pedís stable (eMode ~90% LTV),
    deployás lo prestado en el motor. Net_equity = m*motor - (m-1)*borrow.
  - Tramo junior (tranche_model.py): el senior es tu "préstamo" al cupón.
  - sUSDe/looping: muerto (carry 3.6% < borrow 5%).

CLAVE sobre el riesgo (lo que casi todos entienden mal):
  Apalancar un motor DELTA-NEUTRAL vía Aave stable/stable eMode tiene ~CERO
  riesgo de liquidación por precio (collateral y deuda son stables, LTV estable).
  El riesgo NO es liquidación Aave: es DRAWDOWN ECONÓMICO amplificado por leverage
  cuando el motor tiene un mal año. Y un riesgo operativo aparte: que el SHORT en
  Hyperliquid se liquide si ETH pumpea y no tenés margen (mitigable con perp a
  bajo leverage; el LP gana cuando ETH sube y compensa).

Datos reales (jun-2026): Aave USDC supply ~3.1%, borrow ~5% (eMode stable ~90% LTV),
Ethena sUSDe carry 3.6% (confirma funding comprimido), motor DN ~10% net.
"""
import argparse
import numpy as np

def engine_year_samples(n, rng, mean=10.0, good_std=8.0, bad_prob=0.15,
                        bad_mean=-15.0, bad_std=10.0):
    """Retorno anual del motor con cola izquierda realista (mezcla: años normales
    + años malos por spike de vol/depeg/slippage del hedge). Calibrado a la data:
    media ~10%, p5 ~ -8 a -28%."""
    bad = rng.random(n) < bad_prob
    good_mean = (mean - bad_prob*bad_mean) / (1 - bad_prob)  # para que la media total sea `mean`
    g = rng.normal(good_mean, good_std, n)
    b = rng.normal(bad_mean, bad_std, n)
    return np.where(bad, b, g) / 100.0

def lever_equity_return(R_engine, m, borrow=5.0, supply=3.1):
    """Net sobre equity de apalancar el motor m veces.
    m=1 -> motor puro. La parte prestada (m-1) paga borrow; el equity propio
    deployado gana motor; (no modelo el supply del collateral por separado para
    no inflar: caso conservador = todo el equity va al motor)."""
    return m * R_engine - (m - 1.0) * (borrow/100.0)

def multiyear(rng, m, years=5, n_paths=40000, **eng):
    """Compone `years` años; devuelve CAGR y max drawdown por path."""
    borrow = eng.pop("borrow", 5.0)
    cagr = np.zeros(n_paths); maxdd = np.zeros(n_paths); ruin = np.zeros(n_paths, bool)
    for k in range(n_paths):
        Rs = engine_year_samples(years, rng, **eng)
        eq = 1.0; peak = 1.0; dd = 0.0
        for r in Rs:
            yr = lever_equity_return(np.array([r]), m, borrow)[0]
            eq *= (1 + yr)
            if eq <= 0: eq = 1e-9; ruin[k] = True
            peak = max(peak, eq); dd = min(dd, eq/peak - 1)
        cagr[k] = eq**(1/years) - 1
        maxdd[k] = dd
    return cagr, maxdd, ruin

def q(x, p): return float(np.percentile(x, p))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine-mean", type=float, default=10.0)
    ap.add_argument("--borrow", type=float, default=5.0)
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--paths", type=int, default=30000)
    args = ap.parse_args()

    rng = np.random.default_rng(7)
    print("="*82)
    print(f"APALANCAR EL MOTOR DN (media {args.engine_mean}%, borrow {args.borrow}%, "
          f"horizonte {args.years}a)")
    print("="*82)
    # 1 año: distribución del retorno de equity por leverage
    print("\n[1 AÑO] retorno sobre equity por leverage m:")
    print(f"{'m':>4}{'media':>8}{'mediana':>9}{'p5':>8}{'p25':>8}{'P(>15%)':>9}{'P(<0)':>8}")
    R1 = engine_year_samples(200000, rng, mean=args.engine_mean)
    for m in [1.0,1.5,2.0,2.5,3.0,4.0]:
        eq = lever_equity_return(R1, m, args.borrow)
        print(f"{m:>4.1f}{np.mean(eq)*100:>7.1f}%{np.median(eq)*100:>8.1f}%"
              f"{q(eq,5)*100:>7.1f}%{q(eq,25)*100:>7.1f}%{np.mean(eq>0.15)*100:>8.0f}%{np.mean(eq<0)*100:>7.0f}%")

    # multi-año: CAGR + drawdown + ruina
    print(f"\n[{args.years} AÑOS compuestos] lo que importa de verdad (drawdown/ruina):")
    print(f"{'m':>4}{'CAGR med':>10}{'CAGR p5':>9}{'maxDD med':>10}{'maxDD p5':>10}{'P(DD>50%)':>11}{'P(ruina)':>10}")
    for m in [1.0,1.5,2.0,2.5,3.0,4.0]:
        cagr,maxdd,ruin = multiyear(rng, m, years=args.years, n_paths=args.paths,
                                    mean=args.engine_mean, borrow=args.borrow)
        print(f"{m:>4.1f}{np.median(cagr)*100:>9.1f}%{q(cagr,5)*100:>8.1f}%"
              f"{np.median(maxdd)*100:>9.1f}%{q(maxdd,5)*100:>9.1f}%"
              f"{np.mean(maxdd<-0.5)*100:>10.0f}%{np.mean(ruin)*100:>9.1f}%")

    print("\nLectura:")
    print(" - m~2.0-2.5 lleva la mediana a 15-18% con CAGR p5 todavía positivo o leve")
    print("   negativo. m>=3 cruza 20% media PERO el p5 y el drawdown se vuelven brutales.")
    print(" - 'Sweet spot' = leverage donde P(>15%) sube fuerte sin que P(DD>50%) explote.")
    print(" - Riesgo NO es liquidación Aave (stable/stable) sino drawdown del motor x m.")
    print("   Vigilá el margen del short en HL (liquidación del hedge si ETH pumpea).")

if __name__ == "__main__":
    main()
