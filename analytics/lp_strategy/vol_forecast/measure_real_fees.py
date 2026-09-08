#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
measure_real_fees — fee APR REAL de los swaps on-chain (no más asumir el 30%)
=============================================================================

Usa data/USDC_ETH_Pool_Swap.csv = logs reales del pool USDC/WETH 0.05% de mainnet
(0x88e6A0c2..., el de más volumen). Cada swap trae: amount0 (USDC 6dec), amount1
(WETH 18dec), `liquidity` (L activa TOTAL del pool en ese swap), sqrtPriceX96, ts.

Para una posición de $V a ancho ±w, centrada al precio de cada swap (re-centrada =
siempre in-range, que es el fee DENSITY in-range que aplica el backtest):
  - mi liquidez L_mía (mismas unidades raw que el pool) vía amounts_for_liquidity
  - mi share del fee del swap = L_mía / (L_total + L_mía)
  - fee del swap = fee_tier(0.05%) · volumen_usd  (|amount0|/1e6 = USD tradeado)
  - mi fee = fee_swap · share
fee APR real(w) = Σ mis_fees / V · (año / span). SIN asumir nada.

Esto reemplaza el FEE_REF sintético del backtest por el número medido.
"""
import os, sys, csv, math
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from v3_math import amounts_for_liquidity

SWAPS = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data", "USDC_ETH_Pool_Swap.csv"))
Q96 = 2 ** 96
FEE_TIER = 0.0005        # pool 0.05%
DEC0, DEC1 = 6, 18       # token0=USDC, token1=WETH
V = 10_000.0
WIDTHS = [0.075, 0.10, 0.15, 0.25, 0.40]


def load():
    rows = []
    with open(SWAPS) as f:
        for r in csv.DictReader(f):
            try:
                rows.append((int(r["timestamp"]), abs(int(r["amount0"])), int(r["sqrtPriceX96"]),
                             int(r["liquidity"])))
            except (ValueError, KeyError):
                continue
    rows.sort()
    return rows


def price_usd(sqrtP):
    raw = (sqrtP / Q96) ** 2            # token1/token0 raw
    return 1e12 / raw if raw > 0 else 0.0   # USDC por ETH (10^(18-6))


def L_for_V(sqrtP, w, v_usd, p_usd):
    """Liquidez (unidades raw del pool) de una posición de $v_usd a ±w centrada."""
    sqrtA = int(sqrtP * math.sqrt(1 - w))
    sqrtB = int(sqrtP * math.sqrt(1 + w))
    a0, a1 = amounts_for_liquidity(sqrtP, sqrtA, sqrtB, 10 ** 18)   # raw para L=1e18
    v_unit = a0 / 10 ** DEC0 + (a1 / 10 ** DEC1) * p_usd            # USD por 1e18 de L
    if v_unit <= 0:
        return 0.0
    return 10 ** 18 * (v_usd / v_unit)


def main():
    rows = load()
    t0, t1 = rows[0][0], rows[-1][0]
    span_days = (t1 - t0) / 86400.0
    import datetime
    d = lambda t: datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%d")
    vol_total = sum(a0 for _, a0, _, _ in rows) / 10 ** DEC0
    p_now = price_usd(rows[-1][2])

    print("=" * 74)
    print(f"FEE APR REAL — pool USDC/WETH 0.05% mainnet | {len(rows)} swaps")
    print(f"span: {d(t0)} -> {d(t1)} ({span_days:.0f}d) | ETH ~${p_now:,.0f}")
    print(f"volumen total: ${vol_total/1e6:,.1f}M  (${vol_total/span_days/1e6:,.1f}M/día)")
    print("=" * 74)

    year = 365.0 / span_days
    print(f"\nfee APR REAL para una posición de ${V:,.0f} (re-centrada, in-range):")
    print(f"  {'ancho':>8}{'fee APR':>12}{'$ fees/año':>14}{'η vs ±40%':>12}")
    base40 = None
    results = {}
    for w in WIDTHS:
        tot = 0.0
        for ts, a0, sqrtP, Ltot in rows:
            p = price_usd(sqrtP)
            vol_usd = a0 / 10 ** DEC0
            Lme = L_for_V(sqrtP, w, V, p)
            share = Lme / (Ltot + Lme)
            tot += FEE_TIER * vol_usd * share
        apr = tot / V * year
        results[w] = apr
        if w == 0.40:
            base40 = apr
        print(f"  ±{w*100:>5.0f}%{apr*100:>11.1f}%{tot*year:>13,.0f}{'':>4}", end="")
        print(f"{(apr/base40):>10.1f}x" if base40 else "")
    print(f"\n>>> FEE REAL @±40% = {results[0.40]*100:.1f}% APR   "
          f"(vs el 30% que asumí en backtest_12mo)")
    print(f">>> a ±10-15% (lo que usa la estrategia): {results[0.10]*100:.0f}-{results[0.15]*100:.0f}% APR real")
    print("\nNOTA: 'siempre in-range' = techo (asume re-center perfecto). En el path real")
    print("estás out-of-range parte del tiempo -> el backtest lo descuenta por separado.")


if __name__ == "__main__":
    main()
