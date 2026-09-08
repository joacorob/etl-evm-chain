#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backtest_12mo — "¿cuánto tendría hoy?" con $5k USDC + $5k ETH hace 12 meses
===========================================================================

Aplica la estrategia GANADORA del estudio (ancho vol-adaptive ∝ σ reciente, clip
±10-40%, re-center real + short estático delta-neutral) sobre el path REAL de ETH
de los últimos 365 días, partiendo de $10k ($5k USDC + $5k ETH al precio de T-12m).

Compara contra HODL (50/50, all-ETH, all-USDC) y contra LP sin hedge. Reporta el
valor final en dólares y hace sensibilidad al fee real (lo único incierto del modelo).

Mecánica = la misma de backtest_real / xrange_optimizer (LP value v3, fee solo in-range
escalado por concentración η, IL real del path, PnL del short, funding).
Honesto: fee sintético, sin slippage/competencia de LPs, 1 activo, 1 régimen (bajista).
"""
import os, sys, csv, math, datetime
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
from build_dataset import rv
from backtest_real import build_lp, lp_value, lp_amounts

ETH = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data", "eth_hourly.csv"))
H = 168
W_REF = 0.40
CAP = 10_000.0           # $5k USDC + $5k ETH


def cap_eff(w):
    r = (1.0 - w) / (1.0 + w)
    return 1.0 / (1.0 - r ** 0.25)


ETA_REF = cap_eff(W_REF)


def load():
    ts, px = [], []
    with open(ETH) as f:
        for r in csv.DictReader(f):
            ts.append(int(r["ts"])); px.append(float(r["price"]))
    return np.array(ts), np.array(px)


def vol_width(ret, t, k=2.0):
    sig = rv(ret, t, 168) * math.sqrt(H)             # σ esperada a 7d
    if math.isnan(sig):
        sig = 0.15
    return float(np.clip(k * sig, 0.10, 0.40))


def run(px, ret, i0, i1, width_fn, fee_ref=0.30, funding_apr=0.05, hedge_mode="static",
        rebal_bps=5.0, perp_bps=5.0):
    """Equity en dólares. width_fn(t)->w. Re-decide ancho cada H; re-centra al salir de
    rango o en cada boundary (paga rebal). hedge_mode:
      'none'    = LP crudo (fees + IL).
      'static'  = short del delta inicial sostenido todo el período (gana en trend, no
                  se whipsapea; lo que backtest_real encontró mejor).
      'dynamic' = re-hedge al delta actual en cada re-center (se whipsapea en round-trips).
    """
    dt = 1.0 / 8760.0
    S = px[i0]; w = width_fn(i0)
    feeapr = fee_ref * cap_eff(w) / ETA_REF
    L, sA, sB, Pa, Pb = build_lp(S, CAP, w)
    Hh = lp_amounts(L, sA, sB, S)[0] if hedge_mode != "none" else 0.0
    fees = fund = hpnl = costs = 0.0; rc = 0
    for t in range(i0 + 1, i1):
        Sp, Sc = px[t - 1], px[t]
        if Pa <= Sp <= Pb:
            fees += lp_value(L, sA, sB, Sp) * feeapr * dt
        fund += abs(Hh) * Sp * funding_apr * dt
        hpnl += Hh * (Sp - Sc)
        if (not (Pa <= Sc <= Pb)) or ((t - i0) % H == 0):
            posval = lp_value(L, sA, sB, Sc)
            costs += posval * (rebal_bps / 1e4)
            if (t - i0) % H == 0:
                w = width_fn(t)
            feeapr = fee_ref * cap_eff(w) / ETA_REF
            L, sA, sB, Pa, Pb = build_lp(Sc, posval, w)
            if hedge_mode == "dynamic":                       # solo dyn re-hedgea
                ethn = lp_amounts(L, sA, sB, Sc)[0]
                costs += abs(ethn - Hh) * Sc * (perp_bps / 1e4); Hh = ethn
            rc += 1
    val = lp_value(L, sA, sB, px[i1 - 1]) + fees + hpnl - fund - costs
    return val, rc, fees, hpnl, fund, costs


def main():
    ts, px = load()
    ret = np.diff(np.log(px))
    i0 = len(px) - 365 * 24
    i1 = len(px)
    S0, Snow = px[i0], px[i1 - 1]
    d = lambda t: datetime.datetime.fromtimestamp(int(t), datetime.timezone.utc).strftime("%Y-%m-%d")

    print("=" * 78)
    print(f"12 MESES: {d(ts[i0])} (${S0:.0f}) -> {d(ts[i1-1])} (${Snow:.0f})  ETH {(Snow/S0-1)*100:+.1f}%")
    print(f"capital inicial: ${CAP:,.0f}  (${CAP/2:,.0f} USDC + ${CAP/2:,.0f} ETH = {CAP/2/S0:.3f} ETH)")
    print("=" * 78)

    # --- benchmarks (sin hacer nada) ---
    eth_amt = (CAP / 2) / S0
    hodl_5050 = CAP / 2 + eth_amt * Snow
    hodl_eth = (CAP / S0) * Snow
    print("\nBENCHMARKS (no hacer nada):")
    print(f"  todo en USDC          : ${CAP:,.0f}   ({0:+.1f}%)")
    print(f"  HODL 50/50 (5k+5k ETH): ${hodl_5050:,.0f}   ({(hodl_5050/CAP-1)*100:+.1f}%)")
    print(f"  HODL todo ETH         : ${hodl_eth:,.0f}   ({(hodl_eth/CAP-1)*100:+.1f}%)")

    # --- estrategia, fee 30% base, por modo de hedge ---
    print("\nESTRATEGIA vol-adaptive (ancho∝σ, clip ±10-40%, fee 30%@±40%, funding 5%):")
    for hm, lbl in [("none", "LP sin hedge (solo fees+IL)"),
                    ("static", "LP + short ESTÁTICO (delta-neutral)"),
                    ("dynamic", "LP + re-hedge dinámico")]:
        val, rc, fees, hpnl, fund, costs = run(px, ret, i0, i1, lambda t: vol_width(ret, t), hedge_mode=hm)
        print(f"  {lbl:36}: ${val:,.0f}  ({(val/CAP-1)*100:+.1f}%)  "
              f"[fees +${fees:,.0f} | hedge {hpnl:+,.0f} | fund -${fund:,.0f} | costs -${costs:,.0f} | {rc} rc]")

    # --- sensibilidad al fee real (lo unico incierto) ---
    print("\nSENSIBILIDAD AL FEE REAL (short estático; el fee@±40% depende de pool/volumen):")
    print(f"  {'fee@±40%':>10}{'valor final':>14}{'retorno':>10}")
    for fe in [0.10, 0.15, 0.20, 0.30, 0.50]:
        val, *_ = run(px, ret, i0, i1, lambda t: vol_width(ret, t), fee_ref=fe, hedge_mode="static")
        flag = "  <- ETH/USDC real ~aca" if fe in (0.10, 0.15) else ""
        print(f"  {fe*100:>8.0f}% {val:>13,.0f} {(val/CAP-1)*100:>+9.1f}%{flag}")

    print("\nNOTA: funding tratado como COSTO (conservador). Un short en perp con funding")
    print("positivo (longs pagan) lo COBRARÍA -> upside extra no contado acá.")


if __name__ == "__main__":
    main()
