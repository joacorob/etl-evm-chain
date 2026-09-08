#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_feegrowth — fee APR REAL día a día / mes a mes (del acumulador on-chain)
================================================================================

Toma data/pool_feegrowth_daily.csv (de fetch_feegrowth.py) y calcula, para una
posición de $V a ancho ±w, el fee que REALMENTE cobró cada día:
  fee_día = L_mía · (Δfgg0/2^128 / 1e6  +  Δfgg1/2^128 / 1e18 · precio)
(in-range; con re-center diario y anchos ≥±10% el descuento out-of-range es chico
porque el movimiento diario de ETH << ancho).

Devuelve: serie diaria, tabla mensual, y el PROMEDIO ANUAL REAL del fee @±40% —
que es el fee_ref que va al backtest. Sin asterisco: es el dato on-chain.

Después inyecta ese fee real en backtest_12mo y reescupe el "cuánto tendría hoy".
"""
import os, sys, csv, math, datetime
from collections import defaultdict
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
from measure_real_fees import L_for_V
from v3_math import amounts_for_liquidity

CSV = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data", "pool_feegrowth_daily.csv"))
Q96 = 2 ** 96
V = 10_000.0
WIDTHS = [0.10, 0.15, 0.25, 0.40]


def load():
    rows = []
    with open(CSV) as f:
        for r in csv.DictReader(f):
            rows.append((int(r["block"]), int(r["ts"]), int(r["sqrtP"]), float(r["price_usd"]),
                         int(r["liquidity"]), int(r["fgg0"]), int(r["fgg1"])))
    rows.sort(key=lambda x: x[1])
    # dedup por ts y filtra precio sano
    seen, out = set(), []
    for x in rows:
        if x[1] in seen or x[3] <= 0:
            continue
        seen.add(x[1]); out.append(x)
    return out


def fee_apr_day(sqrtP, price, dfgg0, dfgg1, w, dt_days):
    Lme = L_for_V(sqrtP, w, V, price)
    fee = Lme * (dfgg0 / 2 ** 128) / 1e6 + Lme * (dfgg1 / 2 ** 128) / 1e18 * price
    return fee, (fee / V / dt_days * 365.0 if dt_days > 0 else 0.0)


def main():
    rows = load()
    d = lambda t: datetime.datetime.fromtimestamp(t, datetime.timezone.utc)
    print("=" * 78)
    print(f"FEE APR REAL del pool USDC/ETH 0.05% mainnet — {len(rows)} días on-chain")
    print(f"span: {d(rows[0][1]):%Y-%m-%d} -> {d(rows[-1][1]):%Y-%m-%d}")
    print("=" * 78)

    daily = []   # (ts, price, {w: apr}, {w: fee_usd})
    for i in range(1, len(rows)):
        b0, t0, s0, p0, l0, f00, f01 = rows[i - 1]
        b1, t1, s1, p1, l1, f10, f11 = rows[i]
        dt = (t1 - t0) / 86400.0
        if dt <= 0 or dt > 5:                 # salta huecos grandes
            continue
        d0 = (f10 - f00) % (2 ** 256); d1 = (f11 - f01) % (2 ** 256)
        aprs, fees = {}, {}
        for w in WIDTHS:
            fee, apr = fee_apr_day(s0, p0, d0, d1, w, dt)
            aprs[w] = apr; fees[w] = fee
        daily.append((t1, p1, aprs, fees, dt))

    # ---- tabla mensual (fee APR promedio ponderado por días) ----
    by_m = defaultdict(lambda: defaultdict(float))
    by_m_days = defaultdict(float)
    for t, p, aprs, fees, dt in daily:
        m = d(t).strftime("%Y-%m")
        by_m_days[m] += dt
        for w in WIDTHS:
            by_m[m][w] += fees[w]            # suma de fees del mes
    print(f"\n{'mes':9}{'días':>6}" + "".join(f"{'±'+str(int(w*100))+'%':>9}" for w in WIDTHS))
    for m in sorted(by_m):
        days = by_m_days[m]
        # APR del mes = fees_mes / V / días * 365
        print(f"{m:9}{days:>6.0f}" + "".join(f"{by_m[m][w]/V/days*365*100:>8.0f}%" for w in WIDTHS))

    # ---- anual real ----
    print("\n" + "-" * 50)
    tot_days = sum(dt for _, _, _, _, dt in daily)
    print(f"PROMEDIO ANUAL REAL ({tot_days:.0f} días cubiertos):")
    annual = {}
    for w in WIDTHS:
        tot_fee = sum(fees[w] for _, _, _, fees, _ in daily)
        annual[w] = tot_fee / V / tot_days * 365.0
        print(f"  fee @±{w*100:.0f}% = {annual[w]*100:.1f}% APR   (${tot_fee/tot_days*365:,.0f}/año sobre $10k)")
    print(f"\n>>> FEE_REF REAL @±40% = {annual[0.40]*100:.1f}% APR  (este es el dato sin asterisco)")

    # ---- inyectar al backtest ----
    print("\n" + "=" * 78)
    print("CUÁNTO TENDRÍA HOY — con el fee REAL medido on-chain inyectado al backtest")
    print("=" * 78)
    import backtest_12mo as B
    ts, px = B.load(); ret = np.diff(np.log(px)); i0 = len(px) - 365 * 24; i1 = len(px)
    S0, Snow = px[i0], px[i1 - 1]
    hodl = B.CAP / 2 + (B.CAP / 2 / S0) * Snow
    fe = annual[0.40]
    for hm, lbl in [("none", "LP sin hedge"), ("static", "LP + short estático")]:
        val, rc, fees, hpnl, fund, costs = B.run(px, ret, i0, i1, lambda t: B.vol_width(ret, t),
                                                 fee_ref=fe, hedge_mode=hm)
        print(f"  {lbl:22}: ${val:,.0f}  ({(val/B.CAP-1)*100:+.1f}%)  [fees +${fees:,.0f} | hedge {hpnl:+,.0f}]")
    print(f"  {'benchmarks':22}: USDC $10,000 | HODL 50/50 ${hodl:,.0f} | ETH {(Snow/S0-1)*100:+.0f}%")


if __name__ == "__main__":
    main()
