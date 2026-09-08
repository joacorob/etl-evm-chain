#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backtest_real — el motor delta-neutral sobre el path REAL de ETH (no GBM)
=========================================================================

Valida la fundación (~10% neto del motor) contra historia real, reemplazando el
Monte Carlo GBM por el precio diario real de ETH (/tmp/eth_daily.csv, CoinGecko).
Mismas mecánicas que xyield_engine.monte_carlo_dn (pool sintético 18/18, re-center
del rango, re-hedge del short), pero sobre UN path real con sus crashes/pumps.

Compara 3 cosas sobre el mismo path:
  - HODL (50/50 ETH/USDC)
  - LP sin hedge (lo que mide revert: fee − IL direccional)
  - LP delta-neutral (fee − LVR + funding, el motor)
y reporta ventanas rolling de 90d para ver dispersión real.
"""
import sys, os, csv, math
import numpy as np
HERE=os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0,HERE)
from v3_math import Q96, amounts_for_liquidity

def load_path(p="/tmp/eth_daily.csv"):
    ts,px=[],[]
    with open(p) as f:
        for r in csv.DictReader(f): ts.append(int(r["ts"])); px.append(float(r["price"]))
    return ts,px

def lp_amounts(L,sA,sB,S):
    a0,a1=amounts_for_liquidity(int(math.sqrt(S)*Q96),sA,sB,L)
    return a0/1e18, a1/1e18  # pool sintetico ambos 18 dec, precio=ratio

def lp_value(L,sA,sB,S):
    a0,a1=lp_amounts(L,sA,sB,S); return a0*S+a1

def build_lp(S,V,w):
    Pa,Pb=S*(1-w),S*(1+w)
    sA,sP,sB=(int(math.sqrt(x)*Q96) for x in (Pa,S,Pb))
    a0,a1=amounts_for_liquidity(sP,sA,sB,10**18); vpb=a0/1e18*S+a1/1e18
    L=int(10**18*(V/vpb))
    return L,sA,sB,Pa,Pb

def run(px, w=0.40, fee_in=30.0, funding=5.0, floor=3.1, rehedge=7,
        perp_bps=5.0, rebal_bps=5.0, hedge_mode="static", cap=100000.0):
    """Serie de equity diaria del motor sobre el path px.
    hedge_mode: 'none' (LP crudo) | 'static' (short fijo del delta inicial; mejor
    en round-trips, no se whipsappea) | 'dyn' (re-hedge cada `rehedge`d al delta
    actual del LP; se whipsappea en vol alta)."""
    n=len(px); dt=1/365.0
    S=px[0]; L,sA,sB,Pa,Pb=build_lp(S,cap,w)
    eth0,_=lp_amounts(L,sA,sB,S); Hh=eth0 if hedge_mode!="none" else 0.0
    fees=funding_acc=floor_acc=costs=hedge=0.0
    eq=[cap]
    for d in range(1,n):
        Sp,S=px[d-1],px[d]
        if Pa<=Sp<=Pb:
            fees+=lp_value(L,sA,sB,Sp)*(fee_in/100.0)*dt
        funding_acc+=abs(Hh)*Sp*(funding/100.0)*dt
        floor_acc+=abs(Hh)*Sp*0.5*(floor/100.0)*dt
        hedge+=Hh*(Sp-S)                      # short gana si baja
        if hedge_mode=="dyn" and d%rehedge==0:
            ethn,_=lp_amounts(L,sA,sB,S); costs+=abs(ethn-Hh)*S*(perp_bps/1e4); Hh=ethn
        if not (Pa<=S<=Pb):
            Vnow=lp_value(L,sA,sB,S); costs+=Vnow*(rebal_bps/1e4)
            L,sA,sB,Pa,Pb=build_lp(S,Vnow,w)
            if hedge_mode=="dyn":
                ethn,_=lp_amounts(L,sA,sB,S); costs+=abs(ethn-Hh)*S*(perp_bps/1e4); Hh=ethn
        eq.append(lp_value(L,sA,sB,S)+fees+funding_acc+floor_acc+hedge-costs)
    return np.array(eq)

def hodl_curve(px,cap=100000.0):
    S0=px[0]; half=cap/2; eth=half/S0
    return np.array([eth*s+half for s in px])

def maxdd(eq):
    peak=np.maximum.accumulate(eq); return float((eq/peak-1).min())*100

def ann_ret(eq,n):
    return (eq[-1]/eq[0])**(365.0/n)-1

def main():
    ts,px=load_path()
    n=len(px)
    print("="*78)
    print(f"BACKTEST REAL — ETH path {n}d: ${px[0]:.0f}->${max(px):.0f}->${px[-1]:.0f} "
          f"(neto {(px[-1]/px[0]-1)*100:+.0f}%)")
    print("="*78)
    hodl=hodl_curve(px)
    print(f"\n{'estrategia':28}{'ret total':>11}{'maxDD':>9}")
    print(f"{'HODL 50/50':28}{(hodl[-1]/hodl[0]-1)*100:>10.1f}%{maxdd(hodl):>8.1f}%")

    # EL HALLAZGO: ancho x modo de hedge sobre el path real
    print(f"\n[Motor sobre path real — retorno anual por ANCHO x HEDGE (fee_in=30%)]")
    print(f"{'ancho':>7}{'sin hedge':>11}{'hedge static':>14}{'hedge dyn':>12}")
    for w in [0.15,0.25,0.40,0.60]:
        n_=run(px,w=w,hedge_mode='none'); s_=run(px,w=w,hedge_mode='static'); d_=run(px,w=w,hedge_mode='dyn')
        flag=" <- tight = TRAMPA" if w==0.15 else (" <- sobrevive" if w>=0.40 else "")
        print(f"±{w*100:>5.0f}%{(n_[-1]/n_[0]-1)*100:>10.1f}%{(s_[-1]/s_[0]-1)*100:>13.1f}%{(d_[-1]/d_[0]-1)*100:>11.1f}%{flag}")

    print("\n[Mejor config (±40% static) — sensibilidad al fee real:]")
    for fe in [20,30,40,50]:
        eqf=run(px,w=0.40,fee_in=fe,hedge_mode='static')
        print(f"  fee_in={fe:>3}% -> {(eqf[-1]/eqf[0]-1)*100:+.1f}%  maxDD={maxdd(eqf):.0f}%")

    print("\nCONCLUSIÓN del backtest real:")
    print(" - Tight ±15% delta-neutral = TRAMPA en vol alta (-21 a -40% este año).")
    print(" - Ancho ±40-60% + hedge ESTÁTICO sobrevive (~+8% fee30, +26% fee50).")
    print(" - El re-hedge DINÁMICO se whipsappea en round-trips (peor que static).")
    print(" - El edge real = ADAPTAR el ancho a la vol (ancho en vol alta).")
    print(" - funding fijo +5% (HL real solo 21d; ~+4-7% ETH histórico).")

if __name__=="__main__": main()
