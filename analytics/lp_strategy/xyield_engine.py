#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xYield engine — "expected goals" para yield en stables
======================================================

Idea (analogía xG del fútbol):
  El APR de cabecera (headline) es como contar sólo los goles: ruidoso y sesgado
  por el resultado. Una posición puede mostrar 60% APR porque el precio se quedó
  quieto dentro del rango (gol con suerte), mientras su *valor esperado* es pobre.
  Acá calculamos el **xYield** = APR neto ESPERADO, descompuesto en sus drivers
  estructurales y repetibles, y lo separamos del APR *realizado* (que incluye la
  suerte del path de precio) vía Monte Carlo.

  xYield = xFee  +  xFunding  +  xFloor  −  xIL_neto  −  costos

Sleeves del portafolio:
  A) Piso        : Aave stable supply (USDC). Bajo riesgo. Ancla la base.
  B) LP estable  : USDC/USDT concentrado. Fee bajo hoy (~0.3-5%), poco peso.
  C) Motor α     : LP de ETH/USDC concentrado (fees reales) + SHORT ETH en
                   Hyperliquid que cobra funding (+~11%/año hoy). Delta-neutral.

Fuentes de datos:
  - Fee REAL del pool WETH/USDC 0.3% Base  -> data/base/WETH_USDC_3000_7d.csv
    (reutiliza lp_simulator + v3_math, ya validados).
  - Funding ETH en vivo  -> Hyperliquid info API (metaAndAssetCtxs).
  - Aave supply APY      -> DefiLlama yields API.

Uso:
  python analytics/lp_strategy/xyield_engine.py            # live + 7d data
  python analytics/lp_strategy/xyield_engine.py --offline  # sin red (usa defaults)
"""
from __future__ import annotations
import os, sys, json, math, argparse, urllib.request
from dataclasses import dataclass

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from v3_math import (Q96, tick_to_sqrt_price_x96, amounts_for_liquidity,
                     open_position, price_to_tick)
from lp_simulator import load_swaps, simulate, Strategy, StrategyState

DATA_3000 = os.path.join(HERE, "..", "..", "data", "base", "WETH_USDC_3000_7d.csv")

# ───────────────────────── live data fetchers ─────────────────────────

def _post_json(url, payload, timeout=12):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def _get_json(url, timeout=20):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())

def fetch_hl_funding(coin="ETH"):
    """Annualized funding for `coin` on Hyperliquid. Positive => SHORT cobra."""
    d = _post_json("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"})
    uni, ctxs = d[0]["universe"], d[1]
    for i, u in enumerate(uni):
        if u["name"] == coin:
            hourly = float(ctxs[i]["funding"])
            return {"mark": float(ctxs[i]["markPx"]),
                    "funding_hourly": hourly,
                    "funding_annual": hourly * 24 * 365 * 100}  # en PORCENTAJE
    raise RuntimeError(f"{coin} no encontrado en HL")

def fetch_aave_usdc(chain="Base"):
    d = _get_json("https://yields.llama.fi/pools")["data"]
    best = None
    for p in d:
        if p["project"] == "aave-v3" and p["chain"] == chain and p["symbol"] == "USDC":
            if best is None or p["tvlUsd"] > best["tvlUsd"]:
                best = p
    if best is None:
        return {"apy": 3.13, "chain": chain}
    return {"apy": best.get("apy", 0.0), "base": best.get("apyBase", 0.0),
            "reward": best.get("apyReward") or 0.0, "tvl": best["tvlUsd"], "chain": chain}

# ───────────────────────── empirical fee engine ───────────────────────

class StaticRange(Strategy):
    """Abre ±W% una vez y mantiene (pasivo). Si --recenter, recentra al salir."""
    name = "static"
    def __init__(self, half_width_pct, recenter=False, min_gap_s=6*3600):
        self.w = half_width_pct / 100.0
        self.recenter = recenter
        self.min_gap_s = min_gap_s
    def decide(self, row, state: StrategyState):
        price = (row.sqrtPriceX96 / Q96) ** 2  # t1/t0 atomic
        tick = row.tick
        if state.position is None:
            return self._range(price)
        if self.recenter and not state.position.in_range(tick):
            if row.timestamp - state.last_rebalance_ts >= self.min_gap_s:
                return self._range(price)
        return None
    def _range(self, price_atomic):
        # rango simétrico en precio: [(1-w)P, (1+w)P]
        import math as _m
        cur_tick = int(_m.floor(_m.log(price_atomic) / _m.log(1.0001)))
        dtick = int(round(_m.log(1 + self.w) / _m.log(1.0001)))
        return (cur_tick - dtick, cur_tick + dtick)

def empirical_fee_apr(df, widths_pct, fee_pips=3000, capital=100_000, recenter=False):
    days = (df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]) / 86400.0
    out = {}
    for w in widths_pct:
        res = simulate(df, StaticRange(w, recenter=recenter),
                       initial_capital_t1=capital, dec0=18, dec1=6,
                       fee_pips=fee_pips, rebalance_cost_bps=5.0)
        fee_apr = res.fees_t1 / capital / days * 365 * 100
        out[w] = {"fee_apr": fee_apr, "in_range": res.in_range_frac,
                  "rebal": res.n_rebalances, "days": days}
    return out

def realized_vol_annual(df, bar="5min"):
    """Vol anualizada sobre precio resampleado a grilla fija (evita el ruido de
    microestructura que cancela y subestima en returns por-swap)."""
    s = pd.Series(df["price"].values.astype(float),
                  index=pd.to_datetime(df["timestamp"].values.astype("int64"), unit="s"))
    px = s.resample(bar).last().dropna()
    lr = np.diff(np.log(px.values))
    lr = lr[np.isfinite(lr)]
    if len(lr) < 5:
        return 0.55
    bars_per_year = 365 * 24 * 60 / int(bar.replace("min", ""))
    return float(np.std(lr) * math.sqrt(bars_per_year))

# ───────────────────────── Monte Carlo: xYield vs realizado ────────────

@dataclass
class DNParams:
    capital: float = 100_000.0
    half_width_pct: float = 12.0     # ancho del rango LP (±%)
    fee_apr_in_range: float = 30.0   # %/año mientras está in-range (empírico)
    funding_annual: float = 10.95    # %/año que cobra el short
    floor_apr: float = 3.13          # %/año Aave sobre el stable ocioso
    vol_annual: float = 0.55         # vol ETH anualizada
    horizon_days: int = 365
    n_paths: int = 6_000
    perp_fee_bps: float = 5.0        # ida+vuelta del perp
    rebal_cost_bps: float = 5.0
    rehedge_days: int = 7            # cada cuánto re-hedgear el delta (bot)
    recenter: bool = True           # re-centrar el rango LP al salirse
    seed: int = 7

def monte_carlo_dn(p: DNParams):
    """
    LP ETH/USDC concentrado (delta-neutral con short estático = ETH inicial del LP).
    Mide la distribución de retorno anualizado por path GBM, separando xYield
    (la media/mediana esperada) del realizado (cada path = un partido jugado).
    """
    rng = np.random.default_rng(p.seed)
    S0 = 1000.0
    w = p.half_width_pct / 100.0
    Pa, Pb = S0 * (1 - w), S0 * (1 + w)
    sqrtP = int(math.sqrt(S0) * Q96)
    sqrtA = int(math.sqrt(Pa) * Q96)
    sqrtB = int(math.sqrt(Pb) * Q96)

    # posición LP: mitad del capital al motor; abrir con value=V_lp en token1(USDC)
    V_lp = p.capital  # todo el sleeve C va al LP; el short es overlay con margen aparte
    # liquidez L tal que value≈V_lp (probe)
    Lp = 10**18
    a0p, a1p = amounts_for_liquidity(sqrtP, sqrtA, sqrtB, Lp)
    v_probe = a0p * S0 / 1e18 + a1p / 1e18  # pool sintetico: ambos 18 dec, precio = ratio directo
    L = int(Lp * (V_lp / v_probe))
    a0_0, a1_0 = amounts_for_liquidity(sqrtP, sqrtA, sqrtB, L)
    eth0 = a0_0 / 1e18            # ETH inicial en el LP -> tamaño del short
    H = eth0                      # short notional en ETH (delta-neutral t=0)

    dt = 1.0 / 365.0
    n_steps = p.horizon_days
    drift = -0.5 * p.vol_annual**2
    rets = np.zeros(p.n_paths)
    w = p.half_width_pct / 100.0

    def lp_for_value(S, Vusd):
        """Liquidez y rango ±w% centrado en S para alojar Vusd de valor."""
        Pa_, Pb_ = S*(1-w), S*(1+w)
        sA, sP, sB = (int(math.sqrt(x)*Q96) for x in (Pa_, S, Pb_))
        a0p, a1p = amounts_for_liquidity(sP, sA, sB, 10**18)
        vpb = a0p*S/1e18 + a1p/1e18
        Ln = int(10**18 * (Vusd/vpb))
        return Ln, sA, sB, Pa_, Pb_

    for k in range(p.n_paths):
        S = S0
        Lk, sA, sB, Pa_, Pb_ = L, sqrtA, sqrtB, Pa, Pb
        a0c, _ = amounts_for_liquidity(int(math.sqrt(S)*Q96), sA, sB, Lk)
        Hh = a0c/1e18                 # short ETH = ETH actual en el LP (delta-neutral t0)
        hedge_pnl = fees = funding = floor = costs = 0.0
        for d in range(n_steps):
            z = rng.standard_normal()
            S_next = S * math.exp(drift*dt + p.vol_annual*math.sqrt(dt)*z)
            # fees sólo si el precio del paso está dentro del rango vigente
            if Pa_ <= S <= Pb_:
                a0i,a1i = amounts_for_liquidity(int(math.sqrt(S)*Q96), sA, sB, Lk)
                v_in = a0i*S/1e18 + a1i/1e18
                fees += v_in*(p.fee_apr_in_range/100.0)*dt
            funding += abs(Hh)*S*(p.funding_annual/100.0)*dt
            floor   += (abs(Hh)*S*0.5)*(p.floor_apr/100.0)*dt
            # PnL del short mark-to-market del paso (gana si baja)
            hedge_pnl += Hh*(S - S_next)
            # re-hedge periódico: Hh <- delta ETH actual del LP
            if (d+1) % p.rehedge_days == 0:
                a0n,_ = amounts_for_liquidity(int(math.sqrt(S_next)*Q96), sA, sB, Lk)
                eth_lp = a0n/1e18
                costs += abs(eth_lp-Hh)*S_next*(p.perp_fee_bps/10000.0)
                Hh = eth_lp
            # re-centrar rango si se salió
            if p.recenter and not (Pa_ <= S_next <= Pb_):
                a0n,a1n = amounts_for_liquidity(int(math.sqrt(S_next)*Q96), sA, sB, Lk)
                Vnow = a0n*S_next/1e18 + a1n/1e18
                costs += Vnow*(p.rebal_cost_bps/10000.0)
                Lk, sA, sB, Pa_, Pb_ = lp_for_value(S_next, Vnow)
                a0r,_ = amounts_for_liquidity(int(math.sqrt(S_next)*Q96), sA, sB, Lk)
                costs += abs(a0r/1e18-Hh)*S_next*(p.perp_fee_bps/10000.0)
                Hh = a0r/1e18
            S = S_next
        sP = int(math.sqrt(S)*Q96)
        a0e, a1e = amounts_for_liquidity(sP, sA, sB, Lk)
        V_lp_end = a0e*S/1e18 + a1e/1e18
        equity_end = V_lp_end + fees + funding + floor + hedge_pnl - costs
        r = equity_end/p.capital - 1.0
        rets[k] = r*(365.0/p.horizon_days)
    return rets

def summarize(rets):
    return {
        "xYield_mean": float(np.mean(rets))*100,
        "median": float(np.median(rets))*100,
        "p5": float(np.percentile(rets,5))*100,
        "p25": float(np.percentile(rets,25))*100,
        "p75": float(np.percentile(rets,75))*100,
        "p95": float(np.percentile(rets,95))*100,
        "prob_gt_15": float(np.mean(rets>0.15))*100,
        "prob_neg": float(np.mean(rets<0))*100,
    }

# ───────────────────────── main ───────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--width", type=float, default=12.0, help="±% ancho LP del motor")
    ap.add_argument("--paths", type=int, default=6000)
    ap.add_argument("--horizon", type=int, default=365)
    args = ap.parse_args()

    print("="*70)
    print("xYIELD ENGINE — ingreso pasivo en stables (lógica xG)")
    print("="*70)

    # 1) anchors de mercado
    if args.offline:
        funding = {"funding_annual": 10.95, "mark": 1683.0}
        aave = {"apy": 3.13, "chain": "Base"}
        print("[offline] usando defaults")
    else:
        try:
            funding = fetch_hl_funding("ETH")
        except Exception as e:
            print("HL falló:", e); funding = {"funding_annual": 10.95, "mark": 1683.0}
        try:
            aave = fetch_aave_usdc("Base")
        except Exception as e:
            print("Aave falló:", e); aave = {"apy": 3.13, "chain": "Base"}
    print(f"\n[MERCADO] ETH≈${funding.get('mark'):.0f}  "
          f"funding ETH HL = {funding['funding_annual']:+.2f}%/año (short cobra)  "
          f"Aave {aave['chain']} USDC = {aave['apy']:.2f}%")

    # 2) fee empírico del pool real (7d)
    vol = 0.55
    fee_tbl = {}
    if os.path.exists(DATA_3000):
        df = load_swaps(DATA_3000)
        vol = realized_vol_annual(df)
        print(f"\n[POOL WETH/USDC 0.3% Base — 7d reales] vol anual ≈ {vol*100:.0f}%")
        fee_tbl = empirical_fee_apr(df, [2,5,10,15,25], recenter=False)
        print(f"{'±W%':>5} {'fee_APR%':>9} {'in_range':>9} {'rebal':>6}")
        for w,v in fee_tbl.items():
            print(f"{w:>5} {v['fee_apr']:>9.1f} {v['in_range']*100:>8.0f}% {v['rebal']:>6}")
    else:
        print("\n[POOL] no encontré el CSV 7d; usando fee_apr default")

    # fee in-range para el ancho elegido (interpolar de la tabla)
    if fee_tbl and args.width in fee_tbl:
        fee_apr_in_range = fee_tbl[args.width]["fee_apr"]
    elif fee_tbl:
        ws = sorted(fee_tbl);
        fee_apr_in_range = float(np.interp(args.width, ws, [fee_tbl[w]["fee_apr"] for w in ws]))
    else:
        fee_apr_in_range = 30.0

    # 3) Monte Carlo del motor delta-neutral — 2 regímenes de fee
    #    El fee in-range de 7d es spot/semana-quieta ("gol con suerte"): lo
    #    haircuteamos para reconciliar con el realizado de vida (~10-14% activo).
    print(f"\n[MOTOR α — LP ETH/USDC ±{args.width:.0f}% + short HL, {args.horizon}d, "
          f"{args.paths} paths, vol={vol*100:.0f}%]")
    scen = {}
    for tag, hc in [("conservador", 0.5), ("optimista", 1.0)]:
        p = DNParams(half_width_pct=args.width, fee_apr_in_range=fee_apr_in_range*hc,
                     funding_annual=funding["funding_annual"], floor_apr=aave["apy"],
                     vol_annual=vol, horizon_days=args.horizon, n_paths=args.paths)
        s = summarize(monte_carlo_dn(p))
        scen[tag] = s
        print(f"  [{tag:11} fee_in_range={fee_apr_in_range*hc:4.0f}%]  "
              f"xYield={s['xYield_mean']:+5.1f}%  mediana={s['median']:+5.1f}%  "
              f"p5..p95=[{s['p5']:+.0f}..{s['p95']:+.0f}]  "
              f"P(>15%)={s['prob_gt_15']:.0f}%  P(<0)={s['prob_neg']:.0f}%")

    # 4) portafolio mixto -> target >15% (usa el escenario CONSERVADOR)
    floor_apr = aave["apy"]
    stable_lp = 4.0   # USDC/USDT concentrado, optimista
    motor = scen["conservador"]["xYield_mean"]
    print(f"\n[PORTAFOLIO — motor conservador {motor:.0f}%, piso {floor_apr:.1f}%, LP-stable {stable_lp:.0f}%]")
    for wA,wB,wC in [(0.50,0.10,0.40),(0.40,0.10,0.50),(0.30,0.10,0.60)]:
        blend = wA*floor_apr + wB*stable_lp + wC*motor
        flag = "  <-- >15% ✓" if blend >= 15 else ""
        print(f"  mezcla {int(wA*100)}/{int(wB*100)}/{int(wC*100)} (piso/LPstable/motor) "
              f"-> xYield portafolio = {blend:.1f}%/año{flag}")

    print("\nNota xG: la media es el xYield (lo repetible). Cada path es un partido;")
    print("el p5 es tu mala racha. Dimensioná por xYield, no por el último realizado.")

if __name__ == "__main__":
    main()
