#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backtest_adaptive — ¿el ancho adaptado a la vol le gana al IL? (+18 meses reales)
=================================================================================

Walk-forward sobre eth_hourly.csv (Binance, 18 meses). Cada H horas re-centramos
una posicion Uni v3 y la mantenemos H horas. Comparamos:

  HODL          : 50/50 ETH/USDC, sin LP
  tight  (fijo) : ancho fijo angosto
  wide   (fijo) : ancho fijo amplio
  adaptive      : ancho = clamp(z * sigma_forecast, wmin, wmax)  <- la tesis

Modelo de fees (la pieza que faltaba en backtest_real, que usaba fee fijo sin
importar el ancho): una posicion mas concentrada captura mas share de la liquidez
activa, asi que el fee APR escala ~1/w mientras esta IN-RANGE:

    fee_apr(w) = FEE_REF_APR * (W_REF / w)      (capeado en FEE_CAP)

Esto crea la tension real: tight = mas fees pero se sale de rango antes (deja de
cobrar y cristaliza IL al re-centrar). El edge de 'adaptive' es ir tight en calma
y wide en vol alta. fee solo se acumula mientras Pa<=S<=Pb.

IL se mide como (valor LP - valor HODL): si el neto LP supera HODL, le ganamos al IL.
"""
import os, sys, math, argparse
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from v3_math import Q96, amounts_for_liquidity
from vol_forecast import load_hourly, ewma_sigma, realized_sigma

# --- supuestos del motor de fees (parametrizables / barridos abajo) ---
FEE_REF_APR = 0.08    # APR de fees de una posicion de referencia W_REF
W_REF = 0.50          # ancho de referencia (±50%)
FEE_CAP = 4.0         # tope de fee APR (400%) para posiciones ultra-tight
REBAL_BPS = 5.0       # costo de re-centrar (swap+gas) en bps del notional


def lp_amounts(L, sA, sB, S):
    a0, a1 = amounts_for_liquidity(int(math.sqrt(S) * Q96), sA, sB, L)
    return a0 / 1e18, a1 / 1e18


def lp_value(L, sA, sB, S):
    a0, a1 = lp_amounts(L, sA, sB, S)
    return a0 * S + a1


def build_lp(S, V, w):
    """Posicion de valor V centrada en S, semiancho w (multiplicativo en log)."""
    Pa, Pb = S * math.exp(-w), S * math.exp(+w)
    sA, sP, sB = (int(math.sqrt(x) * Q96) for x in (Pa, S, Pb))
    a0, a1 = amounts_for_liquidity(sP, sA, sB, 10 ** 18)
    vpb = a0 / 1e18 * S + a1 / 1e18
    L = int(10 ** 18 * (V / vpb))
    return L, sA, sB, Pa, Pb


def fee_apr(w):
    return min(FEE_CAP, FEE_REF_APR * (W_REF / max(w, 1e-4)))


def width_for(strategy, ts_sigma, z, wmin, wmax, wtight, wwide):
    if strategy == "tight":
        return wtight
    if strategy == "wide":
        return wwide
    if strategy == "adaptive":
        return float(np.clip(z * ts_sigma, wmin, wmax))
    raise ValueError(strategy)


def run_strategy(px, ret, rebal_idx, strategy, H, z, wmin, wmax, wtight, wwide,
                 cap=100_000.0, sigma_source="ewma"):
    """Encadena ventanas: en cada rebal_idx re-centra; mantiene H horas.
    Devuelve dict con equity final, % tiempo in-range, fees, IL_vs_hodl, ancho medio."""
    dt = 1.0 / (365.0 * 24.0)   # paso horario en anios
    V = cap
    fees_report = costs_report = 0.0   # acumuladores SOLO para reporte
    in_range_hrs = total_hrs = 0
    widths = []
    eq_curve = [cap]
    for k, i in enumerate(rebal_idx):
        S0 = px[i]
        # forecast de sigma de la ventana usando SOLO info hasta i
        if sigma_source == "ewma":
            sig_h = ewma_sigma(ret, i)
        else:
            sig_h = realized_sigma(ret, i, lookback=H)
        sig_win = sig_h * math.sqrt(H)
        w = width_for(strategy, sig_win, z, wmin, wmax, wtight, wwide)
        widths.append(w)
        # costo de re-centrar (no en la primera apertura), descontado del capital
        cost = V * (REBAL_BPS / 1e4) if k > 0 else 0.0
        V -= cost
        costs_report += cost
        L, sA, sB, Pa, Pb = build_lp(S0, V, w)
        # simular la ventana hora a hora; las fees de ESTA ventana se realizan al cierre
        end = min(i + H, len(px) - 1)
        far = fee_apr(w)
        fees_win = 0.0
        for j in range(i + 1, end + 1):
            S = px[j]
            total_hrs += 1
            if Pa <= S <= Pb:
                fees_win += lp_value(L, sA, sB, S) * far * dt
                in_range_hrs += 1
        # equity al cierre = posicion + fees de la ventana (se compone en la prox)
        V = lp_value(L, sA, sB, px[end]) + fees_win
        fees_report += fees_win
        eq_curve.append(V)
    # HODL sobre el mismo span
    i0, iN = rebal_idx[0], min(rebal_idx[-1] + H, len(px) - 1)
    eth0 = (cap / 2) / px[i0]
    hodl_end = eth0 * px[iN] + cap / 2
    return dict(
        equity=V, ret=V / cap - 1.0,
        hodl=hodl_end, hodl_ret=hodl_end / cap - 1.0,
        vs_hodl=V / hodl_end - 1.0,
        in_range=in_range_hrs / max(total_hrs, 1),
        fees=fees_report / cap, costs=costs_report / cap,
        wbar=float(np.mean(widths)), eq_curve=np.array(eq_curve),
    )


def maxdd(eq):
    peak = np.maximum.accumulate(eq)
    return float((eq / peak - 1).min()) * 100


def window_pnl(px, ret, i, w, H, cap=1.0):
    """PnL neto-vs-HODL de UNA posicion de ancho w sobre la ventana [i, i+H],
    aislada (capital fresco). Devuelve (net_vs_hodl, fees, in_range_frac)."""
    dt = 1.0 / (365.0 * 24.0)
    S0 = px[i]
    L, sA, sB, Pa, Pb = build_lp(S0, cap, w)
    end = min(i + H, len(px) - 1)
    far = fee_apr(w)
    fees = 0.0; inr = 0
    for j in range(i + 1, end + 1):
        S = px[j]
        if Pa <= S <= Pb:
            fees += lp_value(L, sA, sB, S) * far * dt; inr += 1
    lp_final = lp_value(L, sA, sB, px[end]) + fees
    eth0 = (cap / 2) / S0
    hodl = eth0 * px[end] + cap / 2
    return lp_final / hodl - 1.0, fees / cap, inr / max(end - i, 1)


def regime_analysis(px, ret, rebal_idx, H, wtight, wwide):
    """¿El forecast de vol ex-ante dice cuando conviene tight vs wide?
    Por ventana mide net-vs-HODL de tight y de wide (aislados), y los cruza con
    el sigma forecasteado (EWMA, conocido ex-ante) y el realizado."""
    fc_sig, rz_sig, tight_adv, t_net, w_net = [], [], [], [], []
    for i in rebal_idx:
        sig_f = ewma_sigma(ret, i) * math.sqrt(H)
        sig_r = realized_sigma(ret, i, lookback=0) if False else math.sqrt(np.sum(ret[i:min(i+H,len(ret))] ** 2))
        nt, _, _ = window_pnl(px, ret, i, wtight, H)
        nw, _, _ = window_pnl(px, ret, i, wwide, H)
        fc_sig.append(sig_f); rz_sig.append(sig_r)
        t_net.append(nt); w_net.append(nw); tight_adv.append(nt - nw)
    fc_sig, rz_sig = np.array(fc_sig), np.array(rz_sig)
    t_net, w_net, tight_adv = np.array(t_net), np.array(w_net), np.array(tight_adv)

    print("\n" + "=" * 82)
    print("ANALISIS DE REGIMEN — ¿el forecast de vol decide tight-vs-wide? (ventanas aisladas)")
    print("=" * 82)
    c_fc = np.corrcoef(fc_sig, tight_adv)[0, 1]
    c_rz = np.corrcoef(rz_sig, tight_adv)[0, 1]
    print(f"corr(sigma_forecast, ventaja_tight) = {c_fc:+.3f}   (negativo = mas vol -> tight peor, como esperamos)")
    print(f"corr(sigma_realizado, ventaja_tight) = {c_rz:+.3f}   (techo con foresight perfecto)")

    # regla de decision: tight si forecast < mediana, wide si no
    thr = np.median(fc_sig)
    pick_tight = fc_sig < thr
    decision_net = np.where(pick_tight, t_net, w_net)
    oracle_net = np.where(rz_sig < np.median(rz_sig), t_net, w_net)  # foresight
    # oracle real: elegir por ventana la mejor de las dos
    oracle_best = np.maximum(t_net, w_net)
    print(f"\nnet-vs-HODL PROMEDIO por ventana (aislado, {len(t_net)} ventanas):")
    print(f"  siempre tight        : {t_net.mean()*100:+.2f}%")
    print(f"  siempre wide         : {w_net.mean()*100:+.2f}%")
    print(f"  decision por FORECAST: {decision_net.mean()*100:+.2f}%   <- usa solo info ex-ante")
    print(f"  decision por oraculo : {oracle_net.mean()*100:+.2f}%   (sigma realizado, no operable)")
    print(f"  best-per-window      : {oracle_best.mean()*100:+.2f}%   (techo absoluto)")
    edge = decision_net.mean() - max(t_net.mean(), w_net.mean())
    print(f"\n  edge del forecast sobre la mejor fija: {edge*100:+.2f} pp/ventana")
    print(f"  => el forecast {'APORTA' if edge > 0 else 'NO aporta'} para decidir el ancho.")
    return dict(corr_fc=c_fc, corr_rz=c_rz, decision=decision_net.mean(),
                tight=t_net.mean(), wide=w_net.mean(), oracle=oracle_best.mean())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--H", type=int, default=168, help="horas por ventana (168=7d)")
    ap.add_argument("--z", type=float, default=1.0, help="adaptive: w = z*sigma_window")
    ap.add_argument("--wmin", type=float, default=0.03)
    ap.add_argument("--wmax", type=float, default=0.60)
    ap.add_argument("--wtight", type=float, default=0.05)
    ap.add_argument("--wwide", type=float, default=0.40)
    ap.add_argument("--start-frac", type=float, default=0.10)
    args = ap.parse_args()

    ts, px, ret = load_hourly()
    n = len(px)
    i0 = int(n * args.start_frac)
    rebal_idx = list(range(i0, n - args.H, args.H))
    span_d = (ts[rebal_idx[-1] + args.H] - ts[i0]) / 86400
    print("=" * 82)
    print(f"BACKTEST ADAPTIVO — ETH {span_d:.0f}d ({span_d/30:.1f} meses), "
          f"{len(rebal_idx)} ventanas de {args.H}h")
    print(f"  precio: ${px[i0]:,.0f} -> ${px[rebal_idx[-1]+args.H]:,.0f}  "
          f"(neto {(px[rebal_idx[-1]+args.H]/px[i0]-1)*100:+.0f}%)")
    print(f"  fee model: {FEE_REF_APR:.0%} APR @ ±{W_REF:.0%}, ~1/w, cap {FEE_CAP:.0%}; rebal {REBAL_BPS:.0f}bps")
    print("=" * 82)

    strategies = [
        ("tight (fijo)", "tight"),
        ("wide  (fijo)", "wide"),
        ("adaptive", "adaptive"),
    ]
    print(f"\n{'estrategia':16}{'ret':>9}{'vs HODL':>10}{'in-range':>10}{'fees':>9}{'ancho~':>9}{'maxDD':>9}")
    rows = {}
    for label, key in strategies:
        r = run_strategy(px, ret, rebal_idx, key, args.H, args.z, args.wmin, args.wmax,
                         args.wtight, args.wwide)
        rows[key] = r
        print(f"{label:16}{r['ret']*100:>8.1f}%{r['vs_hodl']*100:>+9.1f}%"
              f"{r['in_range']*100:>9.0f}%{r['fees']*100:>8.1f}%"
              f"{r['wbar']*100:>7.0f}%{maxdd(r['eq_curve']):>8.1f}%")
    hodl_ret = rows["adaptive"]["hodl_ret"]
    print(f"{'HODL 50/50':16}{hodl_ret*100:>8.1f}%{'—':>10}{'—':>10}{'—':>9}{'—':>9}{'—':>9}")

    print("\nLECTURA:")
    a = rows["adaptive"]; t = rows["tight"]; wd = rows["wide"]
    print(f"  - 'vs HODL' > 0 => fees superan al IL (le ganamos al impermanent loss).")
    print(f"  - adaptive vs HODL: {a['vs_hodl']*100:+.1f}%  | tight: {t['vs_hodl']*100:+.1f}%  | wide: {wd['vs_hodl']*100:+.1f}%")
    winner = max(rows, key=lambda k: rows[k]["vs_hodl"])
    print(f"  - mejor neto-vs-IL: {winner}")
    print(f"  - tight in-range solo {t['in_range']*100:.0f}% del tiempo (se sale -> deja de cobrar y cristaliza IL).")

    regime_analysis(px, ret, rebal_idx, args.H, args.wtight, args.wwide)


if __name__ == "__main__":
    main()
