#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest mean-reversion para stables triangulados (DAI/USDC/USDT) en Uniswap v3.

Cambios:
- FIX split_pair: parsea tokens reales (DAI, USDC, USDT) para evitar 'USD'/'TUSDC'.
- Nuevo --band-steps: banda expresada en múltiplos de tickSpacing (preferible al --band-ticks).
- Chequeos y logs mínimos.

Estrategia (resumen):
- Para cada fee y par directo:
  * Precio directo P_direct = 1.0001**tick.
  * Precio implícito por triangulación con los otros dos pares.
  * Spread y z-score (rolling).
  * Entra si |z| >= z_entry. Dirección: z>0 → esperamos BAJA (banda abajo, token0-only); z<0 → SUBA (banda arriba, token1-only).
  * Sale por take |z| <= z_exit, stop |z| >= sl_z, o EoD.
  * PnL = fees estimadas: fee_rate * fee_share * volumen_en_banda (aprox).

Entradas CSV:
  ../data/DAIUSDC{100|500|3000}_Swap.csv
  ../data/DAIUSDT{100|500|3000}_Swap.csv
  ../data/USDTUSDC{100|500|3000}_Swap.csv

CSV esperado (mínimo):
  timestamp, tick, amount0, amount1  (liquidity opcional)

Salida:
  - trades_out.csv con detalle.
  - Resumen por fee/par en consola.
"""

import argparse
import os
import re
from pathlib import Path
from typing import Dict, Tuple, List

import pandas as pd
import numpy as np

PAIR_NAMES = ["DAIUSDC", "DAIUSDT", "USDTUSDC"]
FEE_MAP = {100: 0.0001, 500: 0.0005, 3000: 0.003}
TICK_SPACING = {100: 1, 500: 10, 3000: 60}  # típicos en v3

TOKENS = ["USDT", "USDC", "DAI"]  # ordenar por más largos primero
TOKEN_DECIMALS = {"DAI": 18, "USDC": 6, "USDT": 6}


def split_pair(pair: str) -> Tuple[str, str]:
    """Parsea token0, token1 desde nombres tipo 'DAIUSDC', 'USDTUSDC', etc."""
    s = pair.upper()
    for t0 in TOKENS:
        if s.startswith(t0):
            t1 = s[len(t0):]
            if t1 in TOKENS:
                return t0, t1
    raise ValueError(f"No pude parsear el par '{pair}'. Esperaba combinar {TOKENS}.")


def price_from_tick(tick: float) -> float:
    return (1.0001 ** tick)


def load_pool_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    cols = [c.lower() for c in df.columns]
    df.rename(columns=dict(zip(df.columns, cols)), inplace=True)

    if "timestamp" not in df.columns:
        raise ValueError(f"{path.name}: falta columna 'timestamp'")
    ts = df["timestamp"]
    if np.issubdtype(ts.dtype, np.number):
        dt = pd.to_datetime(ts, unit="s", utc=True)
    else:
        # ISO string o ms
        dt = pd.to_datetime(ts, utc=True, errors="coerce")
        if dt.isna().all():
            dt = pd.to_datetime(ts.astype("int64"), unit="ms", utc=True)
    df.index = dt

    if "tick" not in df.columns:
        raise ValueError(f"{path.name}: falta columna 'tick'")

    for col in ("amount0", "amount1"):
        if col not in df.columns:
            df[col] = 0.0

    keep = ["tick", "amount0", "amount1"]
    if "liquidity" in df.columns:
        keep.append("liquidity")
    df = df[keep].sort_index()
    return df


def scale_amounts_to_units(pair: str, df: pd.DataFrame) -> pd.Series:
    """Aproxima volumen USD por swap (stables ~1 USD)."""
    t0, t1 = split_pair(pair)
    try:
        d0 = TOKEN_DECIMALS[t0]
        d1 = TOKEN_DECIMALS[t1]
    except KeyError as e:
        raise KeyError(f"Falta decimals para token {e.args[0]} en TOKEN_DECIMALS") from e

    a0 = df["amount0"].astype("float64") / (10 ** d0)
    a1 = df["amount1"].astype("float64") / (10 ** d1)
    vol_usd = a0.abs() + a1.abs()
    return vol_usd


def resample_last(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    return df.resample(rule).last().ffill()


def build_price_panel(data_dir: Path, resample: str) -> Dict[int, Dict[str, pd.DataFrame]]:
    panel: Dict[int, Dict[str, pd.DataFrame]] = {100: {}, 500: {}, 3000: {}}
    # Acepta DAIUSDC500_Swap.csv o DAIUSDC_500_Swap.csv
    pat = re.compile(rf"^({'|'.join(PAIR_NAMES)})(_)?(100|500|3000)_Swap\.csv$", re.I)

    loaded = []
    for p in data_dir.glob("*_Swap.csv"):
        m = pat.match(p.name)
        if not m:
            continue
        pair = m.group(1).upper()
        fee = int(m.group(3))
        df = load_pool_csv(p)
        df["price"] = price_from_tick(df["tick"].astype("float64"))
        df["vol_usd"] = scale_amounts_to_units(pair, df)
        df_bar = resample_last(df[["tick", "price", "vol_usd"]], resample)
        panel[fee][pair] = df_bar
        loaded.append((pair, fee, len(df)))

    if not loaded:
        raise FileNotFoundError(f"No se encontraron CSV válidos en {data_dir} (nombres esperados como DAIUSDC500_Swap.csv).")
    print("[LOAD] Archivos leídos:")
    for pair, fee, n in sorted(loaded):
        print(f"  - {pair} fee={fee} rows={n}")

    return panel


def triangulated_prices(block: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    idx = None
    for pair in PAIR_NAMES:
        if pair in block:
            idx = block[pair].index if idx is None else idx.union(block[pair].index)
    if idx is None:
        return pd.DataFrame()

    def col(pair, name):
        return block[pair][name].reindex(idx).ffill() if pair in block else pd.Series(index=idx, dtype="float64")

    df = pd.DataFrame(index=idx)
    for pair in PAIR_NAMES:
        df[f"tick_{pair}"] = col(pair, "tick")
        df[f"P_{pair}"] = col(pair, "price")
        df[f"V_{pair}"] = col(pair, "vol_usd")

    # Implícitos
    df["P_DAIUSDC_via"] = df["P_DAIUSDT"] * df["P_USDTUSDC"]
    df["P_DAIUSDT_via"] = df["P_DAIUSDC"] / df["P_USDTUSDC"].replace(0, np.nan)
    df["P_USDTUSDC_via"] = df["P_DAIUSDC"] / df["P_DAIUSDT"].replace(0, np.nan)
    return df


def compute_spreads_and_z(df: pd.DataFrame, lookback_bars: int) -> pd.DataFrame:
    out = df.copy()
    minp = max(10, lookback_bars // 10)
    for pair in PAIR_NAMES:
        if f"P_{pair}" not in out or f"P_{pair}_via" not in out:
            continue
        spread = out[f"P_{pair}"] - out[f"P_{pair}_via"]
        ma = spread.rolling(lookback_bars, min_periods=minp).mean()
        sd = spread.rolling(lookback_bars, min_periods=minp).std(ddof=1)
        out[f"spread_{pair}"] = spread
        out[f"z_{pair}"] = (spread - ma) / sd.replace(0, np.nan)
    return out


def align_to_spacing(tick: float, spacing: int) -> int:
    """Alinea un tick al múltiplo inferior del spacing."""
    return int(np.floor(tick / spacing) * spacing)


def backtest_fee_block(
    fee: int,
    df: pd.DataFrame,
    z_entry: float,
    z_exit: float,
    sl_z: float,
    band_ticks_param: int,
    band_steps: int | None,
    fee_share: float,
    verbose: bool = False,
) -> pd.DataFrame:
    trades: List[dict] = []
    fee_rate = FEE_MAP[fee]
    spacing = TICK_SPACING[fee]

    for pair in PAIR_NAMES:
        need = [f"z_{pair}", f"tick_{pair}", f"P_{pair}", f"V_{pair}"]
        if any(c not in df.columns for c in need):
            continue

        in_pos = False
        cur = None

        n_rows = len(df)
        if verbose:
            print(f"[START] Backtest {pair} fee={fee} rows={n_rows}")

        for i, (t, row) in enumerate(df.iterrows()):
            if verbose and i % 5000 == 0:
                print(f"[PROGRESS] {pair} fee={fee}: {i}/{n_rows} filas procesadas...")
            z = row[f"z_{pair}"]
            tick = row[f"tick_{pair}"]

            if not in_pos:
                if pd.notna(z) and abs(z) >= z_entry:
                    direction = +1 if z < 0 else -1  # z<0: suba; z>0: baja
                    entry_tick_raw = row[f"tick_{pair}"]
                    entry_tick = align_to_spacing(entry_tick_raw, spacing)

                    # banda efectiva
                    if band_steps is not None:
                        band_ticks_eff = max(spacing, band_steps * spacing)
                    else:
                        band_ticks_eff = max(spacing, (band_ticks_param // spacing) * spacing)

                    if direction > 0:
                        lower_tick = entry_tick
                        upper_tick = entry_tick + band_ticks_eff
                    else:
                        lower_tick = entry_tick - band_ticks_eff
                        upper_tick = entry_tick

                    cur = {
                        "fee": fee,
                        "pair": pair,
                        "dir": direction,
                        "entry_time": t,
                        "entry_tick": int(entry_tick),
                        "band_ticks": int(band_ticks_eff),
                        "lower_tick": int(lower_tick),
                        "upper_tick": int(upper_tick),
                        "entry_z": float(z),
                        "size_units": 1000.0,
                        "fees_usd": 0.0,
                    }
                    if verbose:
                        print(f"[ENTRY] {pair} fee={fee} t={t} z={z:.2f} dir={'LONG' if direction>0 else 'SHORT'} tick={entry_tick}")
                    in_pos = True
            else:
                # acumular fees si el tick está dentro de banda
                in_band = (tick >= cur["lower_tick"]) and (tick <= cur["upper_tick"])
                if in_band:
                    vol_usd = row[f"V_{pair}"]
                    est_fees = fee_rate * fee_share * (vol_usd if pd.notna(vol_usd) else 0.0)
                    cur["fees_usd"] += float(est_fees)

                # condiciones de salida
                exit_signal = False
                exit_reason = None
                if pd.notna(z) and abs(z) <= z_exit:
                    exit_signal = True
                    exit_reason = "take"
                elif pd.notna(z) and abs(z) >= sl_z:
                    exit_signal = True
                    exit_reason = "stop"

                if exit_signal:
                    cur["exit_time"] = t
                    cur["exit_tick"] = int(tick) if pd.notna(tick) else np.nan
                    cur["exit_z"] = float(z) if pd.notna(z) else np.nan
                    cur["exit_reason"] = exit_reason
                    cur["pnl_usd"] = cur["fees_usd"]
                    trades.append(cur)
                    if verbose:
                        print(f"[EXIT] {pair} fee={fee} t={t} reason={exit_reason} pnl={cur['pnl_usd']:.2f}")
                    in_pos = False
                    cur = None

        # cierre al final si quedó abierta
        if in_pos and cur is not None:
            last = df.iloc[-1]
            cur["exit_time"] = df.index[-1]
            cur["exit_tick"] = int(last.get(f"tick_{pair}", np.nan)) if pd.notna(last.get(f"tick_{pair}", np.nan)) else np.nan
            cur["exit_z"] = float(last.get(f"z_{pair}", np.nan)) if pd.notna(last.get(f"z_{pair}", np.nan)) else np.nan
            cur["exit_reason"] = "eod"
            cur["pnl_usd"] = cur["fees_usd"]
            trades.append(cur)
            if verbose:
                print(f"[EXIT] {pair} fee={fee} End-of-data pnl={cur['pnl_usd']:.2f}")

    if not trades:
        return pd.DataFrame(columns=[
            "fee","pair","dir","entry_time","exit_time","entry_tick","exit_tick","entry_z","exit_z",
            "band_ticks","lower_tick","upper_tick","size_units","fees_usd","pnl_usd","exit_reason"
        ])
    return pd.DataFrame(trades)


def parse_args():
    ap = argparse.ArgumentParser(description="Backtest mean-reversion triangulada para stables en Uniswap v3")
    ap.add_argument("--data-dir", type=str, default="../data", help="Carpeta que contiene los CSV *_Swap.csv")
    ap.add_argument("--z-entry", type=float, default=2.0, help="Umbral z-score de entrada")
    ap.add_argument("--z-exit", type=float, default=0.5, help="Umbral z-score de salida (take)")
    ap.add_argument("--sl-z", type=float, default=3.5, help="Stop por z-score (cierre si empeora)")
    ap.add_argument("--band-ticks", type=int, default=20, help="Ancho de banda en ticks (deprecated si usas --band-steps)")
    ap.add_argument("--band-steps", type=int, default=None, help="Ancho de banda como múltiplos de tickSpacing (recomendado)")
    ap.add_argument("--fee-share", type=float, default=0.02, help="Proxy de tu % de liquidez activa (0.02=2%)")
    ap.add_argument("--resample", type=str, default="1min", help="Frecuencia para señal (p.ej. 1min, 5min)")
    ap.add_argument("--lookback", type=int, default=1440, help="Ventana en barras para media/std del spread")
    ap.add_argument("--verbose", action="store_true", help="Mostrar logs detallados durante el backtest")
    return ap.parse_args()


def main():
    args = parse_args()
    data_dir = Path(args.data_dir)

    panel = build_price_panel(data_dir, args.resample)

    all_trades = []
    for fee in [100, 500, 3000]:
        block = panel.get(fee, {})
        if not block:
            continue
        prices = triangulated_prices(block)
        if prices.empty:
            continue

        lookback_bars = int(args.lookback)
        enriched = compute_spreads_and_z(prices, lookback_bars)

        trades_fee = backtest_fee_block(
            fee=fee,
            df=enriched,
            z_entry=args.z_entry,
            z_exit=args.z_exit,
            sl_z=args.sl_z,
            band_ticks_param=args.band_ticks,
            band_steps=args.band_steps,
            fee_share=args.fee_share,
            verbose=args.verbose,
        )
        if not trades_fee.empty:
            all_trades.append(trades_fee)

    if not all_trades:
        print("[WARN] No se generaron trades. Revisa nombres de archivos, presencia de los 3 pares por fee y parámetros.")
        return

    trades = pd.concat(all_trades, ignore_index=True)
    trades.sort_values("entry_time", inplace=True)
    trades.to_csv("trades_out.csv", index=False)

    total_pnl = trades["pnl_usd"].sum()
    n_trades = len(trades)
    by_fee = trades.groupby("fee")["pnl_usd"].agg(["count", "sum"]).reset_index()
    by_pair = trades.groupby("pair")["pnl_usd"].agg(["count", "sum"]).reset_index()

    print("\n================ BACKTEST SUMMARY ================")
    print(f"Trades totales: {n_trades}")
    print(f"PNL total (USD, solo fees estimadas): {total_pnl:,.2f}\n")

    print("PNL por fee:")
    print(by_fee.to_string(index=False))
    print("\nPNL por par:")
    print(by_pair.to_string(index=False))

    if n_trades > 0:
        duras = (pd.to_datetime(trades["exit_time"]) - pd.to_datetime(trades["entry_time"])).dt.total_seconds() / 60.0
        print(f"\nDuración media por trade (min): {duras.mean():.1f}")
        print(f"PNL medio por trade (USD): {trades['pnl_usd'].mean():.2f}")
        print(f"Mediana PNL por trade (USD): {trades['pnl_usd'].median():.2f}")
        print("\nCSV detallado: trades_out.csv")


if __name__ == "__main__":
    main()
