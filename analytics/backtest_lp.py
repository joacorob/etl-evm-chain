#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest LP single-side ultra-angosto (con logs/verbosidad):
- Entrada: |z| > z_entry. Si pool caro (USD/ETH > ref) => BAJA -> rango debajo (token1-only); si barato => ARRIBA (token0-only).
- Rango: angosto y alineado a tickSpacing; se recomienda por grid-search (0.05% / 0.10%).
- Fees: feeTier * vol_en_rango * share; share ≈ L_user / (L_activa + L_user) usando 'liquidity' del swap como proxy.
- Salida: pasó el rango favorable o SL 2% adverso.
- Gestión: exposición simultánea ≤ 20k USD; tamaño por trade = 1k USD.
- Horizonte: último año (--year-lookback).
- Verbosidad: --log-every N (heartbeat) y --verbose (aperturas/cierres).
"""

import argparse, os, json, math, glob
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple
from datetime import datetime
import pandas as pd
import numpy as np

CAPITAL_LIMIT_USD = 20_000
POSITION_SIZE_USD = 1_000
Z_EXIT = 0.25
SL_PCT = 0.02
MIN_POOLS_FOR_REF = 3

Z_CANDIDATES = [1.25, 1.5, 1.75, 2.0, 2.5]
WINDOW_MINUTES = [5, 10, 15]
RANGE_PCT_CAND = [0.0005, 0.0010]  # 0.05% y 0.10%

FEE_TIER_MAP = {100:0.0001, 500:0.0005, 1000:0.0010, 3000:0.0030}
TICK_SPACING_MAP = {100:1, 500:10, 1000:20, 3000:60}
DEC0_MAP = {"USDC":6, "USDT":6, "DAI":18}  # token0 decimales (stable)

def human(ts: int) -> str:
    try:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)

def parse_fee_from_pool(pool: str) -> int:
    return int(pool.replace("ETH","").replace("USDC","").replace("USDT","").replace("DAI",""))

def usd_per_eth_from_tick(tick: int) -> float:
    return math.exp(-float(tick) * math.log(1.0001))

def sqrt_ratio_from_tick(tick: int) -> float:
    return 1.0001 ** (tick / 2.0)

@dataclass
class Position:
    pool: str
    fee: int
    direction: str           # "down" (rango abajo, token1-only) o "up" (rango arriba, token0-only)
    entry_ts: int
    entry_tick: int
    entry_price: float       # USD/ETH
    tick_lower: int
    tick_upper: int
    L_user: float
    notional: float = POSITION_SIZE_USD
    ever_in_range: bool = False
    open: bool = True
    fees_usd: float = 0.0

def align_ticks_for_range(entry_tick: int, fee: int, target_pct: float, direction: str) -> Tuple[int,int]:
    spacing = TICK_SPACING_MAP[fee]
    width = math.ceil(math.log(1.0 + target_pct) / math.log(1.0001))
    width = max(width, spacing)
    if direction == "down":
        upper = (entry_tick // spacing) * spacing - spacing
        lower = upper - ((width // spacing) * spacing)
    else:
        lower = ((entry_tick + spacing) // spacing) * spacing
        upper = lower + ((width // spacing) * spacing)
    if lower >= upper:
        upper = lower + spacing
    return lower, upper

def L_from_amounts_single_side(tick_l: int, tick_u: int, amount0: float=None, amount1: float=None) -> float:
    sa = sqrt_ratio_from_tick(tick_l)
    sb = sqrt_ratio_from_tick(tick_u)
    if amount1 is not None:
        return amount1 / (sb - sa)             # token1-only
    if amount0 is not None:
        return amount0 * (sa * sb) / (sb - sa) # token0-only
    raise ValueError("Provide amount0 or amount1")

def amounts_at_price(L: float, tick_l: int, tick_u: int, tick_now: int) -> Tuple[float,float]:
    sa = sqrt_ratio_from_tick(tick_l)
    sb = sqrt_ratio_from_tick(tick_u)
    s = sqrt_ratio_from_tick(tick_now)
    if tick_now <= tick_l:
        amt0 = L * (sb - sa) / (sa * sb); amt1 = 0.0
    elif tick_now >= tick_u:
        amt0 = 0.0; amt1 = L * (sb - sa)
    else:
        amt0 = L * (sb - s) / (s * sb)
        amt1 = L * (s - sa)
    return amt0, amt1

def load_events(data_dir: str, year_lookback_days: int, verbose: bool=False) -> pd.DataFrame:
    print(f"[LOAD] Escaneando carpeta: {data_dir}")
    swap_paths = sorted(glob.glob(os.path.join(data_dir, "*_Swap.csv")))
    if not swap_paths:
        swap_paths = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
        print(f"[LOAD] No se hallaron *_Swap.csv; usando {len(swap_paths)} archivos *.csv")
    else:
        print(f"[LOAD] Archivos *_Swap.csv detectados: {len(swap_paths)}")

    usecols = ["timestamp","tick","contract_name","liquidity","amount0","event_name"]
    dfs = []
    total_rows_raw = 0
    total_rows_kept = 0

    for p in swap_paths:
        try:
            hdr = pd.read_csv(p, nrows=0)
            cols_present = [c for c in usecols if c in hdr.columns]
            df = pd.read_csv(p, usecols=cols_present)
        except Exception as e:
            print(f"[LOAD] ⚠️ Fallo leyendo {os.path.basename(p)}: {e}")
            continue

        raw_rows = len(df)
        total_rows_raw += raw_rows

        if "event_name" in df.columns:
            df = df[df["event_name"] == "Swap"]

        if not {"timestamp","tick","contract_name"}.issubset(df.columns):
            print(f"[LOAD] ⚠️ {os.path.basename(p)} sin columnas mínimas, se salta.")
            continue

        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df["tick"] = pd.to_numeric(df["tick"], errors="coerce")
        df["amount0"] = pd.to_numeric(df.get("amount0", np.nan), errors="coerce")
        df["liquidity"] = pd.to_numeric(df.get("liquidity", np.nan), errors="coerce")
        df = df.dropna(subset=["timestamp","tick","contract_name"]).copy()

        if not df.empty and df["timestamp"].median() > 1e12:
            df["timestamp"] = (df["timestamp"] // 1000)
            if verbose:
                print(f"[LOAD] {os.path.basename(p)}: timestamps parecían en ms -> convertidos a s.")

        df["timestamp"] = df["timestamp"].astype("int64", copy=False)
        df["tick"] = df["tick"].astype("int64", copy=False)
        df["pool"] = df["contract_name"].astype(str)

        kept = len(df)
        total_rows_kept += kept
        if verbose:
            print(f"[LOAD] {os.path.basename(p)}: filas={raw_rows}, usadas={kept}")

        dfs.append(df[["timestamp","tick","pool","liquidity","amount0"]])

    if not dfs:
        raise RuntimeError("No se encontraron CSV válidos.")

    all_df = pd.concat(dfs, ignore_index=True)

    max_ts = int(all_df["timestamp"].max())
    cutoff = max_ts - year_lookback_days * 24 * 3600
    pre_cut = len(all_df)
    all_df = all_df[all_df["timestamp"] >= cutoff].copy()
    post_cut = len(all_df)

    all_df["usd_per_eth"] = all_df["tick"].apply(usd_per_eth_from_tick)

    def stable_sym(pool: str) -> str:
        if pool.startswith("USDC"): return "USDC"
        if pool.startswith("USDT"): return "USDT"
        return "DAI"
    all_df["stable"] = all_df["pool"].apply(stable_sym)

    def vol_usd(row):
        dec = DEC0_MAP[row["stable"]]
        a0 = 0.0 if pd.isna(row["amount0"]) else float(row["amount0"])
        return abs(a0) / (10 ** dec)
    all_df["vol_usd"] = all_df.apply(vol_usd, axis=1)

    all_df.sort_values(["timestamp"], inplace=True, kind="mergesort")
    all_df.reset_index(drop=True, inplace=True)

    if len(all_df) == 0:
        raise RuntimeError("No quedaron eventos tras la limpieza (revisar CSVs).")

    pools = sorted(all_df["pool"].unique().tolist())
    first_ts = int(all_df["timestamp"].min())
    last_ts = int(all_df["timestamp"].max())
    print(f"[LOAD] Filas brutas: {total_rows_raw:,} | Filas válidas: {total_rows_kept:,}")
    print(f"[LOAD] Rango temporal recortado a {year_lookback_days} días: {human(first_ts)} → {human(last_ts)} | kept {post_cut:,}/{pre_cut:,}")
    print(f"[LOAD] Pools detectados ({len(pools)}): {', '.join(pools)}")
    counts = all_df["pool"].value_counts().head(12)
    for pool, cnt in counts.items():
        print(f"[LOAD]  - {pool}: {cnt:,} eventos")

    return all_df

def backtest_lp(all_df: pd.DataFrame, z_entry: float, window_min: int, target_pct: float,
                log_every: int=200000, verbose: bool=False) -> Tuple[float, Dict]:
    print(f"[BT] Iniciando LP: z_entry={z_entry}, W={window_min}min, rango≈{target_pct*100:.3f}%")
    window_sec = window_min * 60
    latest_price: Dict[str, float] = {}
    latest_tick: Dict[str, int] = {}
    diffs_by_pool: Dict[str, deque] = defaultdict(deque)

    open_positions: List[Position] = []
    equity = 0.0
    exposure = 0.0
    trades = []

    has_open = defaultdict(lambda: False)

    def close_position(pos: Position, ts: int, tick_now: int, price_now: float, reason: str):
        nonlocal equity, exposure
        amt0, amt1 = amounts_at_price(pos.L_user, pos.tick_lower, pos.tick_upper, tick_now)
        value = amt0 + amt1 * price_now
        pnl = (value - pos.notional) + pos.fees_usd
        equity += pnl
        exposure -= pos.notional
        pos.open = False
        trades.append({
            "pool": pos.pool,
            "direction": pos.direction,
            "entry_ts": pos.entry_ts,
            "entry_price": pos.entry_price,
            "tick_lower": pos.tick_lower,
            "tick_upper": pos.tick_upper,
            "exit_ts": ts,
            "exit_price": price_now,
            "fees_usd": pos.fees_usd,
            "pnl_usd": pnl,
            "reason": reason
        })
        has_open[pos.pool] = False
        if verbose:
            print(f"[TRADE] CLOSE {pos.pool} {reason} @ {human(ts)} "
                  f"fees=${pos.fees_usd:,.2f} pnl=${pnl:,.2f} eq=${equity:,.2f}")

    total_rows = len(all_df)
    last_heartbeat_price_ref = None

    for i, row in all_df.iterrows():
        ts = int(row["timestamp"]); pool = row["pool"]
        tick = int(row["tick"]); p_pool = float(row["usd_per_eth"])
        latest_price[pool] = p_pool
        latest_tick[pool] = tick

        if len(latest_price) >= MIN_POOLS_FOR_REF:
            p_ref = float(np.median(list(latest_price.values())))
            last_heartbeat_price_ref = p_ref
        else:
            continue

        d = p_pool - p_ref
        dq = diffs_by_pool[pool]
        dq.append((ts, d))
        while dq and (ts - dq[0][0] > window_sec):
            dq.popleft()
        vals = [x[1] for x in dq]
        z = None
        if len(vals) >= 10:
            mu = float(np.mean(vals)); sd = float(np.std(vals, ddof=1))
            z = (d - mu) / sd if sd > 1e-12 else None

        # gestionar posiciones abiertas de ESTE pool
        for pos in list(open_positions):
            if not pos.open or pos.pool != pool:
                continue
            adverse = (p_pool - pos.entry_price) / pos.entry_price
            if pos.direction == "down" and adverse > SL_PCT:
                close_position(pos, ts, tick, p_pool, "stop_loss")
                continue
            if pos.direction == "up" and adverse < -SL_PCT:
                close_position(pos, ts, tick, p_pool, "stop_loss")
                continue

            if pos.tick_lower <= tick < pos.tick_upper:
                pos.ever_in_range = True
                fee_rate = FEE_TIER_MAP[pos.fee]
                L_activa = float(row["liquidity"]) if not pd.isna(row["liquidity"]) else 0.0
                share = pos.L_user / (L_activa + pos.L_user) if (L_activa + pos.L_user) > 0 else 0.0
                fees = row["vol_usd"] * fee_rate * share
                pos.fees_usd += fees

            if pos.direction == "down":
                if pos.ever_in_range and tick < pos.tick_lower:
                    close_position(pos, ts, tick, p_pool, "passed_range")
                    continue
            else:
                if pos.ever_in_range and tick >= pos.tick_upper:
                    close_position(pos, ts, tick, p_pool, "passed_range")
                    continue

        # señal de entrada
        if z is not None and abs(z) > z_entry and not has_open[pool]:
            if exposure + POSITION_SIZE_USD <= CAPITAL_LIMIT_USD:
                direction = "down" if (p_pool > p_ref) else "up"
                fee = parse_fee_from_pool(pool)
                tick_l, tick_u = align_ticks_for_range(entry_tick=tick, fee=fee,
                                                       target_pct=target_pct, direction=direction)
                if direction == "down":
                    amount1 = POSITION_SIZE_USD / p_pool
                    L_user = L_from_amounts_single_side(tick_l, tick_u, amount1=amount1)
                else:
                    amount0 = POSITION_SIZE_USD
                    L_user = L_from_amounts_single_side(tick_l, tick_u, amount0=amount0)

                pos = Position(pool=pool, fee=fee, direction=direction, entry_ts=ts, entry_tick=tick,
                               entry_price=p_pool, tick_lower=tick_l, tick_upper=tick_u, L_user=L_user)
                open_positions.append(pos)
                has_open[pool] = True
                exposure += POSITION_SIZE_USD
                trades.append({
                    "pool": pool, "direction": direction, "signal_ts": ts, "signal_price": p_pool,
                    "tick_lower": tick_l, "tick_upper": tick_u, "action": "open",
                    "note": f"z={z:.2f} > {z_entry:.2f} -> LP range"
                })
                if verbose:
                    print(f"[TRADE] OPEN  {pool} {direction} fee={fee} "
                          f"[{tick_l},{tick_u}] z={z:.2f} @ {human(ts)} "
                          f"L_user={L_user:.6g} exp=${exposure:,.0f}")

        # heartbeat
        if log_every and (i % log_every == 0) and i > 0:
            pct = 100.0 * i / max(1, total_rows - 1)
            print(f"[BT] Progreso {i:,}/{total_rows:,} ({pct:5.1f}%) | ts={human(ts)} "
                  f"| pools={len(latest_price)} | exp=${exposure:,.0f} "
                  f"| open={sum(1 for p in open_positions if p.open)} "
                  f"| p_ref≈{(last_heartbeat_price_ref or 0):.2f}")

    # cierre EoD
    last_price_by_pool = all_df.groupby("pool")["usd_per_eth"].last().to_dict()
    last_tick_by_pool = all_df.groupby("pool")["tick"].last().to_dict()
    last_ts_by_pool = all_df.groupby("pool")["timestamp"].last().to_dict()
    for pos in open_positions:
        if pos.open:
            p = last_price_by_pool.get(pos.pool, pos.entry_price)
            t = int(last_ts_by_pool.get(pos.pool, pos.entry_ts))
            tk = int(last_tick_by_pool.get(pos.pool, pos.entry_tick))
            amt0, amt1 = amounts_at_price(pos.L_user, pos.tick_lower, pos.tick_upper, tk)
            value = amt0 + amt1 * p
            pnl = (value - pos.notional) + pos.fees_usd
            equity += pnl
            exposure -= pos.notional
            pos.open = False
            trades.append({
                "pool": pos.pool, "direction": pos.direction,
                "entry_ts": pos.entry_ts, "entry_price": pos.entry_price,
                "tick_lower": pos.tick_lower, "tick_upper": pos.tick_upper,
                "exit_ts": t, "exit_price": p, "fees_usd": pos.fees_usd,
                "pnl_usd": pnl, "reason": "eod"
            })
            if verbose:
                print(f"[TRADE] CLOSE {pos.pool} eod @ {human(t)} "
                      f"fees=${pos.fees_usd:,.2f} pnl=${pnl:,.2f} eq=${equity:,.2f}")

    roi = equity / CAPITAL_LIMIT_USD
    metrics = {
        "z_entry": z_entry,
        "window_min": window_min,
        "range_pct": target_pct,
        "equity_usd": equity,
        "roi_on_20k": roi,
        "num_trades": sum(1 for t in trades if t.get("action") != "open")
    }
    print(f"[BT] Finalizado LP: z={z_entry}, W={window_min}min, rango≈{target_pct*100:.3f}% "
          f"| ROI(yr)={roi:.4f} | Equity=${equity:,.2f} | Trades cerrados={metrics['num_trades']}")
    return roi, {"metrics": metrics, "trades": trades}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="Carpeta con CSVs (ideal *_Swap.csv)")
    ap.add_argument("--year-lookback", type=int, default=365)
    ap.add_argument("--log-every", type=int, default=100000, help="Heartbeat cada N filas (0=off)")
    ap.add_argument("--verbose", action="store_true", help="Imprimir aperturas/cierres de trades")
    args = ap.parse_args()

    print("[MAIN] ================== BACKTEST LP ==================")
    print(f"[MAIN] Params: lookback={args.year_lookback}d | log_every={args.log_every} | verbose={args.verbose}")
    df = load_events(args.data_dir, args.year_lookback, verbose=args.verbose)
    print("[MAIN] Dataframe listo. Iniciando grid-search z×W×range ...")

    best = None
    best_payload = None
    best_tuple = None
    for W in WINDOW_MINUTES:
        for z in Z_CANDIDATES:
            for rpct in RANGE_PCT_CAND:
                roi, payload = backtest_lp(df, z_entry=z, window_min=W, target_pct=rpct,
                                           log_every=args.log_every, verbose=args.verbose)
                print(f"[GRID] z={z:<4} W={W:<3} range={rpct*100:.3f}% -> ROI={roi:.5f}")
                if (best is None) or (roi > best):
                    best = roi
                    best_tuple = (z, W, rpct)
                    best_payload = payload
                    print(f"[GRID]  ✅ Nuevo mejor: z={z}, W={W}, range={rpct*100:.3f}% | ROI={roi:.5f}")

    z_opt, W_opt, rpct_opt = best_tuple
    print(f"[RESULT] Recomendado: z_entry={z_opt}, window={W_opt}min, range≈{rpct_opt*100:.3f}% "
          f"| ROI(yr)={best_payload['metrics']['roi_on_20k']:.4f}")

    with open("metrics_lp.json","w") as f:
        json.dump(best_payload["metrics"], f, indent=2)
    pd.DataFrame(best_payload["trades"]).to_csv("trades_lp.csv", index=False)
    print("[RESULT] Archivos escritos: metrics_lp.json, trades_lp.csv")

if __name__ == "__main__":
    main()
