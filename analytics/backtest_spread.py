#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest "paridad" (sin LP): arbitra divergencias entre pools ETH-stable.
- Señal: |z-score| > z_entry contra la mediana cross-pool (event-driven).
- Dirección: revertir hacia la mediana (si pool caro: short ETH; si barato: long ETH).
- Salida: reconvergencia (|z| < z_exit) o stop-loss 2%.
- Gestión de riesgo: máx. exposición simultánea 20k USD; tamaño fijo 1k USD por trade.
- Horizonte: último año (--year-lookback).
- Output: ROI sobre capital de trading (20k), CSV de trades y JSON de métricas.

Requiere: pandas, numpy
"""

import argparse, os, json, math, glob, sys
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple
from datetime import datetime
import pandas as pd
import numpy as np

# ---------- Config base ----------
CAPITAL_LIMIT_USD = 20_000
POSITION_SIZE_USD = 1_000
Z_EXIT = 0.25          # umbral de salida por reconvergencia
SL_PCT = 0.02          # 2% stop loss
MIN_POOLS_FOR_REF = 3  # para calcular mediana confiable
Z_CANDIDATES = [1.25, 1.5, 1.75, 2.0, 2.5, 3.0]
WINDOW_MINUTES = [5, 10, 15]

# Pools esperados (no obligatorio, solo para ordenar reportes)
EXPECTED_POOLS = {
    "USDCETH100","USDCETH500","USDCETH1000","USDCETH3000",
    "USDTETH100","USDTETH500","USDTETH1000","USDTETH3000",
    "DAIETH100","DAIETH500","DAIETH1000","DAIETH3000"
}

def human(ts: int) -> str:
    try:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)

def usd_per_eth_from_tick(tick: int) -> float:
    # Numéricamente estable: 1 / (1.0001 ** tick) == exp(-tick*log(1.0001))
    return math.exp(-float(tick) * math.log(1.0001))

@dataclass
class Position:
    pool: str
    direction: str  # "short_eth" (espera baja de USD/ETH) o "long_eth" (espera suba)
    entry_ts: int
    entry_price: float  # USD/ETH
    notional: float = POSITION_SIZE_USD
    open: bool = True

    def pnl_usd(self, exit_price: float) -> float:
        # Short ETH gana si precio baja; Long ETH gana si sube
        if self.direction == "short_eth":
            ret = (self.entry_price - exit_price) / self.entry_price
        else:
            ret = (exit_price - self.entry_price) / self.entry_price
        return self.notional * ret

def load_events(data_dir: str, year_lookback_days: int, verbose: bool=False) -> pd.DataFrame:
    print(f"[LOAD] Escaneando carpeta: {data_dir}")

    # Prioriza *_Swap.csv; si no hay, toma *.csv
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
            header = pd.read_csv(p, nrows=0)
            cols_present = [c for c in usecols if c in header.columns]
            df = pd.read_csv(p, usecols=cols_present)
        except Exception as e:
            print(f"[LOAD] ⚠️ Fallo leyendo {os.path.basename(p)}: {e}")
            continue

        raw_rows = len(df)
        total_rows_raw += raw_rows

        # Si existe event_name, filtra a Swap (aunque el nombre del archivo ya lo indica)
        if "event_name" in df.columns:
            df = df[df["event_name"] == "Swap"]

        # Campos mínimos
        if "timestamp" not in df.columns or "tick" not in df.columns or "contract_name" not in df.columns:
            print(f"[LOAD] ⚠️ {os.path.basename(p)} sin columnas mínimas, se salta.")
            continue

        # Coerción de tipos + limpieza
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df["tick"] = pd.to_numeric(df["tick"], errors="coerce")
        df = df.dropna(subset=["timestamp","tick","contract_name"]).copy()

        # Sec vs ms
        if not df.empty and df["timestamp"].median() > 1e12:
            df["timestamp"] = (df["timestamp"] // 1000)
            if verbose:
                print(f"[LOAD] {os.path.basename(p)}: timestamps parecían en ms -> convertidos a s.")

        # Cast seguro a int64
        df["timestamp"] = df["timestamp"].astype("int64", copy=False)
        df["tick"] = df["tick"].astype("int64", copy=False)

        df["pool"] = df["contract_name"].astype(str)
        if "liquidity" not in df.columns: df["liquidity"] = np.nan
        if "amount0" not in df.columns: df["amount0"] = np.nan

        kept = len(df)
        total_rows_kept += kept
        if verbose:
            print(f"[LOAD] {os.path.basename(p)}: filas={raw_rows}, usadas={kept}")

        dfs.append(df[["timestamp","tick","pool","liquidity","amount0"]])

    if not dfs:
        raise RuntimeError("No se encontraron CSV válidos en la carpeta.")

    all_df = pd.concat(dfs, ignore_index=True)

    # Último año
    max_ts = int(all_df["timestamp"].max())
    cutoff = max_ts - year_lookback_days * 24 * 3600
    pre_cut = len(all_df)
    all_df = all_df[all_df["timestamp"] >= cutoff].copy()
    post_cut = len(all_df)

    # Precio USD/ETH
    all_df["usd_per_eth"] = all_df["tick"].apply(usd_per_eth_from_tick)

    # Ordenar
    all_df.sort_values(["timestamp"], inplace=True, kind="mergesort")
    all_df.reset_index(drop=True, inplace=True)

    if len(all_df) == 0:
        raise RuntimeError("No quedaron eventos tras la limpieza (revisar CSVs).")

    # Resumen
    pools = sorted(all_df["pool"].unique().tolist())
    first_ts = int(all_df["timestamp"].min())
    last_ts = int(all_df["timestamp"].max())
    print(f"[LOAD] Filas brutas: {total_rows_raw:,} | Filas válidas: {total_rows_kept:,}")
    print(f"[LOAD] Rango temporal recortado a {year_lookback_days} días: {human(first_ts)} → {human(last_ts)} | kept {post_cut:,}/{pre_cut:,}")
    print(f"[LOAD] Pools detectados ({len(pools)}): {', '.join(pools)}")

    # Conteo por pool (top 12)
    counts = all_df["pool"].value_counts().head(12)
    for pool, cnt in counts.items():
        print(f"[LOAD]  - {pool}: {cnt:,} eventos")

    return all_df

def backtest(all_df: pd.DataFrame, z_entry: float, window_min: int, log_every: int=200000, verbose: bool=False) -> Tuple[float, Dict]:
    print(f"[BT] Iniciando backtest: z_entry={z_entry}, ventana={window_min}min")
    window_sec = window_min * 60
    latest_price: Dict[str, float] = {}              # precio actual de cada pool
    diffs_by_pool: Dict[str, deque] = defaultdict(deque)  # (ts, diff) últimos window_sec por pool
    open_positions: List[Position] = []
    equity = 0.0
    exposure = 0.0
    trades = []

    def close_position(pos: Position, ts: int, price_now: float, reason: str):
        nonlocal equity, exposure
        pnl = pos.pnl_usd(price_now)
        equity += pnl
        exposure -= pos.notional
        pos.open = False
        trades.append({
            "pool": pos.pool,
            "direction": pos.direction,
            "entry_ts": pos.entry_ts,
            "entry_price": pos.entry_price,
            "exit_ts": ts,
            "exit_price": price_now,
            "pnl_usd": pnl,
            "reason": reason
        })
        if verbose:
            print(f"[TRADE] CLOSE {pos.pool} {reason} @ {human(ts)} PnL=${pnl:,.2f} Eq=${equity:,.2f}")

    # Índice por pool para detectar si ya hay una posición abierta
    has_open = defaultdict(lambda: False)

    total_rows = len(all_df)
    last_heartbeat_price_ref = None

    for i, row in all_df.iterrows():
        ts = int(row["timestamp"])
        pool = row["pool"]
        p_pool = float(row["usd_per_eth"])
        latest_price[pool] = p_pool

        # precio de referencia = mediana cross-pool
        if len(latest_price) >= MIN_POOLS_FOR_REF:
            p_ref = float(np.median(list(latest_price.values())))
            last_heartbeat_price_ref = p_ref
        else:
            continue

        # actualizar series de diffs para z-score de ESTE pool
        d = p_pool - p_ref
        dq = diffs_by_pool[pool]
        dq.append((ts, d))
        # purga ventana temporal
        while dq and (ts - dq[0][0] > window_sec):
            dq.popleft()

        # calcular z para ESTE pool
        vals = [x[1] for x in dq]
        if len(vals) < 10:
            z = None
        else:
            mu = float(np.mean(vals))
            sd = float(np.std(vals, ddof=1))
            z = (d - mu) / sd if sd > 1e-12 else None

        # manejar posiciones abiertas: SL / reconvergencia
        for pos in list(open_positions):
            if not pos.open or pos.pool != pool:
                continue
            # stop loss 2% direccional
            adverse = ((p_pool - pos.entry_price)/pos.entry_price)
            if pos.direction == "short_eth" and adverse > SL_PCT:
                close_position(pos, ts, p_pool, "stop_loss")
                has_open[pos.pool] = False
                continue
            if pos.direction == "long_eth" and adverse < -SL_PCT:
                close_position(pos, ts, p_pool, "stop_loss")
                has_open[pos.pool] = False
                continue
            # salida por reconvergencia
            if z is not None and abs(z) < Z_EXIT:
                close_position(pos, ts, p_pool, "reconvergence")
                has_open[pos.pool] = False
                continue

        # señal de entrada: |z| > z_entry en ESTE pool
        if z is not None and not has_open[pool] and abs(z) > z_entry:
            if exposure + POSITION_SIZE_USD <= CAPITAL_LIMIT_USD:
                direction = "short_eth" if (p_pool > p_ref) else "long_eth"
                pos = Position(pool=pool, direction=direction, entry_ts=ts, entry_price=p_pool)
                open_positions.append(pos)
                has_open[pool] = True
                exposure += POSITION_SIZE_USD
                trades.append({
                    "pool": pool,
                    "direction": direction,
                    "signal_ts": ts,
                    "signal_price": p_pool,
                    "action": "open",
                    "note": f"z={z:.2f} > {z_entry:.2f} -> {direction}"
                })
                if verbose:
                    print(f"[TRADE] OPEN  {pool} {direction} @ {human(ts)} z={z:.2f} exp=${exposure:,.0f}")

        # heartbeat de progreso
        if log_every and (i % log_every == 0) and i > 0:
            pct = 100.0 * i / max(1, total_rows - 1)
            print(f"[BT] Progreso {i:,}/{total_rows:,} ({pct:5.1f}%) | ts={human(ts)} | pools={len(latest_price)} | exp=${exposure:,.0f} | open={sum(1 for p in open_positions if p.open)} | p_ref≈{last_heartbeat_price_ref:.2f}")

    # cerrar remanentes a último precio conocido del propio pool
    last_price_by_pool = all_df.groupby("pool")["usd_per_eth"].last().to_dict()
    last_ts_by_pool = all_df.groupby("pool")["timestamp"].last().to_dict()
    for pos in open_positions:
        if pos.open:
            p = last_price_by_pool.get(pos.pool, pos.entry_price)
            t = int(last_ts_by_pool.get(pos.pool, pos.entry_ts))
            close_position(pos, t, p, "eod")

    # ROI sobre capital de trading (20k)
    roi = equity / CAPITAL_LIMIT_USD
    metrics = {
        "z_entry": z_entry,
        "window_min": window_min,
        "equity_usd": equity,
        "roi_on_20k": roi,
        "num_trades": sum(1 for t in trades if t.get("action") != "open")
    }
    print(f"[BT] Finalizado: z_entry={z_entry}, W={window_min}min | ROI(yr)={roi:.4f} | Equity=${equity:,.2f} | Trades cerrados={metrics['num_trades']}")
    return roi, {"metrics": metrics, "trades": trades}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="Carpeta con CSVs (ideal *_Swap.csv)")
    ap.add_argument("--year-lookback", type=int, default=365)
    ap.add_argument("--log-every", type=int, default=100000, help="Heartbeat cada N filas (0=off)")
    ap.add_argument("--verbose", action="store_true", help="Imprimir aperturas/cierres de trades")
    args = ap.parse_args()

    print("[MAIN] ================== BACKTEST SPREAD ==================")
    print(f"[MAIN] Params: lookback={args.year_lookback}d | log_every={args.log_every} | verbose={args.verbose}")
    df = load_events(args.data_dir, args.year_lookback, verbose=args.verbose)
    print("[MAIN] Dataframe listo. Iniciando grid-search z×W ...")

    best = None
    best_payload = None
    best_pair = None
    for W in WINDOW_MINUTES:
        for z in Z_CANDIDATES:
            roi, payload = backtest(df, z_entry=z, window_min=W, log_every=args.log_every, verbose=args.verbose)
            print(f"[GRID] z={z:<4} W={W:<3} -> ROI={roi:.5f}")
            if (best is None) or (roi > best):
                best = roi
                best_payload = payload
                best_pair = (z, W)
                print(f"[GRID]  ✅ Nuevo mejor: z={z}, W={W} | ROI={roi:.5f}")

    z_opt, W_opt = best_pair
    print(f"[RESULT] Recomendado: z_entry={z_opt}, window={W_opt}min | ROI(yr)={best_payload['metrics']['roi_on_20k']:.4f}")
    # Guardar outputs
    with open("metrics_spread.json","w") as f:
        json.dump(best_payload["metrics"], f, indent=2)
    pd.DataFrame(best_payload["trades"]).to_csv("trades_spread.csv", index=False)
    print("[RESULT] Archivos escritos: metrics_spread.json, trades_spread.csv")

if __name__ == "__main__":
    main()
