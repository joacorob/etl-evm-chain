#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mean-reversion bot for Stable/ETH pools
======================================

This script scans Uniswap v3 swap data for DAI/ETH, USDC/ETH and USDT/ETH pools
(across the common 100 ∣ 500 ∣ 3000 fee tiers).  It generates LONG / SHORT signals
when the price of a given pool deviates more than *threshold_pct* from the
cross-pool consensus price (median across all stable/ETH pools at that moment).

The strategy is hyper-conservative: we always wait for full mean reversion
(back to ≤0 deviation) before closing a position and we size each trade to one
unit of notional (1 ETH or the equivalent in stable for shorts).  No leverage
is assumed and fees/slippage are ignored (you can easily adapt that).

Back-test results are reported per-trade and aggregated by month *and* year.

Example
-------
    python stable_eth_meanrevert_bot.py \
        --data   ../data \
        --freq   60s \
        --thr    0.10  # 0.10 %=entry threshold

Requirements:  pandas, numpy, argparse, tqdm (optional progress-bar)
"""
import argparse, os, re, glob, math, warnings, sys, datetime as dt
from typing import Dict, List, Tuple, Any

import numpy as np
import pandas as pd
from tqdm import tqdm

# ─────────────────────────── constants ────────────────────────────
STABLES = ("DAI", "USDC", "USDT")
FEES    = ("100", "500", "3000")
FILE_RGX = re.compile(r"(?P<stable>DAI|USDC|USDT)ETH(?P<fee>100|500|3000)_Swap\.csv$", re.IGNORECASE)
LOG_BASE = 1.0001  # Uniswap v3 tick base

# ─────────────────────────── utils ────────────────────────────────

def pct_from_tick(q: float) -> float:
    """Converts *q* ticks → percentage deviation (approx)."""
    return (LOG_BASE**q - 1.0) * 100.0

def hr(title=""):
    print("\n" + "="*12 + f" {title} " + "="*12)

# ─────────────────────────── IO helpers ───────────────────────────

def list_swap_csvs(data_dir: str) -> List[str]:
    """Return list of valid *stable*ETH*Swap.csv files inside *data_dir*."""
    files = sorted(glob.glob(os.path.join(data_dir, "*ETH*_Swap.csv")))
    keep  = [f for f in files if FILE_RGX.search(os.path.basename(f))]
    if not keep:
        raise FileNotFoundError("No swap CSVs matching {stable}ETH{fee}_Swap.csv found in {data_dir}")
    return keep

def _read_one_csv(path: str, tz="UTC", downcast: bool=True, usecols=("timestamp","tick")) -> pd.DataFrame:
    """Read a single swap CSV: returns DataFrame[ timestamp, tick(Int64) ]."""
    df = pd.read_csv(path, usecols=list(usecols), on_bad_lines="skip", engine="c")
    df = df.dropna(subset=["timestamp","tick"]).copy()
    df["tick"] = pd.to_numeric(df["tick"], errors="coerce")
    df = df.dropna(subset=["tick"]).copy()
    if downcast:
        df["tick"] = df["tick"].astype(np.int32)
    else:
        df["tick"] = df["tick"].astype(np.int64)

    # Uniswap stores timestamps as Unix-seconds integers
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(np.int64), unit="s", utc=True)
    df = df.sort_values("timestamp")
    return df

def csv_to_usd_per_eth_ticks(path: str) -> pd.DataFrame:
    """Convert swap CSV to DataFrame[ timestamp, usd_per_eth_tick, stable, fee ]."""
    m = FILE_RGX.search(os.path.basename(path))
    if not m:
        raise ValueError(f"Unexpected filename: {path}")
    stable, fee = m.group("stable").upper(), m.group("fee")

    df = _read_one_csv(path)

    # Correct polarity: if median tick <0 ⇒ token1/token0 <1 ⇒ (stable/ETH) ticks are *already* usd/eth.
    # else we flip the sign.
    med = df["tick"].median()
    usd_per_eth_tick = df["tick"].values if med < 0 else -df["tick"].values

    out = pd.DataFrame({
        "timestamp": df["timestamp"].values,
        "usd_per_eth_tick": usd_per_eth_tick,
        "stable": stable,
        "fee": fee,
    })
    return out

def resample_ticks(df: pd.DataFrame, freq: str="60s") -> pd.DataFrame:
    """Resample irregular ticks into regular *freq* grid (seconds)."""
    tmp = df.set_index("timestamp")["usd_per_eth_tick"].resample(freq).last().ffill()
    out = tmp.to_frame(name="usd_per_eth_tick").reset_index()
    out["stable"] = df["stable"].iloc[0]
    out["fee"]    = df["fee"].iloc[0]
    return out

# ─────────────────────────── core logic ──────────────────────────

def build_price_matrix(csv_paths: List[str], freq="60s") -> Tuple[pd.DataFrame, List[str]]:
    """Return pivot-table DataFrame[ts × pool] with USD/ETH price."""
    series: List[pd.DataFrame] = []
    for p in tqdm(csv_paths, desc="Reading CSVs"):
        ticks   = csv_to_usd_per_eth_ticks(p)
        res     = resample_ticks(ticks, freq)
        # Convert ticks → price (usd per eth)
        res["price"] = LOG_BASE ** res["usd_per_eth_tick"]
        pool_name = f"{res['stable'].iloc[0]}ETH{res['fee'].iloc[0]}"
        res = res[["timestamp", "price"]].rename(columns={"price": pool_name})
        series.append(res)

    # Join on timestamp (inner join keeps common grid)
    base = series[0]
    for s in series[1:]:
        base = base.merge(s, on="timestamp", how="inner")

    base = base.sort_values("timestamp").reset_index(drop=True)
    pools = [c for c in base.columns if c != "timestamp"]
    return base, pools

# ─────────────────────────── back-test ───────────────────────────

def simulate_mean_reversion(df: pd.DataFrame, pools: List[str], thr_pct: float=0.10) -> pd.DataFrame:
    """Run mean-reversion simulation.

    Parameters
    ----------
    df : DataFrame as returned by build_price_matrix.
    thr_pct : deviation threshold in **percent** (e.g. 0.10 for 0.10%).

    Returns a trades DataFrame.
    """
    threshold = thr_pct/100.0  # percentage → fraction
    mean_price = df[pools].mean(axis=1)

    open_pos: Dict[str, Dict[str,Any]] = {}
    trades: List[Dict[str,Any]] = []

    for idx, row in df.iterrows():
        ts   = row["timestamp"]
        mean = mean_price.iloc[idx]
        for pool in pools:
            price = row[pool]
            dev   = (price - mean)/mean  # +ve ⇒ overpriced (short signal)
            pos   = open_pos.get(pool)

            # Entry conditions (no open pos)
            if pos is None:
                if dev <= -threshold:   # LONG (buy ETH cheap)
                    open_pos[pool] = {"side":"long", "entry_ts":ts, "entry_price":price, "entry_dev":dev}
                elif dev >= threshold:  # SHORT (sell ETH expensive)
                    open_pos[pool] = {"side":"short","entry_ts":ts, "entry_price":price, "entry_dev":dev}
                continue

            # Exit condition: mean reversion (dev crosses 0)
            side = pos["side"]
            if (side=="long" and dev >= 0) or (side=="short" and dev <= 0):
                # Close trade
                pct_ret = (price - pos["entry_price"])/pos["entry_price"] if side=="long" else (pos["entry_price"] - price)/pos["entry_price"]
                trades.append({
                    "pool": pool,
                    "side": side,
                    "entry_ts": pos["entry_ts"],
                    "exit_ts": ts,
                    "entry_price": pos["entry_price"],
                    "exit_price": price,
                    "pct_return": pct_ret
                })
                open_pos.pop(pool)
    return pd.DataFrame(trades)

# ─────────────────────────── alternative Z-score strategy ─────────

def simulate_zscore_reversion(df: pd.DataFrame, pools: List[str], *, lookback: int = 1440, entry_z: float = 2.0, exit_z: float = 0.2, max_hold: int = 10080) -> pd.DataFrame:
    """Simulates mean-reversion using rolling Z-scores.

    Parameters
    ----------
    df        : DataFrame of prices as returned by build_price_matrix.
    lookback  : Rolling window length (in rows) to compute mean/std (e.g. 1440 rows ≈ 1 day @60s).
    entry_z   : |Z| threshold to open a trade.
    exit_z    : |Z| threshold to close (default 0.2 ≈ back to near-mean).
    max_hold  : Maximum holding period (rows). Forces exit even if |Z|>exit_z.
    """

    # Pre-compute rolling mean/std of deviations per pool
    consensus = df[pools].mean(axis=1)
    dev_df    = df[pools].sub(consensus, axis=0)

    roll_mean = dev_df.rolling(lookback, min_periods=lookback).mean()
    roll_std  = dev_df.rolling(lookback, min_periods=lookback).std(ddof=0)
    z_df      = (dev_df - roll_mean) / roll_std

    open_pos: Dict[str, Dict[str,Any]] = {}
    trades: List[Dict[str,Any]] = []

    for idx in range(len(df)):
        ts = df["timestamp"].iloc[idx]
        for pool in pools:
            z = z_df[pool].iloc[idx]
            if math.isnan(z):
                continue  # not enough history yet

            pos = open_pos.get(pool)

            # Entry
            if pos is None:
                if z <= -entry_z:
                    open_pos[pool] = {"side":"long", "entry_idx":idx, "entry_ts":ts, "entry_price":df[pool].iloc[idx], "entry_z":z}
                elif z >=  entry_z:
                    open_pos[pool] = {"side":"short","entry_idx":idx, "entry_ts":ts, "entry_price":df[pool].iloc[idx], "entry_z":z}
                continue

            # Exit conditions
            held = idx - pos["entry_idx"]
            side = pos["side"]
            price = df[pool].iloc[idx]

            exit_cond = False
            if side=="long" and z >= -exit_z:
                exit_cond = True
            elif side=="short" and z <=  exit_z:
                exit_cond = True
            elif held >= max_hold:
                exit_cond = True  # time stop

            if exit_cond:
                pct_ret = (price - pos["entry_price"])/pos["entry_price"] if side=="long" else (pos["entry_price"] - price)/pos["entry_price"]
                trades.append({
                    "pool": pool,
                    "side": side,
                    "entry_ts": pos["entry_ts"],
                    "exit_ts": ts,
                    "entry_price": pos["entry_price"],
                    "exit_price": price,
                    "entry_z": pos["entry_z"],
                    "exit_z": z,
                    "pct_return": pct_ret,
                    "held_rows": held
                })
                open_pos.pop(pool)

    return pd.DataFrame(trades)

# ─────────────────────────── reporting ───────────────────────────

def aggregate_returns(trades: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if trades.empty:
        return pd.DataFrame(), pd.DataFrame()
    trades["entry_ts"] = pd.to_datetime(trades["entry_ts"], utc=True)
    trades["exit_ts"]  = pd.to_datetime(trades["exit_ts"], utc=True)

    trades["year"]  = trades["exit_ts"].dt.year
    trades["month"] = trades["exit_ts"].dt.to_period("M")

    monthly = trades.groupby("month")["pct_return"].sum().to_frame(name="sum_return")
    yearly  = trades.groupby("year")["pct_return"].sum().to_frame(name="sum_return")
    return monthly, yearly

# ─────────────────────────── CLI ─────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Mean-reversion bot for stable/ETH pools")
    p.add_argument("--data", required=True, type=str, help="Directory with *ETH*_Swap.csv files")
    p.add_argument("--freq", type=str, default="60s", help="Resample frequency (pandas offset string, default 60s)")
    p.add_argument("--thr",  type=float, default=0.10, help="(pct mode) Deviation threshold in percent (default 0.10)")
    p.add_argument("--mode", choices=["pct","zscore"], default="zscore", help="Back-test mode: pct (old) or zscore (improved)")
    p.add_argument("--lookback", type=int, default=1440, help="(zscore) Rolling window length in rows (default 1440 ≈1d @60s)")
    p.add_argument("--entry_z", type=float, default=2.0, help="(zscore) |Z| threshold to open trade (default 2.0)")
    p.add_argument("--exit_z",  type=float, default=0.2, help="(zscore) |Z| threshold to close trade (default 0.2)")
    p.add_argument("--max_hold", type=int, default=10080, help="(zscore) Max holding period in rows (default 10080 ≈1w @60s)")
    p.add_argument("--report", action="store_true", help="Print trades & performance summary")

    args = p.parse_args()

    hr("LOADING DATA")
    csvs = list_swap_csvs(args.data)
    df_prices, pools = build_price_matrix(csvs, freq=args.freq)
    print(f"✓ Joined dataset: {len(df_prices)} rows, {len(pools)} pools on grid {args.freq}")

    hr("SIMULATING")
    if args.mode == "pct":
        trades = simulate_mean_reversion(df_prices, pools, thr_pct=args.thr)
    else:
        trades = simulate_zscore_reversion(
            df_prices, pools,
            lookback=args.lookback,
            entry_z=args.entry_z,
            exit_z=args.exit_z,
            max_hold=args.max_hold,
        )
    print(f"✓ Completed: {len(trades)} trades generated")

    monthly, yearly = aggregate_returns(trades)

    if args.report:
        hr("TRADES (head)")
        print(trades.head())
        hr("MONTHLY RETURNS (Σ pct)")
        print(monthly.tail(24))
        hr("YEARLY RETURNS (Σ pct)")
        print(yearly)

    # Quick summary always
    hr("SUMMARY")
    total = trades["pct_return"].sum() if not trades.empty else 0.0
    print(f"Total strategy return over back-test: {total*100:.2f}% (un-compounded) across {len(trades)} trades\n")

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
    main()
