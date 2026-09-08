#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_ml_dataset.py – generate supervised-learning dataset for mean-reversion bot.

Each row corresponds to a trade *entry* triggered by the same rules as
stable_eth_meanrevert_bot_v2.  We store only information observable at the
entry timestamp plus a target label `hit` (True if trade final PnL > 0).

Saved CSV columns (proposed):
    ts, pool, side, dev, abs_dev, zscore, fee_tier, stable, hour, dow, hit

Usage example
-------------
python analytics/build_ml_dataset.py \
    --data ./data \
    --thr 0.10 \
    --stop_pct 2.0 \
    --open_fee_pct 0.01 \
    --lookback 1440 \
    --out dataset.csv
"""
from __future__ import annotations
import argparse, os, re, glob, warnings
from typing import Dict, List, Any, Deque
from collections import deque

import numpy as np
import pandas as pd
from tqdm import tqdm

# ───────── constants (copied from v2) ─────────
STABLES = ("DAI", "USDC", "USDT")
FEES    = ("100", "500", "3000")
FILE_RGX = re.compile(r"(?P<stable>DAI|USDC|USDT)ETH(?P<fee>100|500|3000)_Swap\\.csv$", re.IGNORECASE)
LOG_BASE = 1.0001
_PRICE_MIN, _PRICE_MAX = 50.0, 10_000.0
STABLE_DECIMALS = {"DAI": 18, "USDC": 6, "USDT": 6}

# ───────── helpers ─────────────────────────────

def tick_to_price_usd_per_eth(tick: int, stable: str) -> float:
    dec = STABLE_DECIMALS[stable]
    p1 = (LOG_BASE ** (-tick)) * (10 ** (dec - 18))
    p2 = (LOG_BASE ** (-tick)) * (10 ** (18 - dec))
    for p in (p1, p2, 1.0/p1 if p1 else np.inf, 1.0/p2 if p2 else np.inf):
        if _PRICE_MIN <= p <= _PRICE_MAX:
            return p
    return float(np.median([p1, p2, 1.0/p1, 1.0/p2]))


def list_swap_csvs(data_dir: str) -> List[str]:
    """Return swap CSVs inside *data_dir* irrespective of the exact case of "Swap"."""
    patterns = ["*ETH*_Swap.csv", "*ETH*_swap.csv"]
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(data_dir, pat)))
    files = sorted(set(files))
    if not files:
        raise FileNotFoundError(f"No swap CSVs like *ETH*_Swap.csv found in {data_dir}.")
    return files


def _read_one_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["timestamp", "tick"], on_bad_lines="skip", engine="c")
    df = df.dropna().copy()
    df["tick"] = pd.to_numeric(df["tick"], errors="coerce").astype(np.int32)
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(np.int64), unit="s", utc=True)
    return df.sort_values("timestamp")


def csv_to_events(path: str) -> pd.DataFrame:
    m = FILE_RGX.search(os.path.basename(path))
    if not m:
        # Ignore non-stable/ETH pools (e.g., DAIUSDC)
        return pd.DataFrame()
    stable, fee = m.group("stable").upper(), m.group("fee")
    df = _read_one_csv(path)
    df["price"] = df["tick"].apply(lambda t: tick_to_price_usd_per_eth(int(t), stable))
    df = df[(df["price"] >= _PRICE_MIN) & (df["price"] <= _PRICE_MAX)]
    pool = f"{stable}ETH{fee}"
    return pd.DataFrame({"timestamp": df["timestamp"], "pool": pool, "price": df["price"], "stable": stable, "fee": fee})


def build_event_stream(csvs: List[str]):
    evts = [csv_to_events(p) for p in tqdm(csvs, desc="Reading CSVs") if os.path.getsize(p) > 0]
    all_events = pd.concat(evts, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
    pools = sorted(all_events["pool"].unique())
    return all_events, pools

# ───────── dataset generation ─────────

def generate_dataset(events: pd.DataFrame,
                     pools: List[str],
                     *,
                     thr_pct: float,
                     stop_pct: float,
                     open_fee_pct: float,
                     lookback: int = 1440):
    threshold = thr_pct / 100.0
    stop_thr  = stop_pct / 100.0
    fee_frac  = open_fee_pct / 100.0

    last_price: Dict[str, float] = {}
    dev_hist: Dict[str, Deque[float]] = {p: deque(maxlen=lookback) for p in pools}
    open_pos: Dict[str, Dict[str, Any]] = {}

    rows: List[Dict[str, Any]] = []

    for idx, row in events.iterrows():
        ts, pool, price = row["timestamp"], row["pool"], row["price"]
        stable, fee = row["stable"], row["fee"]
        last_price[pool] = price

        if len(last_price) < len(pools):
            continue

        global_mean = float(np.mean(list(last_price.values())))
        dev = (price - global_mean) / global_mean
        hist = dev_hist[pool]
        hist.append(dev)
        z = 0.0 if len(hist) < lookback or np.std(hist, ddof=0) == 0 else (dev - np.mean(hist)) / np.std(hist, ddof=0)

        pos = open_pos.get(pool)

        # Entry condition
        if pos is None:
            if abs(dev) < threshold or abs(dev) <= fee_frac:
                continue
            side = "long" if dev < 0 else "short"
            feature_row = {
                "ts": ts,
                "pool": pool,
                "side": side,
                "dev": dev,
                "abs_dev": abs(dev),
                "zscore": z,
                "fee_tier": int(fee),
                "stable": stable,
                "hour": ts.hour,
                "dow": ts.dayofweek,
                # placeholder for label
                "hit": np.nan,
            }
            open_pos[pool] = {"entry_price": price, "side": side, "row_ref": feature_row}
            continue

        # Exit logic
        side = pos["side"]
        pct_ret_live = (price - pos["entry_price"]) / pos["entry_price"] if side == "long" else (pos["entry_price"] - price) / pos["entry_price"]
        exit = False
        hit = None
        if pct_ret_live <= -stop_thr:
            exit = True
            hit = False  # stop-loss always means loss
        elif (side == "long" and dev >= 0) or (side == "short" and dev <= 0):
            exit = True
            hit = pct_ret_live > fee_frac  # after subtracting fee, positive? approx
        if exit:
            pos_row = pos["row_ref"]
            pos_row["hit"] = hit
            rows.append(pos_row)
            open_pos.pop(pool)

    return pd.DataFrame(rows)

# ───────── main ─────────

def main():
    ap = argparse.ArgumentParser(description="Build ML dataset for mean-reversion bot")
    ap.add_argument("--data", required=True, help="Directory with *ETH*_Swap.csv files")
    ap.add_argument("--thr", type=float, default=0.10, help="Deviation threshold (%)")
    ap.add_argument("--stop_pct", type=float, default=2.0, help="Stop-loss threshold (%)")
    ap.add_argument("--open_fee_pct", type=float, default=0.01, help="Opening fee (%)")
    ap.add_argument("--lookback", type=int, default=1440, help="Rolling window for z-score (rows)")
    ap.add_argument("--out", type=str, default="dataset.csv", help="Output CSV path")
    args = ap.parse_args()

    csvs = list_swap_csvs(args.data)
    events_df, pools = build_event_stream(csvs)
    ds = generate_dataset(events_df, pools, thr_pct=args.thr, stop_pct=args.stop_pct,
                           open_fee_pct=args.open_fee_pct, lookback=args.lookback)
    ds = ds.dropna(subset=["hit"]).reset_index(drop=True)
    ds.to_csv(args.out, index=False)
    print(f"✓ Dataset saved to {args.out} – {len(ds)} rows, {ds['hit'].mean()*100:.2f}% positives")

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
    main()
