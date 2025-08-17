#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# python analytics/stable_eth_meanrevert_bot.py --data ./data --mode zscore --lookback 100 --entry_z 3.0 --exit_z 0.3 --max_hold 1000 --notional 1000 --start_capital 80000 --trades_csv my_trades.csv --report --stop_pct 2.0 --open_fee_pct 0.01 

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
from collections import deque

# ─────────────────────────── constants ────────────────────────────
STABLES = ("DAI", "USDC", "USDT")
FEES    = ("100", "500", "3000")
FILE_RGX = re.compile(r"(?P<stable>DAI|USDC|USDT)ETH(?P<fee>100|500|3000)_Swap\.csv$", re.IGNORECASE)
LOG_BASE = 1.0001  # Uniswap v3 tick base
# Plausible ETH price bounds (USD). Used to pick correct orientation
_PRICE_MIN, _PRICE_MAX = 50.0, 10000.0

# ------------------------------------------------------------------
# Tick → USD/ETH price helper
# ------------------------------------------------------------------


def tick_to_price_usd_per_eth(tick: int, stable: str) -> float:
    """Convert *tick* to USD/ETH price heuristically.

    We don't know whether *stable* is token0 or token1 in the pool (Uniswap sorts
    tokens by address, not by symbol).  For each tick we try both orientations
    and pick the first price that falls into a sensible range (50‒10 000 USD).
    This works because any erroneous orientation yields either a microscopic or
    astronomical price far outside typical ETH prices.
    """
    dec_s = STABLE_DECIMALS[stable]

    # Core exponent – always positive for price magnitude
    base = LOG_BASE ** abs(tick)

    # Candidate prices (stable per ETH)
    candidates = []

    # 1) Assume tick encapsulates ETH priced in *stable* (token0=stable)
    #    Price = 1.0001^{-tick} * 10^{dec_s-18}
    price1 = (LOG_BASE ** (-tick)) * (10 ** (dec_s - 18))
    candidates.append(price1)

    # 2) Inverse orientation (token0=ETH)
    price2 = (LOG_BASE ** (-tick)) * (10 ** (18 - dec_s))
    candidates.append(price2)

    # 3) Reciprocal of the above two (handles potential sign inversions)
    candidates.append(1.0 / price1 if price1 != 0 else float("inf"))
    candidates.append(1.0 / price2 if price2 != 0 else float("inf"))

    # Pick first candidate inside plausible bounds
    for p in candidates:
        if _PRICE_MIN <= p <= _PRICE_MAX:
            return p

    # As fallback, return median of candidates (still better than None)
    return float(np.median(candidates))

# Map stable symbol → decimals (needed to adjust price ratios)
STABLE_DECIMALS = {
    "DAI": 18,
    "USDC": 6,
    "USDT": 6,
}

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

    # Store tick as-is; conversion to price done later with heuristics
    out = pd.DataFrame({
        "timestamp": df["timestamp"].values,
        "tick": df["tick"].values,
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
    out["scale"]  = df["scale"].iloc[0]
    return out

# ─────────────────────────── event-stream builder ────────────────────────────

def build_event_stream(csv_paths: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """Return DataFrame[ timestamp, pool, price ] sorted by *timestamp* and list of pools."""
    events: List[pd.DataFrame] = []
    for p in tqdm(csv_paths, desc="Reading CSVs"):
        ticks = csv_to_usd_per_eth_ticks(p)
        ticks["price"] = ticks.apply(lambda r: tick_to_price_usd_per_eth(int(r["tick"]), r["stable"]), axis=1)
        # Filter out obvious outliers (e.g., bad data)
        ticks = ticks[(ticks["price"] >= 50.0) & (ticks["price"] <= 10000.0)]
        if ticks.empty:
            continue  # Skip files with no valid rows after filtering
        pool_name = f"{ticks['stable'].iloc[0]}ETH{ticks['fee'].iloc[0]}"
        tmp = ticks[["timestamp", "price"]].copy()
        tmp["pool"] = pool_name
        events.append(tmp)

    all_events = pd.concat(events, ignore_index=True)
    all_events = all_events.sort_values("timestamp").reset_index(drop=True)
    pools = sorted(all_events["pool"].unique())
    return all_events, pools

# ─────────────────────────── core logic ──────────────────────────

def build_price_matrix(csv_paths: List[str], freq="60s") -> Tuple[pd.DataFrame, List[str]]:
    """Return pivot-table DataFrame[ts × pool] with USD/ETH price."""
    series: List[pd.DataFrame] = []
    for p in tqdm(csv_paths, desc="Reading CSVs"):
        ticks   = csv_to_usd_per_eth_ticks(p)
        res     = resample_ticks(ticks, freq)
        res["price"] = res.apply(lambda r: tick_to_price_usd_per_eth(int(r["usd_per_eth_tick"]) , r["stable"]), axis=1)
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

# ───────────────── event-level simulations (no resampling) ────────────────

def simulate_mean_reversion_events(events: pd.DataFrame, pools: List[str], thr_pct: float = 0.10, *, stop_pct: float = 2.0, open_fee_pct: float = 0.01) -> pd.DataFrame:
    """Mean-reversion on raw event stream (ordered by timestamp)."""
    threshold = thr_pct / 100.0
    stop_thresh = stop_pct / 100.0
    fee_frac    = open_fee_pct / 100.0
    last_price: Dict[str, float] = {}
    last_exit_ts: Dict[str, pd.Timestamp] = {}
    open_pos: Dict[str, Dict[str, Any]] = {}
    trades: List[Dict[str, Any]] = []

    for idx, row in events.iterrows():
        ts   = row["timestamp"]
        pool = row["pool"]
        price = row["price"]

        # Update latest price of this pool
        last_price[pool] = price

        # Wait until we have at least one price for every pool
        if len(last_price) < len(pools):
            continue

        mean_price = np.mean(list(last_price.values()))
        dev = (price - mean_price) / mean_price  # +ve ⇒ overpriced (short signal)

        pos = open_pos.get(pool)

        # Entry logic
        if pos is None:
            # Salta si la desviación no cubre el costo de apertura
            if abs(dev) <= fee_frac:
                continue
            # avoid reopening a trade in the *exact* same timestamp it was just closed
            if last_exit_ts.get(pool) is not None and last_exit_ts[pool] == ts:
                continue
            if dev <= -threshold:   # LONG
                open_pos[pool] = {"side": "long", "entry_ts": ts, "entry_price": price, "entry_dev": dev, "entry_mean_price": mean_price}
            elif dev >= threshold:  # SHORT
                open_pos[pool] = {"side": "short", "entry_ts": ts, "entry_price": price, "entry_dev": dev, "entry_mean_price": mean_price}
            continue

        # Exit logic (mean reversion)
        side = pos["side"]
        # Stop-loss: close the position if the unrealised loss reaches the threshold
        pct_ret_live = (price - pos["entry_price"]) / pos["entry_price"] if side == "long" else (pos["entry_price"] - price) / pos["entry_price"]
        if pct_ret_live <= -stop_thresh:
            trades.append({
                "pool": pool,
                "side": side,
                "entry_ts": pos["entry_ts"],
                "exit_ts": ts,
                "entry_price": pos["entry_price"],
                "exit_price": price,
                "entry_mean_price": pos["entry_mean_price"],
                "exit_mean_price": mean_price,
                "pct_return": pct_ret_live - fee_frac,
            })
            open_pos.pop(pool)
            last_exit_ts[pool] = ts
            continue
        if (side == "long" and dev >= 0) or (side == "short" and dev <= 0):
            pct_ret = (price - pos["entry_price"]) / pos["entry_price"] if side == "long" else (pos["entry_price"] - price) / pos["entry_price"]
            trades.append({
                "pool": pool,
                "side": side,
                "entry_ts": pos["entry_ts"],
                "exit_ts": ts,
                "entry_price": pos["entry_price"],
                "exit_price": price,
                "entry_mean_price": pos["entry_mean_price"],
                "exit_mean_price": mean_price,
                "pct_return": pct_ret - fee_frac,
            })
            open_pos.pop(pool)
            last_exit_ts[pool] = ts

    return pd.DataFrame(trades)


def simulate_zscore_reversion_events(events: pd.DataFrame, pools: List[str], *, lookback: int = 1440, entry_z: float = 2.0, exit_z: float = 0.2, max_hold: int = 10080, stop_pct: float = 2.0, open_fee_pct: float = 0.01) -> pd.DataFrame:
    """Z-score based mean-reversion on raw event stream."""
    last_price: Dict[str, float] = {}
    last_exit_ts: Dict[str, pd.Timestamp] = {}
    dev_hist: Dict[str, deque] = {p: deque(maxlen=lookback) for p in pools}
    open_pos: Dict[str, Dict[str, Any]] = {}
    trades: List[Dict[str, Any]] = []
    stop_thresh = stop_pct / 100.0
    fee_frac    = open_fee_pct / 100.0

    for idx, row in events.iterrows():
        ts   = row["timestamp"]
        pool = row["pool"]
        price = row["price"]

        last_price[pool] = price

        # Need all pools priced at least once
        if len(last_price) < len(pools):
            continue

        mean_price = np.mean(list(last_price.values()))
        dev = (price - mean_price) / mean_price

        # Update rolling history for this pool
        hist = dev_hist[pool]
        hist.append(dev)

        if len(hist) < lookback:
            continue  # Not enough history yet

        roll_mean = np.mean(hist)
        roll_std  = np.std(hist, ddof=0)
        z = 0.0 if roll_std == 0 else (dev - roll_mean) / roll_std

        pos = open_pos.get(pool)

        # Entry
        if pos is None:
            if abs(dev) <= fee_frac:  # requiere cubrir las dos comisiones
                continue  # no vale la pena abrir
            if last_exit_ts.get(pool) is not None and last_exit_ts[pool] == ts:
                continue  # skip if just closed at the same ts
            if z <= -entry_z:
                open_pos[pool] = {"side": "long", "entry_idx": idx, "entry_ts": ts, "entry_price": price, "entry_z": z, "entry_mean_price": mean_price}
            elif z >= entry_z:
                open_pos[pool] = {"side": "short", "entry_idx": idx, "entry_ts": ts, "entry_price": price, "entry_z": z, "entry_mean_price": mean_price}
            continue

        # Stop-loss check (before standard exit conditions)
        side = pos["side"]
        pct_ret_live = (price - pos["entry_price"]) / pos["entry_price"] if side == "long" else (pos["entry_price"] - price) / pos["entry_price"]
        if pct_ret_live <= -stop_thresh:
            trades.append({
                "pool": pool,
                "side": side,
                "entry_ts": pos["entry_ts"],
                "exit_ts": ts,
                "entry_price": pos["entry_price"],
                "exit_price": price,
                "entry_z": pos["entry_z"],
                "exit_z": z,
                "entry_mean_price": pos["entry_mean_price"],
                "exit_mean_price": mean_price,
                "pct_return": pct_ret_live - fee_frac,
                "held_rows": idx - pos["entry_idx"],
            })
            open_pos.pop(pool)
            last_exit_ts[pool] = ts
            continue

        # Exit conditions
        held = idx - pos["entry_idx"]
        side = pos["side"]
        exit_cond = False
        if side == "long" and z >= -exit_z:
            exit_cond = True
        elif side == "short" and z <= exit_z:
            exit_cond = True
        elif held >= max_hold:
            exit_cond = True  # time stop

        if exit_cond:
            pct_ret = (price - pos["entry_price"]) / pos["entry_price"] if side == "long" else (pos["entry_price"] - price) / pos["entry_price"]
            trades.append({
                "pool": pool,
                "side": side,
                "entry_ts": pos["entry_ts"],
                "exit_ts": ts,
                "entry_price": pos["entry_price"],
                "exit_price": price,
                "entry_z": pos["entry_z"],
                "exit_z": z,
                "entry_mean_price": pos["entry_mean_price"],
                "exit_mean_price": mean_price,
                "pct_return": pct_ret - fee_frac,
                "held_rows": held,
            })
            open_pos.pop(pool)
            last_exit_ts[pool] = ts

    return pd.DataFrame(trades)

# ─────────────────────────── reporting ───────────────────────────

def aggregate_returns(trades: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if trades.empty:
        return pd.DataFrame(), pd.DataFrame()
    trades["entry_ts"] = pd.to_datetime(trades["entry_ts"], utc=True)
    trades["exit_ts"]  = pd.to_datetime(trades["exit_ts"], utc=True)

    trades["year"]  = trades["exit_ts"].dt.year
    trades["month"] = trades["exit_ts"].dt.to_period("M")

    monthly = trades.groupby("month").agg(sum_return=("pct_return","sum"), sum_profit=("pnl","sum"))
    yearly  = trades.groupby("year").agg(sum_return=("pct_return","sum"), sum_profit=("pnl","sum"))
    return monthly, yearly

# ─────────────────────────── CLI ─────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Mean-reversion bot for stable/ETH pools")
    p.add_argument("--data", required=True, type=str, help="Directory with *ETH*_Swap.csv files")
    p.add_argument("--freq", type=str, default="60s", help="Resample frequency (pandas offset string, default 60s)")
    p.add_argument("--thr",  type=float, default=0.10, help="(pct mode) Deviation threshold in percent (default 0.10)")
    p.add_argument("--mode", choices=["pct","zscore"], default="zscore", help="Back-test mode: pct (old) or zscore (improved)")
    p.add_argument("--lookback", type=int, default=1440, help="(zscore) Rolling window length in rows (default 1440 ≈1d @60s)")
    p.add_argument("--entry_z", type=float, default=3.0, help="(zscore) |Z| threshold to open trade (default 3.0 – more selective)")
    p.add_argument("--exit_z",  type=float, default=0.3, help="(zscore) |Z| threshold to close trade (default 0.3)")
    p.add_argument("--max_hold", type=int, default=10080, help="(zscore) Max holding period in rows (default 10080 ≈1w @60s)")
    p.add_argument("--stop_pct", type=float, default=2.0, help="Stop-loss threshold in percent (default 2.0)")
    p.add_argument("--open_fee_pct", type=float, default=0.01, help="Fee percentage charged when opening a position (default 0.01)")
    p.add_argument("--report", action="store_true", help="Print trades & performance summary")
    p.add_argument("--notional", type=float, default=1000.0, help="Trade notional size in stable units per position (default 1000)")
    p.add_argument("--start_capital", type=float, default=80000.0, help="Initial capital in stable units (default 80000)")
    p.add_argument("--trades_csv", type=str, default="trades.csv", help="Output CSV file path for executed trades (default trades.csv)")
    p.add_argument("--plot_pnl", action="store_true", help="Display cumulative PnL graph (stable units)")

    args = p.parse_args()

    hr("LOADING DATA")
    csvs = list_swap_csvs(args.data)
    events_df, pools = build_event_stream(csvs)
    print(f"✓ Loaded event stream: {len(events_df)} events across {len(pools)} pools")

    hr("SIMULATING")
    if args.mode == "pct":
        trades = simulate_mean_reversion_events(events_df, pools, thr_pct=args.thr, stop_pct=args.stop_pct, open_fee_pct=args.open_fee_pct)
    else:
        trades = simulate_zscore_reversion_events(
            events_df, pools,
            lookback=args.lookback,
            entry_z=args.entry_z,
            exit_z=args.exit_z,
            max_hold=args.max_hold,
            stop_pct=args.stop_pct,
            open_fee_pct=args.open_fee_pct,
        )
    print(f"✓ Completed: {len(trades)} trades generated")

    # Compute PnL for each trade based on fixed notional and persist to CSV
    trades.insert(0, "id", np.arange(1, len(trades)+1))  # numeric trade id
    trades["pnl"] = trades["pct_return"] * args.notional  # profit/loss in stable units
    trades["hit"] = trades["pct_return"] > 0  # true if trade profitable
    trades.to_csv(args.trades_csv, index=False)

    monthly, yearly = aggregate_returns(trades)

    if args.report:
        hr("TRADES (head)")
        print(trades.head())
        hr("MONTHLY RETURNS (Σ pct & profit)")
        print(monthly.tail(24))
        hr("YEARLY RETURNS (Σ pct & profit)")
        print(yearly)

    # Quick summary always
    hr("SUMMARY")
    total_pct = trades["pct_return"].sum() if not trades.empty else 0.0
    total_profit = trades["pnl"].sum() if not trades.empty else 0.0
    final_balance = args.start_capital + total_profit
    win_trades = trades["hit"].sum() if not trades.empty else 0
    win_rate = (win_trades / len(trades) * 100) if not trades.empty else 0.0

    print(f"Total strategy return over back-test: {total_pct*100:.2f}% (un-compounded) across {len(trades)} trades")
    print(f"Win rate: {win_trades}/{len(trades)} trades = {win_rate:.2f}%")
    print(f"Total profit: {total_profit:.2f} stable units (starting capital {args.start_capital:.2f} → final balance {final_balance:.2f})\n")

    # Optional PnL plot
    if args.plot_pnl and not trades.empty:
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("matplotlib is not installed. Install it to enable plotting (pip install matplotlib).")
        else:
            hr("PnL GRAPH")
            cum = trades.sort_values("exit_ts").copy()
            cum["cum_pnl"] = cum["pnl"].cumsum()
            plt.figure(figsize=(10,6))
            plt.plot(cum["exit_ts"], cum["cum_pnl"], label="Cumulative PnL")
            plt.xlabel("Exit timestamp")
            plt.ylabel("Cumulative PnL (stable units)")
            plt.title("Cumulative Strategy PnL Over Time")
            plt.grid(True)
            plt.legend()
            plt.tight_layout()
            plt.show()

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
    main()
