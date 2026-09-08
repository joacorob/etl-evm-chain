#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mean-reversion bot ‑- version 2
===============================

Back-tests a simple mean-reversion strategy on Uniswap v3 Stable/ETH pools
(DAI/ETH, USDC/ETH, USDT/ETH across the 0.01 %, 0.05 % and 0.3 % fee tiers).

Rules (confirmed by user):
1. Global reference price = simple mean of the latest price observed for *each* pool.
2. Deviation  dev = (price_pool − price_global)/price_global.
3. Entry: when |dev| ≥ thr (%) *and* |dev| exceeds the opening fee.
   • LONG  if dev < −thr  (buy ETH with stable)
   • SHORT if dev > +thr  (sell/borrow ETH for stable)
   Notional is fixed: 1 000 stable units (USDC/DAI/USDT).
4. Exit: first of
   • dev crosses 0 (mean reversion) **with a strictly later timestamp**
   • stop-loss: unrealised loss ≤ −stop_pct (%)
5. Cost: open_fee_pct (%) is subtracted from the PnL of every trade.
6. Outputs: CSV of trades + monthly & yearly PnL in stable USD.

CLI example:
    python analytics/stable_eth_meanrevert_bot_v2.py \
        --data ./data \
        --thr 0.10 \
        --stop_pct 2.0 \
        --open_fee_pct 0.01 \
        --notional 1000 \
        --start_capital 80000 \
        --trades_csv my_trades_v2.csv \
        --report
"""

import argparse, os, re, glob, warnings
from typing import Dict, List, Tuple, Any

import numpy as np
import pandas as pd
from tqdm import tqdm

# ─────────────────────────── constants ────────────────────────────
STABLES = ("DAI", "USDC", "USDT")
FEES    = ("100", "500", "3000")
FILE_RGX = re.compile(r"(?P<stable>DAI|USDC|USDT)ETH(?P<fee>100|500|3000)_Swap\.csv$", re.IGNORECASE)
LOG_BASE = 1.0001  # Uniswap v3 tick base
_PRICE_MIN, _PRICE_MAX = 50.0, 10_000.0  # plausible bounds in USD/ETH

# Map stable symbol → decimals (used to choose the correct tick orientation)
STABLE_DECIMALS = {
    "DAI": 18,
    "USDC": 6,
    "USDT": 6,
}

# ─────────────────── Tick → price helpers ─────────────────────────

def tick_to_price_usd_per_eth(tick: int, stable: str) -> float:
    """Convert *tick* → USD/ETH price using heuristics.

    We don't know which token is token0.  Try both orientations and pick the
    first price within realistic bounds.
    """
    dec_s = STABLE_DECIMALS[stable]
    price1 = (LOG_BASE ** (-tick)) * (10 ** (dec_s - 18))
    price2 = (LOG_BASE ** (-tick)) * (10 ** (18 - dec_s))
    candidates = [price1, price2, 1.0/price1 if price1 else np.inf, 1.0/price2 if price2 else np.inf]
    for p in candidates:
        if _PRICE_MIN <= p <= _PRICE_MAX:
            return p
    # Fallback – median of all
    return float(np.median(candidates))

# ────────────────────── CSV helpers ───────────────────────────────

def list_swap_csvs(data_dir: str) -> List[str]:
    """Return list of *stable*ETH*Swap.csv inside *data_dir*."""
    files = sorted(glob.glob(os.path.join(data_dir, "*ETH*_Swap.csv")))
    keep  = [f for f in files if FILE_RGX.search(os.path.basename(f))]
    if not keep:
        raise FileNotFoundError(f"No swap CSVs found in {data_dir}")
    return keep


def _read_one_csv(path: str, usecols=("timestamp", "tick")) -> pd.DataFrame:
    """Read CSV → DataFrame[timestamp, tick]."""
    df = pd.read_csv(path, usecols=list(usecols), on_bad_lines="skip", engine="c")
    df = df.dropna(subset=["timestamp", "tick"]).copy()
    df["tick"] = pd.to_numeric(df["tick"], errors="coerce").astype(np.int32)
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(np.int64), unit="s", utc=True)
    return df.sort_values("timestamp")


def csv_to_price_events(path: str) -> pd.DataFrame:
    """Convert one swap CSV → DataFrame[timestamp, pool, price]."""
    m = FILE_RGX.search(os.path.basename(path))
    if not m:
        raise ValueError(f"Unexpected filename {path}")
    stable, fee = m.group("stable").upper(), m.group("fee")
    df = _read_one_csv(path)
    df["price"] = df["tick"].apply(lambda t: tick_to_price_usd_per_eth(int(t), stable))
    # Filter outliers
    df = df[(df["price"] >= _PRICE_MIN) & (df["price"] <= _PRICE_MAX)]
    pool = f"{stable}ETH{fee}"
    return pd.DataFrame({
        "timestamp": df["timestamp"],
        "pool": pool,
        "price": df["price"],
    })


def build_event_stream(csv_paths: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """Aggregate all CSVs into a single event stream sorted by timestamp."""
    evts: List[pd.DataFrame] = []
    for p in tqdm(csv_paths, desc="Reading CSVs"):
        ev = csv_to_price_events(p)
        if not ev.empty:
            evts.append(ev)
    if not evts:
        raise ValueError("No valid rows after filtering")
    all_events = pd.concat(evts, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
    pools = sorted(all_events["pool"].unique())
    return all_events, pools

# ──────────────────── Simulation (pct mode) ───────────────────────

def simulate_pct(events: pd.DataFrame,
                 pools: List[str],
                 *,
                 thr_pct: float = 0.10,
                 stop_pct: float = 2.0,
                 open_fee_pct: float = 0.01) -> pd.DataFrame:
    """Run mean-reversion simulation in *pct* mode."""
    threshold  = thr_pct / 100.0
    stop_thr   = stop_pct / 100.0
    fee_frac   = open_fee_pct / 100.0

    last_price: Dict[str, float] = {}
    last_exit_ts: Dict[str, pd.Timestamp] = {}
    open_pos: Dict[str, Dict[str, Any]] = {}
    trades: List[Dict[str, Any]] = []

    for idx, row in events.iterrows():
        ts, pool, price = row["timestamp"], row["pool"], row["price"]
        last_price[pool] = price

        # Need at least one price for every pool to compute global mean
        if len(last_price) < len(pools):
            continue

        global_mean = float(np.mean(list(last_price.values())))
        dev = (price - global_mean) / global_mean  # +ve ⇒ overpriced

        pos = open_pos.get(pool)

        # ───── Entry logic ─────────────────────────────
        if pos is None:
            if abs(dev) < threshold or abs(dev) <= fee_frac:
                continue  # Not enough deviation or doesn’t cover fees
            # avoid reopen at same timestamp right after closing
            if last_exit_ts.get(pool) == ts:
                continue
            side = "long" if dev < 0 else "short"
            open_pos[pool] = {
                "side": side,
                "entry_ts": ts,
                "entry_price": price,
                "entry_dev": dev,
                "entry_mean_price": global_mean,
            }
            continue

        # ───── Exit logic ──────────────────────────────
        side = pos["side"]
        # Live return for stop-loss
        pct_ret_live = (price - pos["entry_price"]) / pos["entry_price"] if side == "long" else (pos["entry_price"] - price) / pos["entry_price"]
        if pct_ret_live <= -stop_thr:
            trades.append({
                "pool": pool,
                "side": side,
                "entry_ts": pos["entry_ts"],
                "exit_ts": ts,
                "entry_price": pos["entry_price"],
                "exit_price": price,
                "entry_dev": pos["entry_dev"],
                "exit_dev": dev,
                "entry_mean_price": pos["entry_mean_price"],
                "exit_mean_price": global_mean,
                "pct_return": pct_ret_live - fee_frac,
            })
            open_pos.pop(pool)
            last_exit_ts[pool] = ts
            continue

        # Mean reversion (cross 0) with strictly later timestamp
        crossed = (side == "long" and dev >= 0) or (side == "short" and dev <= 0)
        if crossed and ts > pos["entry_ts"]:
            pct_ret = (price - pos["entry_price"]) / pos["entry_price"] if side == "long" else (pos["entry_price"] - price) / pos["entry_price"]
            trades.append({
                "pool": pool,
                "side": side,
                "entry_ts": pos["entry_ts"],
                "exit_ts": ts,
                "entry_price": pos["entry_price"],
                "exit_price": price,
                "entry_dev": pos["entry_dev"],
                "exit_dev": dev,
                "entry_mean_price": pos["entry_mean_price"],
                "exit_mean_price": global_mean,
                "pct_return": pct_ret - fee_frac,
            })
            open_pos.pop(pool)
            last_exit_ts[pool] = ts

    return pd.DataFrame(trades)

# ─────────────────────── Reporting helpers ────────────────────────

def aggregate_returns(trades: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if trades.empty:
        return pd.DataFrame(), pd.DataFrame()
    trades = trades.copy()
    trades["exit_ts"] = pd.to_datetime(trades["exit_ts"], utc=True)
    trades["month"] = trades["exit_ts"].dt.to_period("M")
    trades["year"]  = trades["exit_ts"].dt.year
    monthly = trades.groupby("month").agg(sum_return=("pct_return", "sum"), sum_profit=("pnl", "sum"))
    yearly  = trades.groupby("year").agg(sum_return=("pct_return", "sum"), sum_profit=("pnl", "sum"))
    return monthly, yearly

# ───────────────────────────── CLI ────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Mean-reversion bot v2 for stable/ETH pools (pct mode)")
    p.add_argument("--data", required=True, type=str, help="Directory with *ETH*_Swap.csv files")
    p.add_argument("--thr", type=float, default=0.10, help="Deviation threshold in percent (default 0.10)")
    p.add_argument("--stop_pct", type=float, default=2.0, help="Stop-loss threshold in percent (default 2.0)")
    p.add_argument("--open_fee_pct", type=float, default=0.01, help="Fee charged when opening a position (default 0.01)")
    p.add_argument("--notional", type=float, default=1000.0, help="Notional per trade in stable units (default 1000)")
    p.add_argument("--start_capital", type=float, default=80_000.0, help="Initial capital (stable units)")
    p.add_argument("--trades_csv", type=str, default="trades_v2.csv", help="Output CSV for executed trades")
    p.add_argument("--report", action="store_true", help="Print trades & performance summary")
    p.add_argument("--plot_pnl", action="store_true", help="Plot cumulative PnL (requires matplotlib)")
    args = p.parse_args()

    print("\n==== LOADING DATA ====")
    csv_paths = list_swap_csvs(args.data)
    events_df, pools = build_event_stream(csv_paths)
    print(f"Loaded {len(events_df):,} price events across {len(pools)} pools")

    print("\n==== SIMULATING ====")
    trades = simulate_pct(events_df, pools, thr_pct=args.thr, stop_pct=args.stop_pct, open_fee_pct=args.open_fee_pct)
    print(f"Simulation finished – {len(trades)} trades generated")

    # PnL & persistence
    trades = trades.copy()
    trades.insert(0, "id", np.arange(1, len(trades) + 1))
    trades["pnl"] = trades["pct_return"] * args.notional
    trades["hit"] = trades["pct_return"] > 0
    trades.to_csv(args.trades_csv, index=False)

    monthly, yearly = aggregate_returns(trades)

    # ─── Reporting ────────────────────────────────────────────
    if args.report:
        print("\n==== TRADES (head) ====")
        print(trades.head())
        print("\n==== MONTHLY RETURNS ====")
        print(monthly.tail(24))
        print("\n==== YEARLY RETURNS ====")
        print(yearly)

    total_pct   = trades["pct_return"].sum() if not trades.empty else 0.0
    total_profit = trades["pnl"].sum() if not trades.empty else 0.0
    final_balance = args.start_capital + total_profit
    win_rate = (trades["hit"].mean() * 100) if not trades.empty else 0.0

    print("\n==== SUMMARY ====")
    print(f"Total strategy return: {total_pct*100:.2f}% across {len(trades)} trades")
    print(f"Win rate: {win_rate:.2f}%")
    print(f"Total profit: {total_profit:.2f} (start {args.start_capital:.2f} → final {final_balance:.2f})\n")

    # Optional PnL plot
    if args.plot_pnl and not trades.empty:
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("matplotlib not installed – skipping plot")
        else:
            cum = trades.sort_values("exit_ts").copy()
            cum["cum_pnl"] = cum["pnl"].cumsum()
            plt.figure(figsize=(10, 6))
            plt.plot(cum["exit_ts"], cum["cum_pnl"], label="Cumulative PnL")
            plt.xlabel("Exit timestamp")
            plt.ylabel("Cumulative PnL (stable units)")
            plt.title("Cumulative Strategy PnL – v2")
            plt.grid(True)
            plt.legend()
            plt.tight_layout()
            plt.show()

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
    main()

