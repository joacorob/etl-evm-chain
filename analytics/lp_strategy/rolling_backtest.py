"""Rolling-window backtest.

Splits the swap stream into overlapping windows (default 7d step 3d) and runs
each strategy on each window. Produces per-window returns, APR distribution,
monthly aggregates and a picture of which council agents won skill weight.
"""
from __future__ import annotations
import argparse
import json
import os
import statistics
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd

from lp_simulator import load_swaps, simulate
from council_strategy import CouncilStrategy, StaticRangeStrategy, FullRangeStrategy


def iso_day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--usdc-side", choices=["t0", "t1"], required=True)
    p.add_argument("--fee-pips", type=int, default=500)
    p.add_argument("--tick-spacing", type=int, default=10)
    p.add_argument("--capital", type=float, default=10000.0)
    p.add_argument("--window-days", type=float, default=7.0)
    p.add_argument("--step-days", type=float, default=3.0)
    p.add_argument("--out", default="analytics/lp_strategy/rolling_results.json")
    args = p.parse_args()

    dec0, dec1 = (6, 18) if args.usdc_side == "t0" else (18, 6)
    print(f"[rolling] loading {args.csv}…", flush=True)
    swaps = load_swaps(args.csv, dec0, dec1)
    if swaps.empty:
        print("empty"); return
    t0 = int(swaps.iloc[0].timestamp)
    t1 = int(swaps.iloc[-1].timestamp)
    span_days = (t1 - t0) / 86400.0
    print(f"[rolling] {len(swaps):,} swaps · {span_days:.1f} days ({iso_day(t0)} → {iso_day(t1)})", flush=True)

    window_s = int(args.window_days * 86400)
    step_s = int(args.step_days * 86400)
    starts = []
    t = t0
    while t + window_s <= t1:
        starts.append(t)
        t += step_s
    print(f"[rolling] {len(starts)} windows × {args.window_days} days, step {args.step_days} days", flush=True)

    # Persistent council that carries learned skill weights across windows.
    persistent_council = CouncilStrategy(
        dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing,
    )
    persistent_council.name = "council_learned"

    def build_strategies():
        return [
            FullRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing),
            StaticRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing, width_bps=50),
            StaticRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing, width_bps=150),
            StaticRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing, width_bps=300),
            StaticRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing, width_bps=500),
            StaticRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing, width_bps=800),
            StaticRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing, width_bps=1500),
            CouncilStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing),
            persistent_council,
        ]

    per_window: list[dict] = []
    strategy_names: list[str] = []
    council_final_skills: dict[str, list[float]] = defaultdict(list)

    for i, ws in enumerate(starts):
        we = ws + window_s
        win = swaps[(swaps.timestamp >= ws) & (swaps.timestamp < we)].reset_index(drop=True)
        if len(win) < 500:
            continue
        row = {"window_start": iso_day(ws), "window_end": iso_day(we - 1), "n_swaps": int(len(win))}
        persistent_council.reset_history_keep_skill()
        strategies = build_strategies()
        for s in strategies:
            r = simulate(win, s, initial_capital_t1=args.capital,
                         dec0=dec0, dec1=dec1, fee_pips=args.fee_pips)
            name = s.name
            ret = (r.final_value_t1 - args.capital) / args.capital
            hodl = (r.hodl_value_t1 - args.capital) / args.capital
            excess = ret - hodl
            days = max((we - ws) / 86400.0, 1e-9)
            apr = ret * 365.0 / days
            row[f"{name}_ret"] = ret
            row[f"{name}_hodl"] = hodl
            row[f"{name}_excess"] = excess
            row[f"{name}_apr"] = apr
            row[f"{name}_in_range"] = r.in_range_frac
            row[f"{name}_rebal"] = r.n_rebalances
            if name not in strategy_names:
                strategy_names.append(name)
            if isinstance(s, CouncilStrategy) and s.members:
                for m in s.members:
                    council_final_skills[m.name].append(m.skill)
        per_window.append(row)
        if (i + 1) % 5 == 0 or i == len(starts) - 1:
            print(f"[rolling] {i+1}/{len(starts)} windows done ({iso_day(ws)})", flush=True)

    # Aggregate
    summary = {}
    for name in strategy_names:
        rets = [w[f"{name}_ret"] for w in per_window if f"{name}_ret" in w]
        excs = [w[f"{name}_excess"] for w in per_window if f"{name}_excess" in w]
        aprs = [w[f"{name}_apr"] for w in per_window if f"{name}_apr" in w]
        if not rets:
            continue
        wins = sum(1 for e in excs if e > 0)
        summary[name] = {
            "n_windows": len(rets),
            "mean_return": statistics.mean(rets),
            "median_return": statistics.median(rets),
            "mean_excess": statistics.mean(excs),
            "median_excess": statistics.median(excs),
            "mean_apr": statistics.mean(aprs),
            "median_apr": statistics.median(aprs),
            "win_rate_vs_hodl": wins / len(excs),
            "stdev_return": statistics.pstdev(rets),
            "best": max(rets),
            "worst": min(rets),
        }

    # average final skill per agent across windows
    agent_skill_avg = {
        k: statistics.mean(v) for k, v in council_final_skills.items()
    }
    top_agents = sorted(agent_skill_avg.items(), key=lambda x: -x[1])[:20]
    bottom_agents = sorted(agent_skill_avg.items(), key=lambda x: x[1])[:10]

    payload = {
        "csv": args.csv,
        "span_days": span_days,
        "n_windows": len(per_window),
        "window_days": args.window_days,
        "step_days": args.step_days,
        "summary": summary,
        "per_window": per_window,
        "top_agents": top_agents,
        "bottom_agents": bottom_agents,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"[rolling] wrote {args.out}", flush=True)

    # Pretty print
    print("\n=== SUMMARY (rolling windows) ===", flush=True)
    print(f"{'strategy':<20} {'n':>4} {'mean ret':>10} {'med ret':>10} {'mean exc':>10} {'win%':>6} {'mean APR':>10} {'best':>8} {'worst':>8}")
    for name, s in sorted(summary.items(), key=lambda x: -x[1]["mean_excess"]):
        print(f"{name:<20} {s['n_windows']:>4} {s['mean_return']:>+10.4%} {s['median_return']:>+10.4%} {s['mean_excess']:>+10.4%} {s['win_rate_vs_hodl']:>5.0%} {s['mean_apr']:>+10.2%} {s['best']:>+8.2%} {s['worst']:>+8.2%}")

    print("\n=== TOP-20 council agents by avg skill ===")
    for name, sk in top_agents:
        print(f"  {name:<25} {sk:.3f}")
    print("\n=== BOTTOM-10 council agents ===")
    for name, sk in bottom_agents:
        print(f"  {name:<25} {sk:.3f}")


if __name__ == "__main__":
    main()
