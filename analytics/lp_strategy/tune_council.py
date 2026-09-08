"""Grid-search the council strategy hyperparameters on a given dataset.

Quick-and-dirty: vary (width_floor, rebalance_min_secs, decision_every_n_swaps)
and pick the best by excess-vs-HODL. Prints a leaderboard.
"""
from __future__ import annotations
import argparse
import itertools

from lp_simulator import load_swaps, simulate
from council_strategy import CouncilStrategy


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--usdc-side", choices=["t0", "t1"], required=True)
    p.add_argument("--fee-pips", type=int, default=500)
    p.add_argument("--tick-spacing", type=int, default=10)
    p.add_argument("--capital", type=float, default=10000.0)
    args = p.parse_args()
    dec0, dec1 = (6, 18) if args.usdc_side == "t0" else (18, 6)
    swaps = load_swaps(args.csv, dec0, dec1)
    print(f"loaded {len(swaps):,} swaps")

    floors = [15, 25, 40, 60]
    cooldowns_h = [1, 2, 3, 6]
    cadences = [100, 200, 400]

    results = []
    for floor, cd_h, cad in itertools.product(floors, cooldowns_h, cadences):
        s = CouncilStrategy(
            dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing,
            width_floor_bps=floor, rebalance_min_secs=cd_h * 3600, decision_every_n_swaps=cad,
        )
        r = simulate(swaps, s, initial_capital_t1=args.capital,
                     dec0=dec0, dec1=dec1, fee_pips=args.fee_pips)
        ret = (r.final_value_t1 - args.capital) / args.capital
        hodl = (r.hodl_value_t1 - args.capital) / args.capital
        excess = ret - hodl
        results.append((excess, ret, hodl, r.in_range_frac, r.n_rebalances, floor, cd_h, cad))

    results.sort(reverse=True)
    print(f"\n{'floor':>6} {'cd_h':>5} {'cad':>5} {'return':>10} {'excess':>10} {'in_range':>9} {'rebal':>6}")
    for excess, ret, hodl, ir, nb, fl, cd, ca in results[:20]:
        print(f"{fl:>6} {cd:>5} {ca:>5} {ret:>+10.4%} {excess:>+10.4%} {ir:>8.1%} {nb:>6}")


if __name__ == "__main__":
    main()
