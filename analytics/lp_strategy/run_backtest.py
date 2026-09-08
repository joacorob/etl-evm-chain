"""End-to-end backtest runner.

Runs the council strategy and several baselines on a swap CSV and prints a
side-by-side performance summary. Supports either token orientation (USDC as
token0 or token1) via the --usdc-side flag.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from dataclasses import asdict

import pandas as pd

from lp_simulator import load_swaps, simulate
from council_strategy import CouncilStrategy, StaticRangeStrategy, FullRangeStrategy
from v3_math import Q96


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--usdc-side", choices=["t0", "t1", "stable"], required=True,
                   help="t0=USDC is token0; t1=USDC is token1; stable=both 6dec stables")
    p.add_argument("--fee-pips", type=int, default=500)
    p.add_argument("--tick-spacing", type=int, default=10)
    p.add_argument("--capital", type=float, default=10000.0)
    p.add_argument("--out", default="analytics/lp_strategy/backtest_results.json")
    p.add_argument("--report", default="analytics/lp_strategy/report.html")
    args = p.parse_args()

    if args.usdc_side == "t0":
        dec0, dec1 = 6, 18  # USDC, WETH
        invert_for_display = True
    elif args.usdc_side == "t1":
        dec0, dec1 = 18, 6  # WETH, USDC
        invert_for_display = False
    elif args.usdc_side == "stable":
        dec0, dec1 = 6, 6  # USDC, USDbC
        invert_for_display = False
    else:
        raise ValueError(args.usdc_side)

    print(f"[run] loading {args.csv}…", flush=True)
    swaps = load_swaps(args.csv, dec0, dec1)
    print(f"[run] {len(swaps):,} swaps spanning blocks {swaps.block_number.min()}–{swaps.block_number.max()}", flush=True)
    span_secs = int(swaps.timestamp.max() - swaps.timestamp.min())
    span_days = span_secs / 86400
    print(f"[run] window: {span_days:.2f} days", flush=True)
    sample_price = swaps["price"].iloc[len(swaps) // 2]
    if invert_for_display and sample_price > 0:
        print(f"[run] sample USD/ETH ≈ {1/sample_price:,.2f}", flush=True)
    else:
        print(f"[run] sample USD/ETH ≈ {sample_price:,.2f}", flush=True)

    if args.usdc_side == "stable":
        widths = [2, 5, 10, 25, 50]
    else:
        widths = [50, 100, 200, 500, 1000]
    strategies = [
        FullRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing),
    ] + [
        StaticRangeStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing, width_bps=w)
        for w in widths
    ] + [
        CouncilStrategy(dec0=dec0, dec1=dec1, fee_pips=args.fee_pips, tick_spacing=args.tick_spacing),
    ]

    results = []
    for s in strategies:
        print(f"[run] simulating {s.name}…", flush=True)
        r = simulate(
            swaps,
            s,
            initial_capital_t1=args.capital,
            dec0=dec0,
            dec1=dec1,
            fee_pips=args.fee_pips,
        )
        # convert to USD-equivalent metrics
        # capital_t1 is in token1 units. If token1 is USDC, that's USD.
        # If token1 is WETH (mainnet), convert at the *initial* price.
        if dec1 == 6:  # token1 is USDC → token1 == USD
            usd_per_t1 = 1.0
        else:
            # token1 is WETH → multiply by ETH price (use median of period)
            mid_sqrtP = int(swaps.iloc[len(swaps) // 2].sqrtPriceX96)
            price_t1_per_t0 = (mid_sqrtP / Q96) ** 2 * (10 ** (dec0 - dec1))
            # price = WETH per USDC; ETH/USD = 1 / price
            usd_per_t1 = 1.0 / price_t1_per_t0  # USD per WETH
        # but capital was specified in t1 units; if t1 is WETH, capital is in WETH.
        # To make this comparable across pools, we report APR which is unit-free.
        days = max(span_days, 1e-9)
        ret = (r.final_value_t1 - args.capital) / args.capital
        hodl_ret = (r.hodl_value_t1 - args.capital) / args.capital
        excess = ret - hodl_ret
        apr = ret * 365.0 / days
        hodl_apr = hodl_ret * 365.0 / days
        results.append({
            "strategy": s.name,
            "final_value_t1": r.final_value_t1,
            "hodl_value_t1": r.hodl_value_t1,
            "fees_t1": r.fees_t1,
            "n_rebalances": r.n_rebalances,
            "in_range_frac": r.in_range_frac,
            "n_swaps_in_range": r.n_swaps_in_range,
            "n_swaps_total": r.n_swaps_total,
            "return": ret,
            "hodl_return": hodl_ret,
            "excess_return": excess,
            "apr": apr,
            "hodl_apr": hodl_apr,
        })
        print(f"        ret={ret:+.4%}  hodl={hodl_ret:+.4%}  excess={excess:+.4%}  APR≈{apr:+.2%}  in_range={r.in_range_frac:.1%}  rebal={r.n_rebalances}", flush=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    payload = {
        "csv": args.csv,
        "usdc_side": args.usdc_side,
        "fee_pips": args.fee_pips,
        "n_swaps": int(len(swaps)),
        "span_days": span_days,
        "results": results,
    }
    with open(args.out, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"[run] wrote {args.out}", flush=True)

    # Quick HTML report
    rows = "".join(
        f"<tr><td>{r['strategy']}</td><td>{r['return']:+.4%}</td><td>{r['hodl_return']:+.4%}</td>"
        f"<td>{r['excess_return']:+.4%}</td><td>{r['apr']:+.2%}</td><td>{r['fees_t1']:.4f}</td>"
        f"<td>{r['in_range_frac']:.1%}</td><td>{r['n_rebalances']}</td></tr>"
        for r in results
    )
    html = f"""<!doctype html><html><head><meta charset='utf-8'>
<title>LP backtest – {os.path.basename(args.csv)}</title>
<style>body{{font-family:system-ui,sans-serif;max-width:880px;margin:24px auto}}
table{{border-collapse:collapse;width:100%}}td,th{{padding:6px 10px;border-bottom:1px solid #ddd;text-align:right}}
th:first-child,td:first-child{{text-align:left}}h1{{font-size:20px}}small{{color:#888}}</style></head>
<body><h1>Uniswap v3 LP Backtest</h1>
<small>Pool: {args.csv} · fee {args.fee_pips/1e4:.3%} · {len(swaps):,} swaps · {span_days:.1f} days</small>
<table><thead><tr><th>strategy</th><th>return</th><th>hodl</th><th>excess</th><th>APR</th><th>fees (t1)</th><th>in-range</th><th>rebals</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""
    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w") as f:
        f.write(html)
    print(f"[run] wrote {args.report}", flush=True)


if __name__ == "__main__":
    main()
