"""Aggregate all backtest JSONs into one nice markdown + HTML report."""
from __future__ import annotations
import json, os, glob, sys


def fmt_pct(x: float) -> str:
    return f"{x * 100:+.3f}%"


def fmt_apr(x: float) -> str:
    return f"{x * 100:+.2f}%"


def render(payload: dict) -> str:
    out = []
    name = os.path.basename(payload["csv"])
    out.append(f"### {name}")
    out.append(f"`fee={payload['fee_pips']/1e6:.3%}` · `n_swaps={payload['n_swaps']:,}` · `window={payload['span_days']:.2f} d`")
    out.append("")
    out.append("| strategy | return | hodl | excess | APR | in-range | rebal |")
    out.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in sorted(payload["results"], key=lambda r: -r["excess_return"]):
        out.append(
            f"| {r['strategy']} | {fmt_pct(r['return'])} | {fmt_pct(r['hodl_return'])} | "
            f"{fmt_pct(r['excess_return'])} | {fmt_apr(r['apr'])} | {r['in_range_frac']:.1%} | {r['n_rebalances']} |"
        )
    out.append("")
    return "\n".join(out)


def main():
    json_dir = "analytics/lp_strategy"
    files = sorted(glob.glob(os.path.join(json_dir, "results_*.json")))
    if not files:
        print("no results", file=sys.stderr)
        sys.exit(1)
    md = ["# Uniswap v3 LP — Backtest Results\n"]
    md.append("Backtests of multiple strategies on Uniswap v3 pools, comparing a council-of-100-agents strategy against passive baselines.\n")
    md.append("**Methodology** — see `analytics/lp_strategy/README.md`. Fees accrue when the position covers the active tick, proportional to the position's share of pool liquidity. Rebalances cost 5 bps of position notional. HODL benchmark = 50/50 split at start, valued at end.\n")
    for f in files:
        with open(f) as h:
            md.append(render(json.load(h)))
    md_text = "\n".join(md)
    with open(os.path.join(json_dir, "REPORT.md"), "w") as h:
        h.write(md_text)
    print("wrote", os.path.join(json_dir, "REPORT.md"))


if __name__ == "__main__":
    main()
