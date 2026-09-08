# LP Strategy — Uniswap v3/v4 Council-of-100 Backtester

A backtesting framework for Uniswap v3 (and architecturally v4) concentrated-liquidity
positions, plus a multi-agent voting strategy that decides **when** to deploy and **at
what range**.

## Why this exists

Concentrated liquidity is profitable only when the position spends time *in range*.
Choosing the range is the entire problem: too tight and you eat IL plus rebalance
costs; too wide and you barely earn fees over HODL.

This module lets you:

1. Pull pool history (Swap events) for any v3 pool on Base via the indexer or the
   built-in `eth_getLogs` fetcher.
2. Backtest a position with realistic fee accrual (proportional to your share of
   pool liquidity, only when in range), IL via the standard v3 amount math, and a
   gas/slippage cost on every rebalance.
3. Compare a council strategy that aggregates 100 agent votes against passive
   baselines (full-range, fixed-width static).

## Architecture

```
fetch_base_swaps.py     → Pull Swap events from Base via JSON-RPC
v3_math.py              → Pure-Python sqrtPriceX96 / tick / liquidity math
lp_simulator.py         → Walk swap stream, accrue fees, IL, rebalance costs
agents.py               → 100 agents (vol, trend, mean-revert, regime, …)
council_strategy.py     → Aggregator + LP simulator adapter
run_backtest.py         → CLI entry point
produce_report.py       → Roll up multiple runs into one report
```

## The council (Plato / epistocracy)

100 agents in 9 archetype families (vol, trend, mean-revert, fee-momentum,
microstructure, regime, quantile-range, imbalance, constants). Each emits a
`Vote(deploy, width_bps, skew, confidence)`.

Aggregation:

```
weight_i      = skill_i * confidence_i
deploy        = sum(yes_weights) > sum(no_weights)
width         = weighted mean of widths from yes voters
skew          = weighted mean of skews from yes voters
```

Skill is updated online — agents that voted "deploy" when the next window saw
small price moves (good for fees, no IL blowup) get their skill nudged up;
ones that voted "deploy" right before a 5%+ move get nudged down. Over time
the weighted vote concentrates around agents that have been historically
right for the current pool — exactly the "voto ponderado por saber" idea.

## Running

```bash
# Pull data (Base WETH/USDC 0.05%)
python analytics/lp_strategy/fetch_base_swaps.py \
  --pool 0xd0b53D9277642d899DF5C87A3966A349A798F224 \
  --from-block 44515000 --to-block 44557944 \
  --chunk 1000 --workers 6 --synthetic-ts \
  --out data/base/WETH_USDC_500_Swap.csv

# Backtest
python analytics/lp_strategy/run_backtest.py \
  --csv data/base/WETH_USDC_500_Swap.csv \
  --usdc-side t1 \
  --fee-pips 500 --tick-spacing 10 \
  --capital 10000

# Produce roll-up report
python analytics/lp_strategy/produce_report.py
```

## Caveats

- Fee accrual assumes our position L is small relative to the pool's active
  liquidity. This is the standard approximation; for very large positions you
  would need to simulate the full tick book.
- Rebalance cost is a flat 5 bps. Realistic on Base for $10k positions.
- v4 hooks aren't simulated. The v4 architecture changes the *plumbing* but the
  range/fee math is identical to v3, so this framework transfers directly: only
  the data fetcher needs swapping (different event signature).
