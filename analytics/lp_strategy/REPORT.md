# Uniswap v3 LP — Backtest Results

Backtests of multiple strategies on Uniswap v3 pools, comparing a council-of-100-agents strategy against passive baselines.

**Methodology** — see `analytics/lp_strategy/README.md`. Fees accrue when the position covers the active tick, proportional to the position's share of pool liquidity. Rebalances cost 5 bps of position notional. HODL benchmark = 50/50 split at start, valued at end.

### WETH_USDC_3000_7d.csv
`fee=0.300%` · `n_swaps=36,347` · `window=7.30 d`

| strategy | return | hodl | excess | APR | in-range | rebal |
|---|---:|---:|---:|---:|---:|---:|
| static_50bps | +10.932% | +4.566% | +6.366% | +546.71% | 100.0% | 72 |
| static_100bps | +5.953% | +4.566% | +1.386% | +297.68% | 100.0% | 26 |
| static_500bps | +4.847% | +4.566% | +0.281% | +242.38% | 100.0% | 2 |
| static_200bps | +4.798% | +4.566% | +0.232% | +239.95% | 100.0% | 8 |
| full_range | +4.478% | +4.566% | -0.089% | +223.91% | 100.0% | 1 |
| static_1000bps | +3.043% | +4.566% | -1.523% | +152.19% | 100.0% | 2 |
| council_v1 | +0.321% | +4.566% | -4.245% | +16.05% | 80.7% | 11 |

### WETH_USDC_500_7d.csv
`fee=0.050%` · `n_swaps=176,754` · `window=7.31 d`

| strategy | return | hodl | excess | APR | in-range | rebal |
|---|---:|---:|---:|---:|---:|---:|
| static_200bps | +4.665% | +4.571% | +0.094% | +233.07% | 100.0% | 11 |
| full_range | +4.480% | +4.571% | -0.091% | +223.84% | 100.0% | 1 |
| static_500bps | +4.116% | +4.571% | -0.455% | +205.63% | 100.0% | 3 |
| static_1000bps | +3.056% | +4.571% | -1.515% | +152.70% | 100.0% | 2 |
| static_100bps | +1.664% | +4.571% | -2.907% | +83.16% | 100.0% | 46 |
| council_v1 | +1.461% | +4.571% | -3.110% | +73.00% | 94.1% | 29 |
| static_50bps | -2.933% | +4.571% | -7.505% | -146.57% | 100.0% | 158 |

### WETH_USDC_500_7d.csv
`fee=0.050%` · `n_swaps=176,754` · `window=7.31 d`

| strategy | return | hodl | excess | APR | in-range | rebal |
|---|---:|---:|---:|---:|---:|---:|
| static_200bps | +4.665% | +4.571% | +0.094% | +233.07% | 100.0% | 11 |
| full_range | +4.480% | +4.571% | -0.091% | +223.84% | 100.0% | 1 |
| static_500bps | +4.116% | +4.571% | -0.455% | +205.63% | 100.0% | 3 |
| static_1000bps | +3.056% | +4.571% | -1.515% | +152.70% | 100.0% | 2 |
| council_v1 | +2.307% | +4.571% | -2.264% | +115.26% | 81.9% | 14 |
| static_100bps | +1.664% | +4.571% | -2.907% | +83.16% | 100.0% | 46 |
| static_50bps | -2.933% | +4.571% | -7.505% | -146.57% | 100.0% | 158 |

### WETH_USDC_500_Swap.csv
`fee=0.050%` · `n_swaps=20,505` · `window=0.99 d`

| strategy | return | hodl | excess | APR | in-range | rebal |
|---|---:|---:|---:|---:|---:|---:|
| council_v1 | +0.923% | +0.764% | +0.158% | +338.84% | 76.0% | 4 |
| static_50bps | +0.866% | +0.764% | +0.101% | +317.90% | 100.0% | 12 |
| static_500bps | +0.843% | +0.764% | +0.079% | +309.72% | 100.0% | 1 |
| static_1000bps | +0.750% | +0.764% | -0.014% | +275.51% | 100.0% | 1 |
| static_200bps | +0.750% | +0.764% | -0.014% | +275.35% | 100.0% | 2 |
| full_range | +0.717% | +0.764% | -0.047% | +263.44% | 100.0% | 1 |
| static_100bps | +0.294% | +0.764% | -0.470% | +107.94% | 100.0% | 6 |

### WETH_USDC_500_Swap.csv
`fee=0.050%` · `n_swaps=20,505` · `window=0.99 d`

| strategy | return | hodl | excess | APR | in-range | rebal |
|---|---:|---:|---:|---:|---:|---:|
| council_v1 | +0.943% | +0.764% | +0.179% | +346.48% | 84.7% | 8 |
| static_50bps | +0.866% | +0.764% | +0.101% | +317.90% | 100.0% | 12 |
| static_500bps | +0.843% | +0.764% | +0.079% | +309.72% | 100.0% | 1 |
| static_1000bps | +0.750% | +0.764% | -0.014% | +275.51% | 100.0% | 1 |
| static_200bps | +0.750% | +0.764% | -0.014% | +275.35% | 100.0% | 2 |
| full_range | +0.717% | +0.764% | -0.047% | +263.44% | 100.0% | 1 |
| static_100bps | +0.294% | +0.764% | -0.470% | +107.94% | 100.0% | 6 |

### USDC_ETH_Pool_Swap.csv
`fee=0.050%` · `n_swaps=16,508` · `window=2.14 d`

| strategy | return | hodl | excess | APR | in-range | rebal |
|---|---:|---:|---:|---:|---:|---:|
| static_500bps | -0.530% | -1.449% | +0.919% | -90.54% | 100.0% | 1 |
| static_1000bps | -0.959% | -1.449% | +0.489% | -163.98% | 100.0% | 1 |
| full_range | -1.475% | -1.449% | -0.026% | -252.11% | 100.0% | 1 |
| council_v1 | -2.206% | -1.449% | -0.757% | -377.06% | 66.9% | 8 |
| static_200bps | -3.713% | -1.449% | -2.265% | -634.71% | 100.0% | 10 |
| static_100bps | -4.558% | -1.449% | -3.110% | -779.14% | 100.0% | 26 |
| static_50bps | -7.137% | -1.449% | -5.688% | -1219.86% | 100.0% | 79 |

### USDC_USDbC_100_Swap.csv
`fee=0.010%` · `n_swaps=2,226` · `window=6.99 d`

| strategy | return | hodl | excess | APR | in-range | rebal |
|---|---:|---:|---:|---:|---:|---:|
| static_50bps | -0.049% | +0.000% | -0.050% | -2.57% | 100.0% | 1 |
| static_100bps | -0.049% | +0.000% | -0.050% | -2.58% | 100.0% | 1 |
| static_200bps | -0.049% | +0.000% | -0.050% | -2.58% | 100.0% | 1 |
| static_500bps | -0.050% | +0.000% | -0.050% | -2.58% | 100.0% | 1 |
| full_range | -0.050% | +0.000% | -0.050% | -2.59% | 100.0% | 1 |
| council_v1 | -0.499% | +0.000% | -0.499% | -26.05% | 91.1% | 10 |
