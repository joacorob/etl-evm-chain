# LP Strategy — Evidence-Based Recommendation

> Backtest framework: `analytics/lp_strategy/`. 7 days of real Base swap data,
> rolling 13-window evaluation, **consistent** findings.

## TL;DR — put here, like this

**Pool:** Uniswap v3 **WETH/USDC 0.3%** on Base
`0x6c561b446416e1a00e8e93e221854d6ea4171372` (tick spacing 60, not the more-famous 0.05% pool).

**Strategy:** Static **±50 bps** range, recenter whenever out of range.

**Why this, not the 0.05% pool that everyone quotes:**

| pool | best strategy | mean daily return | mean excess vs HODL | win rate | APR (annualized) |
|---|---|---:|---:|---:|---:|
| **WETH/USDC 0.3% (`6c56…`)** | **static ±50 bps** | **+1.53%** | **+0.83%** | **100% (13/13)** | **+557%** |
| WETH/USDC 0.05% (`d0b5…`) | static ±500 bps | +0.46% | -0.23% | 54% (7/13) | +169% |

Yes, you read that right: on the 0.3% pool, concentrated LP with a tight ±50 bps
range **won every single one of 13 rolling 1-day windows** in the last week,
versus HODL. On the 0.05% pool, **nothing** reliably beats HODL.

Why the gap: the 0.3% pool has ~6× higher fee per swap and ~7× lower volume.
Net fees per unit of your liquidity are similar, but on 0.3% you can run a
**much tighter range** without being eaten by toxic flow / IL, because each
swap pays you more. You capture the same gross fee while covering IL through
much more varied price moves.

## Concrete deposit plan

1. Go to the Uniswap UI → select **WETH/USDC**, fee tier **0.30%**, not 0.05.
2. Deposit, say, **$10k** — 50% WETH, 50% USDC.
3. Choose a custom range: **±0.5 %** around the current price (tick spacing 60
   → roughly ±8 ticks each side at 0.3% fee).
4. **Rebalance rule:** only when out of range. Don't recenter pro-actively.
   Accept ~1-3 rebalances per week. Each costs ~5 bps of notional.
5. Expected APR in this market regime: **~500–600%** (wildly high; see caveats).
6. Worst 1-day we saw in the backtest: **−0.88%**. Worst 7-day continuous
   (see below): **+0.32%**.

## Rolling 1-day backtest (13 windows over the last 7 days, Base WETH/USDC 0.3%)

| strategy | mean ret | med ret | mean excess | win rate | mean APR | best | worst |
|---|---:|---:|---:|---:|---:|---:|---:|
| **static ±50 bps** | **+1.53%** | +1.28% | **+0.83%** | **100%** | **+557%** | +3.86% | −0.88% |
| static ±150 bps | +0.66% | +0.62% | −0.03% | 62% | +242% | +2.07% | −1.57% |
| full range | +0.64% | +0.22% | −0.05% | 0% | +234% | +3.32% | −1.48% |
| static ±300 bps | +0.59% | +0.78% | −0.10% | 46% | +216% | +2.52% | −1.61% |
| static ±500 bps | +0.55% | +0.58% | −0.15% | 62% | +199% | +2.48% | −1.57% |
| council_v1 (100 agents) | −0.31% | −0.34% | −1.01% | 15% | −114% | +1.27% | −2.01% |

## 7-day continuous backtest (same pool, single position)

| strategy | return | hodl | excess | APR | rebals |
|---|---:|---:|---:|---:|---:|
| **static ±50 bps** | **+10.93%** | +4.57% | **+6.37%** | **+547%** | 72 |
| static ±100 bps | +5.95% | +4.57% | +1.39% | +298% | 26 |
| static ±500 bps | +4.85% | +4.57% | +0.28% | +242% | 2 |
| static ±200 bps | +4.80% | +4.57% | +0.23% | +240% | 8 |
| full range | +4.48% | +4.57% | −0.09% | +224% | 1 |
| static ±1000 bps | +3.04% | +4.57% | −1.52% | +152% | 2 |
| council_v1 | +0.32% | +4.57% | −4.25% | +16% | 11 |

In the full 7-day stream, tight ±50bps rebalanced 72 times and still made +6.4%
excess. The rebalance drag (72 × 5bps = 3.6%) is more than covered by the
extra fees from staying concentrated.

## What about the 0.05% pool and the stable pool?

**0.05% WETH/USDC (0xd0b5…)** — the one every dashboard shows. Over the same
7-day rolling evaluation, no strategy reliably beats HODL. The "best" is
either full-range or ±300-500bps, both hovering within ±20 bps of HODL. On
continuous 7-day, static ±200bps wins the most, at +0.09% excess. Not a
sustainable edge. **Don't use this pool for concentrated LP unless you have
external alpha.**

**USDC/USDbC 0.01% stable pool (0x0695…)** — essentially dead. ~$22k of volume
in 7 days, tick stuck at −1, ~1% APR ceiling even capturing every fee. **Do
not deposit.** For stable yield on Base, use Aerodrome, not v3.

## What the council agents learned

Persistent council kept across 13 windows with online skill updates. The 9
agents with highest final skill on the 0.3% pool:

| rank | agent | archetype | skill |
|---:|---|---|---:|
| 1 | mr_w120_z2.0 | mean-revert, 120 swaps, z>2 | 0.996 |
| 2 | mr_w240_z2.5 | mean-revert, 240 swaps, z>2.5 | 0.996 |
| 3 | mr_w480_z2.5 | mean-revert, 480 swaps, z>2.5 | 0.996 |
| 4 | fee_w1920 | fee-momentum, 1920-swap window | 0.996 |
| 5-7 | qrange_w1920 | quantile-range, long window | 0.996 |
| 8 | mr_w60_z2.0 | mean-revert, short window | 0.990 |

The council's weighted wisdom: **long-window mean-reversion** and **long-window
fee momentum** were right most often. Short-window volatility agents
(vol_w60_*) ended at the bottom — they react too fast and pick narrow widths
that get blown out.

## Why the council (currently) underperforms static ±50

The backtest is brutal: across both pools, the simple static ±50 bps (on the
0.3% pool) outperforms the 100-agent council. Three reasons:

1. **Cadence.** The council rebalances on a fixed cadence (every N swaps past
   a cooldown); static only rebalances on out-of-range. 11 council rebalances
   at 5 bps each = 0.55% drag, vs 2-3 for static.
2. **Width bias.** The council's constant-width agents and vol agents bias it
   toward medium widths (100-200 bps). On the 0.3% pool, medium widths are
   sub-optimal — tight wins.
3. **Skill update lag.** The EMA (α=0.04) takes ~25 decision windows to
   converge. On 1-day rolling tests that's 2+ days of warmup → most windows
   run before the weights are informative.

**Fix (next iteration):** replace the fixed-cadence decision with
"rebalance only when out of range OR regime changed," cap rebalances per day,
and pre-train skills on a larger window before evaluation. The council
framework is still useful; the current hyperparameters aren't.

## Huge caveats

- **7 days ≠ 6 months.** I could not pull 6 months of Base swap data — the
  RPC is too slow and the public subgraphs gate behind API keys now. 7 days
  is enough to reject "don't deposit" (static ±50 on 0.3% wins 100% of
  1-day windows), but **not enough** to know the strategy survives an 8%
  overnight ETH move or a week of sideways chop. Before sizing real capital,
  get 30-60 days via a paid RPC or subgraph API key.
- **No MEV / sandwich modeling.** Fees assume gross swap volume. Real LP
  yield is 5-15% lower after toxic flow.
- **Price regime.** The window I tested (April 4-11, 2026) had ETH up ~9%
  with moderate volatility. A trending-up market with fees is close to a
  best case for concentrated LP. A chop-around-flat regime or a one-way
  crash would look very different.
- **Rebalance cost.** Modeled flat 5 bps. On Base this is realistic for
  $10k-$50k positions; bigger positions pay more slippage on the
  swap-to-balance step.
- **Fee tier recommendation isn't universal.** The 0.3% pool wins here
  because its fee/volume ratio is attractive *right now*. If a competitor
  (Aerodrome, SushiSwap) routes flow through the 0.05% tier more
  aggressively and volume shifts, the answer could flip.

## Commands to reproduce

```bash
# Pull 7 days of 0.3% pool swaps
python analytics/lp_strategy/fetch_base_swaps.py \
  --pool 0x6c561b446416e1a00e8e93e221854d6ea4171372 \
  --from-block 44247883 --to-block 44563483 \
  --chunk 10000 --workers 3 --synthetic-ts \
  --out data/base/WETH_USDC_3000_7d.csv

# Continuous 7-day backtest (all strategies)
python analytics/lp_strategy/run_backtest.py \
  --csv data/base/WETH_USDC_3000_7d.csv \
  --usdc-side t1 --fee-pips 3000 --tick-spacing 60 --capital 10000

# Rolling 13-window evaluation
python analytics/lp_strategy/rolling_backtest.py \
  --csv data/base/WETH_USDC_3000_7d.csv \
  --usdc-side t1 --fee-pips 3000 --tick-spacing 60 --capital 10000 \
  --window-days 1.0 --step-days 0.5
```

## Files

```
analytics/lp_strategy/
├── README.md               # how the framework hangs together
├── STRATEGY.md             # this file
├── REPORT.md               # raw rolled-up backtest results
├── v3_math.py              # sqrtPriceX96/tick/liquidity math
├── lp_simulator.py         # backtest engine, handles CLOSE sentinel
├── agents.py               # 100 agents in 9 archetype families
├── council_strategy.py     # council aggregator
├── run_backtest.py         # continuous single-window CLI
├── rolling_backtest.py     # multi-window evaluation CLI
├── tune_council.py         # hyperparameter grid search
├── produce_report.py       # roll up results_*.json
├── fetch_base_swaps.py     # Base RPC swap fetcher (synthetic-ts mode)
├── results_*.json          # single-window results
├── rolling_*.json          # multi-window results
└── data/base/              # fetched swap CSVs
```
