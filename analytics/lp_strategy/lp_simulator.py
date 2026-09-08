"""Pool-aware Uniswap v3 LP backtest engine.

Given a swap stream (with tick / sqrtPriceX96 / liquidity / amounts), simulate
holding a concentrated-liquidity position and accrue fees only when in range,
proportional to the in-range share of the pool's active liquidity.

Fee accrual model
-----------------
For each swap with input volume `vol_in` (atomic, on the input side) the pool
takes `fee = vol_in * fee_pips / 1e6`. Of that, our position earns
`fee * L_pos / L_active`, but ONLY if our range covers the pre-swap tick.
This is the standard approximation used by every public v3 backtester
(uniswap-v3-backtest, gammaswap, revert.finance methodology) and matches the
on-chain math to first order, ignoring intra-swap range crossings.
"""
from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import Iterable, Iterator
import pandas as pd

from v3_math import (
    Q96,
    Position,
    open_position,
    tick_to_sqrt_price_x96,
    sqrt_price_x96_to_tick,
)


@dataclass
class SimResult:
    final_value_t1: float
    hodl_value_t1: float
    fees_t1: float
    n_rebalances: int
    in_range_frac: float
    n_swaps_in_range: int
    n_swaps_total: int
    equity_curve: list[tuple[int, float]] = field(default_factory=list)


def _swap_fee_in_token1(row, fee_pips: int) -> float:
    """Fee paid by this swap denominated in token1 atomic units."""
    a0 = row.amount0
    a1 = row.amount1
    sqrtP = row.sqrtPriceX96
    price = (sqrtP / Q96) ** 2  # t1_atomic per t0_atomic
    if a0 > 0:  # token0 is the input
        in_atomic_t1 = a0 * price
    else:  # token1 is the input
        in_atomic_t1 = float(a1)
    return abs(in_atomic_t1) * fee_pips / 1_000_000.0


@dataclass
class StrategyState:
    position: Position | None = None
    capital_t1: float = 0.0
    last_rebalance_ts: int = 0


CLOSE = "CLOSE"  # sentinel returned by `decide` to request closing the position


class Strategy:
    """Base class. Override `decide` to react to the current swap row."""

    name = "base"

    def decide(self, row, state: StrategyState):  # pragma: no cover
        """Return new (tick_lower, tick_upper) to open a position, the sentinel
        CLOSE to close the existing position, or None to do nothing."""
        return None


def simulate(
    swaps: pd.DataFrame,
    strategy: Strategy,
    *,
    initial_capital_t1: float,
    dec0: int,
    dec1: int,
    fee_pips: int,
    rebalance_cost_bps: float = 5.0,
    snapshot_every: int = 200,
) -> SimResult:
    """Walk the swap stream and run the strategy.

    Costs:
      - rebalance_cost_bps: bps of the position notional charged on every
        position open (covers gas + slippage on the swap-to-balance step).
    """
    state = StrategyState(capital_t1=initial_capital_t1)
    fees_total_atomic_t1 = 0.0
    n_rebal = 0
    n_in_range = 0
    n_total = 0
    eq: list[tuple[int, float]] = []
    initial_price0 = None  # for hodl benchmark

    for row in swaps.itertuples():
        sqrtP = row.sqrtPriceX96
        tick = row.tick
        ts = row.timestamp
        if initial_price0 is None:
            initial_price0 = (sqrtP / Q96) ** 2 * (10 ** (dec0 - dec1))

        # 1) Maybe rebalance / (re)open / close
        decision = strategy.decide(row, state)
        if decision == CLOSE:
            if state.position is not None:
                a0, a1 = state.position.amounts(sqrtP)
                val = (a0 * (sqrtP / Q96) ** 2 + a1) / (10 ** dec1)
                val += state.position.fees1 / (10 ** dec1)
                state.capital_t1 = val * (1.0 - rebalance_cost_bps / 10_000.0)
                state.position = None
                state.last_rebalance_ts = ts
                n_rebal += 1
        elif decision is not None:
            tl, tu = decision
            if state.position is None or state.position.tick_lower != tl or state.position.tick_upper != tu:
                if state.position is not None:
                    a0, a1 = state.position.amounts(sqrtP)
                    val = (a0 * (sqrtP / Q96) ** 2 + a1) / (10 ** dec1)
                    val += state.position.fees1 / (10 ** dec1)
                    state.capital_t1 = val
                state.capital_t1 *= (1.0 - rebalance_cost_bps / 10_000.0)
                state.position = open_position(sqrtP, tl, tu, state.capital_t1, dec0, dec1, fee_pips)
                state.last_rebalance_ts = ts
                n_rebal += 1

        # 2) Accrue fees if in range
        n_total += 1
        if state.position is not None and state.position.in_range(tick):
            n_in_range += 1
            L_pool = max(int(row.liquidity), 1)
            L_share = state.position.liquidity / L_pool
            if L_share > 0:
                fee_t1 = _swap_fee_in_token1(row, fee_pips)
                fees_total_atomic_t1 += fee_t1 * L_share
                # store as token1 fees for simplicity (close to truth for tight ranges around the price)
                state.position.fees1 += int(fee_t1 * L_share)

        if n_total % snapshot_every == 0 and state.position is not None:
            val = state.position.value_in_token1(sqrtP) + state.position.fees1 / (10 ** dec1)
            eq.append((ts, val))

    # final close
    if swaps.empty:
        return SimResult(0, 0, 0, 0, 0, 0, 0, eq)
    last = swaps.iloc[-1]
    sqrtP_final = int(last.sqrtPriceX96)
    if state.position is not None:
        val = state.position.value_in_token1(sqrtP_final) + state.position.fees1 / (10 ** dec1)
        state.capital_t1 = val
    fees_t1 = fees_total_atomic_t1 / (10 ** dec1)

    # HODL benchmark: 50/50 in token0/token1 at first price, valued at last price
    final_price0 = (sqrtP_final / Q96) ** 2 * (10 ** (dec0 - dec1))
    half = initial_capital_t1 / 2.0
    t0_units = half / initial_price0
    hodl = t0_units * final_price0 + half

    return SimResult(
        final_value_t1=state.capital_t1,
        hodl_value_t1=hodl,
        fees_t1=fees_t1,
        n_rebalances=n_rebal,
        in_range_frac=n_in_range / max(n_total, 1),
        n_swaps_in_range=n_in_range,
        n_swaps_total=n_total,
        equity_curve=eq,
    )


def load_swaps(path: str, dec0: int = 18, dec1: int = 6) -> pd.DataFrame:
    df = pd.read_csv(path)
    needed = ["amount0", "amount1", "sqrtPriceX96", "liquidity", "tick", "timestamp", "block_number"]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"missing column {c}")
    df = df[needed].copy()
    df = df.dropna(subset=needed).reset_index(drop=True)
    df["amount0"] = df["amount0"].astype(object).apply(lambda v: int(float(v)))
    df["amount1"] = df["amount1"].astype(object).apply(lambda v: int(float(v)))
    df["sqrtPriceX96"] = df["sqrtPriceX96"].astype(object).apply(lambda v: int(float(v)))
    df["liquidity"] = df["liquidity"].astype(object).apply(lambda v: int(float(v)))
    df["tick"] = df["tick"].astype(float).astype("int64")
    df["timestamp"] = df["timestamp"].astype(float).astype("int64")
    df = df.sort_values(["block_number", "timestamp"]).reset_index(drop=True)
    df["price"] = (df["sqrtPriceX96"].astype(float) / Q96) ** 2 * (10 ** (dec0 - dec1))
    return df
