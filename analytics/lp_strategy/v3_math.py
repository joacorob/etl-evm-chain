"""Uniswap v3 concentrated-liquidity math, isolated and pure-Python.

All amounts are in token *atomic* units (wei-style integers) unless otherwise
noted, mirroring the on-chain semantics. Helpers convert to/from human units.
"""
from __future__ import annotations
import math
from dataclasses import dataclass

Q96 = 1 << 96
Q192 = 1 << 192
TICK_BASE = 1.0001


def tick_to_sqrt_price_x96(tick: int) -> int:
    return int(math.floor((TICK_BASE ** (tick / 2)) * Q96))


def sqrt_price_x96_to_tick(sqrt_price_x96: int) -> int:
    sp = sqrt_price_x96 / Q96
    return int(math.floor(math.log(sp * sp) / math.log(TICK_BASE)))


def tick_to_price(tick: int, dec0: int, dec1: int) -> float:
    """Price of token0 in units of token1 (human-decimal adjusted)."""
    return (TICK_BASE ** tick) * (10 ** (dec0 - dec1))


def price_to_tick(price: float, dec0: int, dec1: int) -> int:
    raw = price / (10 ** (dec0 - dec1))
    return int(round(math.log(raw) / math.log(TICK_BASE)))


def liquidity_for_amount0(sqrtA: int, sqrtB: int, amount0: int) -> int:
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA
    intermediate = (sqrtA * sqrtB) // Q96
    return (amount0 * intermediate) // (sqrtB - sqrtA)


def liquidity_for_amount1(sqrtA: int, sqrtB: int, amount1: int) -> int:
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA
    return (amount1 * Q96) // (sqrtB - sqrtA)


def liquidity_for_amounts(sqrtP: int, sqrtA: int, sqrtB: int, a0: int, a1: int) -> int:
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA
    if sqrtP <= sqrtA:
        return liquidity_for_amount0(sqrtA, sqrtB, a0)
    if sqrtP < sqrtB:
        l0 = liquidity_for_amount0(sqrtP, sqrtB, a0)
        l1 = liquidity_for_amount1(sqrtA, sqrtP, a1)
        return min(l0, l1)
    return liquidity_for_amount1(sqrtA, sqrtB, a1)


def amount0_for_liquidity(sqrtA: int, sqrtB: int, L: int) -> int:
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA
    return (L * Q96 * (sqrtB - sqrtA)) // (sqrtB * sqrtA)


def amount1_for_liquidity(sqrtA: int, sqrtB: int, L: int) -> int:
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA
    return (L * (sqrtB - sqrtA)) // Q96


def amounts_for_liquidity(sqrtP: int, sqrtA: int, sqrtB: int, L: int) -> tuple[int, int]:
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA
    if sqrtP <= sqrtA:
        return amount0_for_liquidity(sqrtA, sqrtB, L), 0
    if sqrtP < sqrtB:
        return amount0_for_liquidity(sqrtP, sqrtB, L), amount1_for_liquidity(sqrtA, sqrtP, L)
    return 0, amount1_for_liquidity(sqrtA, sqrtB, L)


@dataclass
class Position:
    tick_lower: int
    tick_upper: int
    liquidity: int
    dec0: int
    dec1: int
    fee_pips: int  # e.g. 500 for 0.05 %
    fees0: int = 0
    fees1: int = 0

    @property
    def sqrt_lower(self) -> int:
        return tick_to_sqrt_price_x96(self.tick_lower)

    @property
    def sqrt_upper(self) -> int:
        return tick_to_sqrt_price_x96(self.tick_upper)

    def in_range(self, tick: int) -> bool:
        return self.tick_lower <= tick < self.tick_upper

    def amounts(self, sqrtP: int) -> tuple[int, int]:
        return amounts_for_liquidity(sqrtP, self.sqrt_lower, self.sqrt_upper, self.liquidity)

    def value_in_token1(self, sqrtP: int) -> float:
        a0, a1 = self.amounts(sqrtP)
        price = (sqrtP / Q96) ** 2  # token1 atomic per token0 atomic
        v_atomic = a0 * price + a1
        return v_atomic / (10 ** self.dec1)


def open_position(
    sqrtP: int,
    tick_lower: int,
    tick_upper: int,
    capital_token1: float,
    dec0: int,
    dec1: int,
    fee_pips: int,
) -> Position:
    """Open a position centered around sqrtP using `capital_token1` of token1.

    Capital is split so that we deposit roughly equal-value amounts of t0/t1
    given the current price (matching what Uniswap actually accepts).
    """
    sqrtA = tick_to_sqrt_price_x96(tick_lower)
    sqrtB = tick_to_sqrt_price_x96(tick_upper)
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA
    capital_atomic1 = int(capital_token1 * (10 ** dec1))
    if sqrtP <= sqrtA:
        # entirely token0
        # value of all-token0 position == capital_token1, convert via price
        price = (sqrtP / Q96) ** 2
        a0 = int(capital_atomic1 / price)
        a1 = 0
    elif sqrtP >= sqrtB:
        a0 = 0
        a1 = capital_atomic1
    else:
        # split: solve for L such that value(L) == capital_atomic1
        # value(L) = L * ((sqrtP - sqrtA)/Q96 + (1/sqrtA_norm - 1/sqrtP_norm)*price)
        # easier numeric: try L=1e18, scale.
        L_probe = 10 ** 18
        a0p, a1p = amounts_for_liquidity(sqrtP, sqrtA, sqrtB, L_probe)
        price = (sqrtP / Q96) ** 2
        v_probe = a0p * price + a1p
        if v_probe == 0:
            raise ValueError("zero probe value")
        L = int(L_probe * (capital_atomic1 / v_probe))
        a0, a1 = amounts_for_liquidity(sqrtP, sqrtA, sqrtB, L)
        return Position(tick_lower, tick_upper, L, dec0, dec1, fee_pips)
    L = liquidity_for_amounts(sqrtP, sqrtA, sqrtB, a0, a1)
    return Position(tick_lower, tick_upper, L, dec0, dec1, fee_pips)
