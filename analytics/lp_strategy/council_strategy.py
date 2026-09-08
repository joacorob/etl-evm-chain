"""Strategy adapter that turns the council vote into a tick range and feeds
the LP simulator."""
from __future__ import annotations
from collections import deque
from dataclasses import dataclass
import math
import numpy as np

from lp_simulator import Strategy, StrategyState, CLOSE
from agents import (
    AgentCtx,
    CouncilMember,
    council_vote,
    make_council,
)
from v3_math import Q96, price_to_tick


@dataclass
class CouncilStrategy(Strategy):
    dec0: int
    dec1: int
    fee_pips: int
    tick_spacing: int
    decision_every_n_swaps: int = 600
    rebalance_min_secs: int = 6 * 3600
    width_floor_bps: float = 40.0
    width_cap_bps: float = 500.0
    members: list[CouncilMember] | None = None
    history_size: int = 4096
    train_skill: bool = True

    def __post_init__(self):
        self.name = "council_v1"
        if self.members is None:
            self.members = make_council()
        self.prices: deque[float] = deque(maxlen=self.history_size)
        self.lrets: deque[float] = deque(maxlen=self.history_size)
        self.vols: deque[float] = deque(maxlen=self.history_size)
        self.fees: deque[float] = deque(maxlen=self.history_size)
        self.tss: deque[int] = deque(maxlen=self.history_size)
        self.liqs: deque[float] = deque(maxlen=self.history_size)
        self.swap_count = 0
        self.last_decision: tuple[int, int] | None = None
        # for online skill update
        self._last_votes: list = []
        self._last_eval_price: float | None = None
        self._last_eval_time: int = 0

    def reset_history_keep_skill(self):
        """Clear rolling context but preserve learned skill weights — used by
        the rolling backtest to isolate each window while keeping the council's
        accumulated knowledge."""
        self.prices.clear()
        self.lrets.clear()
        self.vols.clear()
        self.fees.clear()
        self.tss.clear()
        self.liqs.clear()
        self.swap_count = 0
        self.last_decision = None
        self._last_votes = []
        self._last_eval_price = None
        self._last_eval_time = 0

    def _ctx(self, current_price: float, current_tick: int) -> AgentCtx:
        return AgentCtx(
            prices=np.fromiter(self.prices, dtype=float),
            log_returns=np.fromiter(self.lrets, dtype=float),
            volumes_t1=np.fromiter(self.vols, dtype=float),
            fees_t1=np.fromiter(self.fees, dtype=float),
            timestamps=np.fromiter(self.tss, dtype=np.int64),
            pool_liquidity=np.fromiter(self.liqs, dtype=float),
            current_price=current_price,
            current_tick=current_tick,
        )

    def decide(self, row, state: StrategyState):  # type: ignore[override]
        # update history
        sqrtP = int(row.sqrtPriceX96)
        price = (sqrtP / Q96) ** 2 * (10 ** (self.dec0 - self.dec1))
        self.prices.append(price)
        if len(self.prices) >= 2:
            self.lrets.append(math.log(self.prices[-1] / self.prices[-2] + 1e-30))
        else:
            self.lrets.append(0.0)
        # rough fee accrued by pool from this swap (token1 atomic, in token1 units)
        a0 = float(row.amount0)
        a1 = float(row.amount1)
        in_t1 = abs(a0) * (sqrtP / Q96) ** 2 if a0 > 0 else abs(a1)
        self.vols.append(in_t1 / (10 ** self.dec1))
        self.fees.append(in_t1 * self.fee_pips / 1_000_000.0 / (10 ** self.dec1))
        self.tss.append(int(row.timestamp))
        self.liqs.append(float(row.liquidity))
        self.swap_count += 1

        # Decide on a cadence to keep the cost down
        if self.swap_count % self.decision_every_n_swaps != 0:
            return None
        if state.last_rebalance_ts and (int(row.timestamp) - state.last_rebalance_ts) < self.rebalance_min_secs:
            return None
        if len(self.prices) < 200:
            return None

        ctx = self._ctx(price, int(row.tick))
        if self.train_skill and self._last_eval_price is not None and self._last_votes:
            from agents import update_skill
            move_pct = (price - self._last_eval_price) / self._last_eval_price
            outcome_pos = abs(move_pct) < 0.005
            update_skill(self.members, self._last_votes, outcome_pos, alpha=0.04)

        decision, votes = council_vote(self.members, ctx)
        self._last_votes = votes
        self._last_eval_price = price
        self._last_eval_time = int(row.timestamp)

        if not decision.deploy:
            # If we currently hold a position and confidence in NO is strong, close.
            if state.position is not None and decision.no_weight > 1.5 * decision.yes_weight:
                return CLOSE
            return None

        width = max(self.width_floor_bps, min(self.width_cap_bps, decision.width_bps))
        # asymmetric range via skew (-1..1). When skew>0 the range biases above price.
        upper_share = 0.5 + 0.4 * decision.skew
        lower_share = 1.0 - upper_share
        upper_price = price * (1 + 2 * width / 1e4 * upper_share)
        lower_price = price * (1 - 2 * width / 1e4 * lower_share)
        tl = price_to_tick(lower_price, self.dec0, self.dec1)
        tu = price_to_tick(upper_price, self.dec0, self.dec1)
        # snap to tick spacing
        spacing = self.tick_spacing
        tl = (tl // spacing) * spacing
        tu = ((tu + spacing - 1) // spacing) * spacing
        if tu - tl < spacing * 2:
            tu = tl + spacing * 2
        # avoid micro-rebalances
        if self.last_decision is not None:
            old_tl, old_tu = self.last_decision
            if abs(tl - old_tl) < spacing and abs(tu - old_tu) < spacing:
                return None
        self.last_decision = (tl, tu)
        return tl, tu


@dataclass
class StaticRangeStrategy(Strategy):
    """Baseline: always hold a fixed-width range, recentered when out of range."""
    dec0: int
    dec1: int
    fee_pips: int
    tick_spacing: int
    width_bps: float = 100.0

    def __post_init__(self):
        self.name = f"static_{int(self.width_bps)}bps"

    def decide(self, row, state):  # type: ignore[override]
        sqrtP = int(row.sqrtPriceX96)
        price = (sqrtP / Q96) ** 2 * (10 ** (self.dec0 - self.dec1))
        if state.position is not None and state.position.in_range(int(row.tick)):
            return None
        upper = price * (1 + self.width_bps / 1e4)
        lower = price * (1 - self.width_bps / 1e4)
        tl = price_to_tick(lower, self.dec0, self.dec1)
        tu = price_to_tick(upper, self.dec0, self.dec1)
        spacing = self.tick_spacing
        tl = (tl // spacing) * spacing
        tu = ((tu + spacing - 1) // spacing) * spacing
        if tu - tl < spacing * 2:
            tu = tl + spacing * 2
        return tl, tu


@dataclass
class FullRangeStrategy(Strategy):
    """Passive baseline: full range, opened once."""
    dec0: int
    dec1: int
    fee_pips: int
    tick_spacing: int

    def __post_init__(self):
        self.name = "full_range"
        self._opened = False

    def decide(self, row, state):  # type: ignore[override]
        if self._opened:
            return None
        self._opened = True
        # max ticks for the pool's spacing
        MAX_TICK = 887272
        spacing = self.tick_spacing
        tl = -(MAX_TICK // spacing) * spacing
        tu = (MAX_TICK // spacing) * spacing
        return tl, tu
