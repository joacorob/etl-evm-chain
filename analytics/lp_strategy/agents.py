"""Council of agents that vote on whether to deploy LP, and at what range.

Each agent receives a context window of recent prices/swaps and emits a Vote:

    Vote(deploy: bool, width_bps: int, skew: float, confidence: float)

* `deploy`   – should we have an active position right now?
* `width_bps` – preferred half-width of the range, in price bps from current
                price (e.g. 50 bps → ±0.5 %).
* `skew`     – -1.0..+1.0, asymmetry. Negative = bias range below price,
                positive = bias above. Useful when an agent expects drift.
* `confidence` – 0..1, how strongly the agent stands behind its vote.

The Council weights each agent by `skill * confidence` and aggregates:

    deploy   = weighted majority
    width    = weighted mean of widths from "deploy" voters
    skew     = weighted mean of skews from "deploy" voters

Skill is updated online based on realised PnL of past decisions (Bayesian-ish:
exponentially-weighted moving average of "was the vote right?"). The Plato/
epistocracy idea: agents that have been right historically get more vote.

100 agents are produced by parameterising ~12 archetypes across windows,
thresholds and biases.
"""
from __future__ import annotations
import math
from dataclasses import dataclass, field
from collections import deque
from typing import Callable

import numpy as np


@dataclass
class Vote:
    deploy: bool
    width_bps: float
    skew: float
    confidence: float
    rationale: str = ""


@dataclass
class AgentCtx:
    """Rolling context handed to every agent on every tick."""
    prices: np.ndarray            # last N prices
    log_returns: np.ndarray       # last N-1 log returns
    volumes_t1: np.ndarray        # last N abs volumes (token1)
    fees_t1: np.ndarray           # last N fee$ collected by pool
    timestamps: np.ndarray        # last N unix ts
    pool_liquidity: np.ndarray    # last N L_active
    current_price: float
    current_tick: int


# ───────────────────────────── archetypes ──────────────────────────────


def vol_agent(window: int, target_in_range_p: float = 0.7, horizon_swaps: int = 1500) -> Callable[[AgentCtx], Vote]:
    """Sets width so the price stays in range with target probability under
    a Gaussian assumption on per-swap log returns over the window, scaled to
    the expected hold horizon (in swaps)."""
    def _f(ctx: AgentCtx) -> Vote:
        r = ctx.log_returns[-window:]
        if len(r) < max(20, window // 4):
            return Vote(False, 0, 0, 0.0, "vol: warmup")
        sigma = float(np.std(r))
        if sigma == 0 or math.isnan(sigma):
            return Vote(False, 0, 0, 0.0, "vol: zero")
        z = {0.5: 0.674, 0.7: 1.036, 0.8: 1.282, 0.9: 1.645, 0.95: 1.96}.get(target_in_range_p, 1.0)
        # scale single-swap stdev to the holding horizon
        w_log = z * sigma * math.sqrt(horizon_swaps)
        width_bps = max(20.0, min(2000.0, 1e4 * w_log))
        return Vote(True, width_bps, 0.0, 0.6, f"vol w={window} σ={sigma:.5f}")
    return _f


def trend_agent(short: int, long: int) -> Callable[[AgentCtx], Vote]:
    """Skew the range in the direction of the prevailing trend."""
    def _f(ctx: AgentCtx) -> Vote:
        if len(ctx.prices) < long:
            return Vote(False, 0, 0, 0.0, "trend: warmup")
        ms = float(np.mean(ctx.prices[-short:]))
        ml = float(np.mean(ctx.prices[-long:]))
        rel = (ms - ml) / ml
        if abs(rel) < 1e-4:
            return Vote(True, 50.0, 0.0, 0.3, "trend: flat")
        skew = max(-1.0, min(1.0, rel * 200.0))
        return Vote(True, 80.0, skew, 0.5, f"trend Δ={rel:+.4%}")
    return _f


def mean_revert_agent(window: int, z_threshold: float = 2.0) -> Callable[[AgentCtx], Vote]:
    """If price is far from mean, deploy *narrow* expecting reversion. If close,
    abstain to avoid wasting fees on noise."""
    def _f(ctx: AgentCtx) -> Vote:
        p = ctx.prices[-window:]
        if len(p) < window:
            return Vote(False, 0, 0, 0.0, "mr: warmup")
        mu = float(np.mean(p))
        sd = float(np.std(p))
        if sd == 0:
            return Vote(False, 0, 0, 0.0, "mr: flat")
        z = (ctx.current_price - mu) / sd
        if abs(z) >= z_threshold:
            skew = -math.copysign(0.6, z)  # bias range toward the mean
            return Vote(True, 30.0, skew, min(1.0, abs(z) / 4.0), f"mr z={z:+.2f}")
        return Vote(False, 0, 0, 0.4, f"mr quiet z={z:+.2f}")
    return _f


def fee_momentum_agent(window: int) -> Callable[[AgentCtx], Vote]:
    """If pool fees have been rising, widen and deploy more aggressively."""
    def _f(ctx: AgentCtx) -> Vote:
        f = ctx.fees_t1[-window:]
        if len(f) < window:
            return Vote(False, 0, 0, 0.0, "fees: warmup")
        first = float(np.mean(f[: window // 2])) + 1e-9
        last = float(np.mean(f[window // 2:])) + 1e-9
        ratio = last / first
        if ratio > 1.2:
            return Vote(True, 60.0, 0.0, min(1.0, math.log(ratio)), f"fee↑ {ratio:.2f}")
        if ratio < 0.7:
            return Vote(False, 0, 0, 0.4, f"fee↓ {ratio:.2f}")
        return Vote(True, 90.0, 0.0, 0.3, f"fee~ {ratio:.2f}")
    return _f


def microstructure_agent(window: int) -> Callable[[AgentCtx], Vote]:
    """Looks at swap-rate (frequency) — high churn → fees, deploy wider."""
    def _f(ctx: AgentCtx) -> Vote:
        ts = ctx.timestamps[-window:]
        if len(ts) < window:
            return Vote(False, 0, 0, 0.0, "micro: warmup")
        span = (ts[-1] - ts[0]) or 1
        rate = window / span  # swaps/sec
        if rate > 0.05:
            return Vote(True, 70.0, 0.0, 0.6, f"churn {rate:.3f}/s")
        return Vote(True, 200.0, 0.0, 0.2, f"slow {rate:.3f}/s")
    return _f


def regime_agent(window: int, vol_low: float, vol_high: float) -> Callable[[AgentCtx], Vote]:
    """Three regimes by realized vol: deploy tight in low, wide in mid, abstain
    in high (range will get blown out)."""
    def _f(ctx: AgentCtx) -> Vote:
        r = ctx.log_returns[-window:]
        if len(r) < window:
            return Vote(False, 0, 0, 0.0, "regime: warmup")
        rv = float(np.std(r) * math.sqrt(60 * 60 * 24 / max((ctx.timestamps[-1] - ctx.timestamps[-window]) / window, 1)))
        if rv < vol_low:
            return Vote(True, 25.0, 0.0, 0.7, f"calm rv={rv:.3f}")
        if rv < vol_high:
            return Vote(True, 80.0, 0.0, 0.5, f"normal rv={rv:.3f}")
        return Vote(False, 0, 0, 0.6, f"storm rv={rv:.3f}")
    return _f


def quantile_range_agent(window: int, q: float) -> Callable[[AgentCtx], Vote]:
    """Width = price range covered by central `q` quantile of recent prices."""
    def _f(ctx: AgentCtx) -> Vote:
        p = ctx.prices[-window:]
        if len(p) < window:
            return Vote(False, 0, 0, 0.0, "q: warmup")
        lo, hi = np.quantile(p, (1 - q) / 2), np.quantile(p, 1 - (1 - q) / 2)
        rng = (hi - lo) / ctx.current_price
        width_bps = max(20.0, min(1500.0, 1e4 * rng))
        return Vote(True, width_bps, 0.0, 0.5, f"q{q} {rng:.4%}")
    return _f


def imbalance_agent(window: int) -> Callable[[AgentCtx], Vote]:
    """Looks at sign of net order flow → expects continuation, skews range."""
    def _f(ctx: AgentCtx) -> Vote:
        if len(ctx.log_returns) < window:
            return Vote(False, 0, 0, 0.0, "imb: warmup")
        r = ctx.log_returns[-window:]
        net = float(np.sum(np.sign(r)))
        skew = max(-1.0, min(1.0, net / window * 2))
        return Vote(True, 100.0, skew, 0.4, f"imb {net:+.0f}/{window}")
    return _f


# ─────────────────────────── council factory ───────────────────────────


@dataclass
class CouncilMember:
    name: str
    fn: Callable[[AgentCtx], Vote]
    skill: float = 1.0  # multiplicative weight; updated online
    correct: int = 0
    seen: int = 0


def make_council() -> list[CouncilMember]:
    members: list[CouncilMember] = []
    add = lambda n, f: members.append(CouncilMember(n, f))

    # vol agents over multiple windows / probabilities → 25
    for w in (60, 120, 240, 480, 960):
        for p in (0.5, 0.7, 0.8, 0.9, 0.95):
            add(f"vol_w{w}_p{int(p*100)}", vol_agent(w, p))
    # trend → 12
    for s, l in [(20, 100), (20, 200), (50, 200), (50, 400), (100, 400), (100, 800),
                 (200, 800), (10, 60), (30, 150), (40, 200), (60, 300), (80, 400)]:
        add(f"trend_{s}_{l}", trend_agent(s, l))
    # mean revert → 12
    for w in (60, 120, 240, 480):
        for z in (1.5, 2.0, 2.5):
            add(f"mr_w{w}_z{z}", mean_revert_agent(w, z))
    # fee momentum → 6
    for w in (60, 120, 240, 480, 960, 1920):
        add(f"fee_w{w}", fee_momentum_agent(w))
    # microstructure → 6
    for w in (30, 60, 120, 240, 480, 960):
        add(f"micro_w{w}", microstructure_agent(w))
    # regime → 9
    for w in (120, 240, 480):
        for lo, hi in [(0.3, 1.2), (0.5, 2.0), (0.8, 3.0)]:
            add(f"regime_w{w}_{lo}_{hi}", regime_agent(w, lo, hi))
    # quantile range → 15
    for w in (120, 240, 480, 960, 1920):
        for q in (0.6, 0.8, 0.9):
            add(f"qrange_w{w}_q{int(q*100)}", quantile_range_agent(w, q))
    # imbalance → 6
    for w in (30, 60, 120, 240, 480, 960):
        add(f"imb_w{w}", imbalance_agent(w))
    # contrarian / "fool" agents that always deploy with fixed widths → 9
    for bps in (25, 40, 60, 80, 100, 150, 200, 300, 500):
        def _const(b=bps):
            return lambda ctx: Vote(True, float(b), 0.0, 0.2, f"const {b}")
        add(f"const_{bps}", _const())
    return members


@dataclass
class CouncilDecision:
    deploy: bool
    width_bps: float
    skew: float
    yes_weight: float
    no_weight: float
    n_yes: int
    n_no: int


def council_vote(members: list[CouncilMember], ctx: AgentCtx) -> tuple[CouncilDecision, list[Vote]]:
    yes_w = no_w = 0.0
    yes_n = no_n = 0
    width_acc = 0.0
    skew_acc = 0.0
    width_w = 0.0
    votes: list[Vote] = []
    for m in members:
        v = m.fn(ctx)
        votes.append(v)
        w = max(0.0, m.skill) * max(0.0, v.confidence)
        if v.deploy:
            yes_w += w
            yes_n += 1
            width_acc += v.width_bps * w
            skew_acc += v.skew * w
            width_w += w
        else:
            no_w += w
            no_n += 1
    deploy = yes_w >= no_w and width_w > 0
    width = width_acc / width_w if width_w > 0 else 0.0
    skew = skew_acc / width_w if width_w > 0 else 0.0
    return CouncilDecision(deploy, width, skew, yes_w, no_w, yes_n, no_n), votes


def update_skill(members: list[CouncilMember], votes: list[Vote], outcome_pos: bool, alpha: float = 0.05):
    """Online skill update. `outcome_pos` is True if deploying *was* a good
    call (we made positive PnL net of fees). Each agent's skill EMAs toward 1
    when right and toward 0 when wrong; correct/seen are pure stats."""
    for m, v in zip(members, votes):
        m.seen += 1
        right = (v.deploy == outcome_pos)
        if right:
            m.correct += 1
        target = 1.0 if right else 0.2
        m.skill = (1 - alpha) * m.skill + alpha * target
