"""Tiny math helpers — kept separate so strategy modules read clean."""

from __future__ import annotations

import math
from collections.abc import Iterable


def clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or b is None:
        return default
    return a / b


def pct_change(prev: float, curr: float) -> float:
    if prev == 0:
        return 0.0
    return (curr - prev) / prev * 100.0


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def linear_score(x: float, lo: float, hi: float, *, clamp_output: bool = True) -> float:
    """Map ``x`` linearly from ``[lo, hi]`` -> [0, 1]."""
    if hi == lo:
        return 0.0
    v = (x - lo) / (hi - lo)
    return clamp(v, 0.0, 1.0) if clamp_output else v


def weighted_average(values: Iterable[tuple[float, float]]) -> float:
    """Weighted average from an iterable of (value, weight)."""
    total = 0.0
    wsum = 0.0
    for v, w in values:
        total += v * w
        wsum += w
    if wsum == 0:
        return 0.0
    return total / wsum
