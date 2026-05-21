"""Rolling/streaming primitives.

Designed for tight RAM: every per-symbol stat fits in a fixed-size deque,
no historical arrays kept around.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable


class RollingWindow:
    """Fixed-size deque with cached numeric aggregates.

    Optimized for ``append + (mean | max | min | sum)`` workloads.

    Mean is maintained incrementally (O(1)). Min/max are computed lazily and
    cached until invalidated by a structural change — cheap because windows
    are tiny (60-300 items at most per symbol).
    """

    __slots__ = ("_buf", "_sum", "_dirty_extrema", "_min", "_max", "maxlen")

    def __init__(self, maxlen: int) -> None:
        if maxlen <= 0:
            raise ValueError("RollingWindow needs maxlen >= 1")
        self.maxlen = maxlen
        self._buf: deque[float] = deque(maxlen=maxlen)
        self._sum: float = 0.0
        self._dirty_extrema = True
        self._min: float = math.inf
        self._max: float = -math.inf

    def __len__(self) -> int:
        return len(self._buf)

    def __iter__(self):
        return iter(self._buf)

    def push(self, value: float) -> None:
        if len(self._buf) == self.maxlen:
            self._sum -= self._buf[0]
            self._dirty_extrema = True  # may be dropping current extremum
        self._buf.append(value)
        self._sum += value

    def extend(self, values: Iterable[float]) -> None:
        for v in values:
            self.push(v)

    def clear(self) -> None:
        self._buf.clear()
        self._sum = 0.0
        self._min = math.inf
        self._max = -math.inf
        self._dirty_extrema = False

    @property
    def sum(self) -> float:
        return self._sum

    @property
    def mean(self) -> float:
        n = len(self._buf)
        return self._sum / n if n else 0.0

    @property
    def last(self) -> float:
        return self._buf[-1] if self._buf else 0.0

    @property
    def first(self) -> float:
        return self._buf[0] if self._buf else 0.0

    def _refresh_extrema(self) -> None:
        if not self._buf:
            self._min, self._max = math.inf, -math.inf
        else:
            self._min = min(self._buf)
            self._max = max(self._buf)
        self._dirty_extrema = False

    @property
    def min(self) -> float:
        if self._dirty_extrema:
            self._refresh_extrema()
        return self._min

    @property
    def max(self) -> float:
        if self._dirty_extrema:
            self._refresh_extrema()
        return self._max

    def stdev(self) -> float:
        n = len(self._buf)
        if n < 2:
            return 0.0
        m = self._sum / n
        s = 0.0
        for v in self._buf:
            d = v - m
            s += d * d
        return math.sqrt(s / (n - 1))

    def percentile(self, q: float) -> float:
        """Lightweight percentile (O(n log n)) — fine for tiny windows."""
        if not self._buf:
            return 0.0
        s = sorted(self._buf)
        k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
        return s[k]

    def to_list(self) -> list[float]:
        return list(self._buf)


class EMAState:
    """Exponential moving average that survives serialization-free."""

    __slots__ = ("alpha", "_value", "_initialized")

    def __init__(self, period: int) -> None:
        if period < 1:
            raise ValueError("EMA period must be >= 1")
        self.alpha = 2.0 / (period + 1)
        self._value = 0.0
        self._initialized = False

    def update(self, x: float) -> float:
        if not self._initialized:
            self._value = x
            self._initialized = True
        else:
            self._value += self.alpha * (x - self._value)
        return self._value

    @property
    def value(self) -> float:
        return self._value

    @property
    def initialized(self) -> bool:
        return self._initialized
