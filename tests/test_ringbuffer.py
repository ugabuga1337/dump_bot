"""Tests for utils.ringbuffer primitives."""

from utils import EMAState, RollingWindow


def test_rolling_window_mean_and_extrema():
    w = RollingWindow(5)
    for v in [10, 20, 30, 40, 50]:
        w.push(v)
    assert len(w) == 5
    assert w.sum == 150
    assert w.mean == 30
    assert w.min == 10
    assert w.max == 50

    w.push(60)  # drops 10
    assert len(w) == 5
    assert w.mean == 40
    assert w.min == 20
    assert w.max == 60


def test_rolling_window_stdev_and_percentile():
    w = RollingWindow(20)
    for v in range(1, 21):  # 1..20
        w.push(v)
    assert abs(w.mean - 10.5) < 1e-6
    # Population vs sample stdev — we use sample (n-1).
    assert abs(w.stdev() - 5.9160797831) < 1e-6
    assert w.percentile(0.0) == 1
    assert w.percentile(1.0) == 20
    assert 9 <= w.percentile(0.5) <= 11


def test_ema_state_initialization():
    e = EMAState(period=3)
    assert not e.initialized
    e.update(10)
    assert e.initialized
    assert e.value == 10
    e.update(20)
    # alpha = 2/(3+1) = 0.5
    assert e.value == 15
    e.update(40)
    assert e.value == 27.5
