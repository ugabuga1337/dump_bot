"""Anti-spam cooldown + duplicate suppression tests."""


from signals.anti_spam import AntiSpam


def test_cooldown_blocks_duplicate_fire(monkeypatch):
    spam = AntiSpam(cooldown_sec=10, watch_ttl_sec=60, duplicate_window_sec=10,
                    global_per_minute_cap=99)
    can, _ = spam.can_fire("DOGEUSDT", 60.0, ["failed_breakout"])
    assert can
    spam.record_fire("DOGEUSDT", 60.0, ["failed_breakout"])
    can, reason = spam.can_fire("DOGEUSDT", 60.0, ["failed_breakout"])
    assert not can
    assert reason in ("cooldown", "duplicate")


def test_global_rate_limit():
    spam = AntiSpam(cooldown_sec=0, watch_ttl_sec=60, global_per_minute_cap=2)
    for sym in ("AAA", "BBB"):
        can, _ = spam.can_fire(sym, 60.0, ["x"])
        assert can
        spam.record_fire(sym, 60.0, ["x"])
    can, reason = spam.can_fire("CCC", 60.0, ["x"])
    assert not can
    assert reason == "global_rate_limit"


def test_duplicate_state_suppression():
    spam = AntiSpam(cooldown_sec=0, watch_ttl_sec=60, duplicate_window_sec=60,
                    global_per_minute_cap=99, score_eps=2.0)
    spam.record_fire("ETHUSDT", 70.0, ["a", "b"])
    can, reason = spam.can_fire("ETHUSDT", 71.0, ["a", "b"])
    assert not can
    assert reason == "duplicate"
    # Different tag signature should be allowed.
    can, _ = spam.can_fire("ETHUSDT", 71.0, ["a", "b", "c"])
    assert can
