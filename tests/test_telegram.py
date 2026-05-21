"""Telegram formatter snapshot test."""

from core.models import ConfidenceLabel, MarketRegime, SignalDecision
from telegram import format_signal


def test_format_signal_contains_key_fields():
    d = SignalDecision(
        symbol="DOGEUSDT",
        ts_ms=1700000000000,
        price=0.2145,
        pump_score=60.0,
        exhaustion_score=82.0,
        fake_pump_score=12.0,
        confidence_score=78.4,
        confidence_label=ConfidenceLabel.HIGH,
        market_regime=MarketRegime.BEAR,
        pump_5m_pct=4.2,
        pump_15m_pct=12.4,
        volume_ratio=6.8,
        oi_change_pct=18.0,
        funding_rate=0.042,
        suggested_entry=0.2143,
        suggested_sl=0.2210,
        suggested_tp1=0.2055,
        suggested_tp2=0.1970,
        reasons=["failed breakout", "RSI divergence", "volume climax"],
        setup_tags=["failed_breakout", "rsi_divergence", "climax_volume"],
        metadata={},
    )
    text = format_signal(d)
    assert "SHORT SETUP DETECTED" in text
    assert "DOGEUSDT" in text
    assert "12.40%" in text or "+12.40%" in text
    assert "Entry" in text
    assert "Confidence" in text
    assert "HIGH" in text
