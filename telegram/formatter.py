"""Telegram message formatting (HTML mode for clean rendering on mobile)."""

from __future__ import annotations

from core.models import ConfidenceLabel, MarketRegime, SignalDecision

_LABEL_TO_EMOJI = {
    ConfidenceLabel.LOW: "⚪️",
    ConfidenceLabel.MEDIUM: "🟡",
    ConfidenceLabel.HIGH: "🔴",
}


def _fmt_price(price: float) -> str:
    if price >= 100:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def _fmt_pct(value: float, *, signed: bool = True) -> str:
    if value is None:
        return "n/a"
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.2f}%"


def format_signal(d: SignalDecision) -> str:
    label_emoji = _LABEL_TO_EMOJI.get(d.confidence_label, "⚪️")
    regime_text = "neutral" if d.market_regime == MarketRegime.NEUTRAL else d.market_regime.value.lower()
    reasons_block = "\n".join(f"• {r}" for r in d.reasons[:8]) if d.reasons else "• (no specific tags)"
    return (
        "🚨 <b>SHORT SETUP DETECTED</b>\n"
        f"\n"
        f"<b>Symbol:</b> <code>{d.symbol}</code>\n"
        f"<b>Price:</b> <code>{_fmt_price(d.price)}</code>\n"
        f"<b>Pump:</b> {_fmt_pct(d.pump_5m_pct)} / 5m, "
        f"{_fmt_pct(d.pump_15m_pct)} / 15m\n"
        f"<b>Volume spike:</b> {d.volume_ratio:.1f}x\n"
        f"<b>OI change:</b> {_fmt_pct(d.oi_change_pct)}\n"
        f"<b>Funding:</b> {_fmt_pct(d.funding_rate, signed=False)}\n"
        f"<b>Exhaustion:</b> {d.exhaustion_score:.0f}/100\n"
        f"<b>Confidence:</b> {label_emoji} {d.confidence_label.value} "
        f"({d.confidence_score:.0f}/100)\n"
        f"\n"
        f"<b>Reasons:</b>\n"
        f"{reasons_block}\n"
        f"\n"
        f"<b>Suggested zones</b>\n"
        f"<b>Entry:</b>   <code>{_fmt_price(d.suggested_entry)}</code>\n"
        f"<b>Stop:</b>    <code>{_fmt_price(d.suggested_sl)}</code>\n"
        f"<b>TP1:</b>     <code>{_fmt_price(d.suggested_tp1)}</code>\n"
        f"<b>TP2:</b>     <code>{_fmt_price(d.suggested_tp2)}</code>\n"
        f"\n"
        f"<b>Market regime:</b> BTC {regime_text}\n"
        f"\n"
        f"<i>Paper-only signal. Not financial advice.</i>"
    )
