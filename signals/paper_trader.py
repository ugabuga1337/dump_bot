"""Paper trader.

Listens to new signals emitted by the engine and opens simulated short
positions for every active :class:`paper_strategy`.  Positions are tracked
in lock-step with :class:`OutcomeTracker` — when the tracker reports a
TP/SL hit or that the tracking window has expired, the matching paper
trade is closed and P&L recorded.

Designed so that ``PaperTrader`` owns no live websocket subscriptions: it
piggybacks on the price updates that flow through the OutcomeTracker so
the work is essentially free (one extra dict lookup per tick).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from storage import Repository
from utils import now_ms

log = logging.getLogger("paper_trader")


@dataclass(slots=True)
class _Position:
    trade_id: int
    strategy_id: int
    signal_id: int
    symbol: str
    entry_price: float
    sl_price: float
    tp1_price: float
    tp2_price: float
    size_usd: float
    opened_ms: int
    tp1_hit: bool = False


@dataclass(slots=True)
class _StrategySnapshot:
    """A tiny cached read of strategy parameters."""

    id: int
    name: str
    deposit: float
    size_pct: float
    sl_mult: float
    tp1_mult: float
    tp2_mult: float
    min_confidence: float
    active: bool
    realized_pnl: float = 0.0
    extras: dict[str, Any] = field(default_factory=dict)

    @property
    def balance(self) -> float:
        return self.deposit + self.realized_pnl


class PaperTrader:
    """Manage simulated trades for every configured paper strategy."""

    def __init__(self, repo: Repository) -> None:
        self._repo = repo
        # signal_id -> list[_Position]
        self._open_by_signal: dict[int, list[_Position]] = {}
        # symbol -> list[_Position]
        self._open_by_symbol: dict[str, list[_Position]] = {}

    async def restore_open(self) -> None:
        """Re-load open paper trades from DB at startup."""
        rows = await self._repo.list_paper_trades(status="open", limit=10_000)
        for r in rows:
            pos = _Position(
                trade_id=int(r["id"]),
                strategy_id=int(r["strategy_id"]),
                signal_id=int(r["signal_id"]),
                symbol=r["symbol"],
                entry_price=float(r["entry_price"]),
                sl_price=float(r["sl_price"]),
                tp1_price=float(r["tp1_price"]),
                tp2_price=float(r["tp2_price"]),
                size_usd=float(r["size_usd"]),
                opened_ms=int(r["opened_ms"]),
            )
            self._open_by_signal.setdefault(pos.signal_id, []).append(pos)
            self._open_by_symbol.setdefault(pos.symbol, []).append(pos)
        log.info("paper_trader_restored", extra={"open_positions": len(rows)})

    # -------- signal lifecycle --------

    async def on_signal(self, signal: dict[str, Any]) -> list[int]:
        """Open paper trades for every active strategy that accepts ``signal``.

        Returns the list of created paper_trade IDs (mostly useful for tests).
        """
        strategies = await self._load_active_strategies()
        if not strategies:
            return []

        symbol = signal.get("symbol") or ""
        signal_id = int(signal.get("id") or signal.get("signal_id") or 0)
        if not signal_id:
            return []
        confidence = float(signal.get("confidence_score") or 0.0)
        entry = float(signal.get("suggested_entry") or signal.get("price") or 0.0)
        sl = float(signal.get("suggested_sl") or 0.0)
        tp1 = float(signal.get("suggested_tp1") or 0.0)
        tp2 = float(signal.get("suggested_tp2") or 0.0)
        if entry <= 0 or sl <= 0 or tp1 <= 0 or tp2 <= 0:
            log.debug("paper_skip_invalid_prices",
                      extra={"signal_id": signal_id, "symbol": symbol})
            return []

        created: list[int] = []
        for strat in strategies:
            if confidence < strat.min_confidence:
                continue
            # SL/TP are price levels. For a short setup the multipliers stretch
            # the distance from entry.  sl is *above* entry, tp's are *below*.
            sl_price = entry + (sl - entry) * strat.sl_mult
            tp1_price = entry - (entry - tp1) * strat.tp1_mult
            tp2_price = entry - (entry - tp2) * strat.tp2_mult
            size_usd = max(0.0, strat.balance * (strat.size_pct / 100.0))
            if size_usd <= 0:
                log.info("paper_skip_no_balance",
                         extra={"strategy_id": strat.id, "balance": strat.balance})
                continue
            trade_id = await self._repo.insert_paper_trade({
                "strategy_id": strat.id,
                "signal_id": signal_id,
                "symbol": symbol,
                "opened_ms": now_ms(),
                "entry_price": entry,
                "sl_price": sl_price,
                "tp1_price": tp1_price,
                "tp2_price": tp2_price,
                "size_usd": size_usd,
            })
            pos = _Position(
                trade_id=trade_id,
                strategy_id=strat.id,
                signal_id=signal_id,
                symbol=symbol,
                entry_price=entry,
                sl_price=sl_price,
                tp1_price=tp1_price,
                tp2_price=tp2_price,
                size_usd=size_usd,
                opened_ms=now_ms(),
            )
            self._open_by_signal.setdefault(signal_id, []).append(pos)
            self._open_by_symbol.setdefault(symbol, []).append(pos)
            created.append(trade_id)
            log.info("paper_trade_opened",
                     extra={"trade_id": trade_id, "strategy": strat.name,
                            "symbol": symbol, "size_usd": round(size_usd, 2)})
        return created

    async def on_price(self, symbol: str, price: float, ts_ms: int) -> None:
        """Step every open position on this symbol; close any that hit SL/TP."""
        positions = self._open_by_symbol.get(symbol)
        if not positions:
            return
        closed: list[tuple[_Position, str, float]] = []
        for pos in positions:
            if price >= pos.sl_price:
                closed.append((pos, "sl", price))
            elif price <= pos.tp2_price:
                closed.append((pos, "tp2", price))
            elif price <= pos.tp1_price and not pos.tp1_hit:
                # We don't close on TP1 — it's a partial milestone — but we
                # remember it for the close_reason if the window expires.
                pos.tp1_hit = True
        for pos, reason, close_price in closed:
            await self._close(pos, reason=reason, close_price=close_price, ts_ms=ts_ms)

    async def on_outcome_finalized(
        self,
        signal_id: int,
        *,
        close_reason: str,
        last_price: float,
        closed_ms: int | None = None,
    ) -> None:
        """Called by the OutcomeTracker when a signal's tracking window closes."""
        positions = self._open_by_signal.get(signal_id, [])
        for pos in list(positions):
            reason = close_reason
            if reason == "expired" and pos.tp1_hit:
                reason = "tp1"
            await self._close(
                pos, reason=reason, close_price=last_price, ts_ms=closed_ms
            )

    # -------- state introspection --------

    def open_symbols(self) -> set[str]:
        return {sym for sym, lst in self._open_by_symbol.items() if lst}

    def has_open(self) -> bool:
        return any(self._open_by_symbol.values())

    def snapshot(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for lst in self._open_by_symbol.values():
            for pos in lst:
                out.append({
                    "trade_id": pos.trade_id,
                    "strategy_id": pos.strategy_id,
                    "signal_id": pos.signal_id,
                    "symbol": pos.symbol,
                    "entry_price": pos.entry_price,
                    "sl_price": pos.sl_price,
                    "tp1_price": pos.tp1_price,
                    "tp2_price": pos.tp2_price,
                    "size_usd": pos.size_usd,
                    "opened_ms": pos.opened_ms,
                    "tp1_hit": pos.tp1_hit,
                })
        return out

    # -------- internals --------

    async def _load_active_strategies(self) -> list[_StrategySnapshot]:
        rows = await self._repo.paper_strategy_summary()
        out: list[_StrategySnapshot] = []
        for r in rows:
            if not int(r.get("active") or 0):
                continue
            strat = await self._repo.get_paper_strategy(int(r["id"]))
            if not strat:
                continue
            out.append(_StrategySnapshot(
                id=int(strat["id"]),
                name=strat["name"],
                deposit=float(strat["deposit"]),
                size_pct=float(strat["size_pct"]),
                sl_mult=float(strat["sl_mult"]),
                tp1_mult=float(strat["tp1_mult"]),
                tp2_mult=float(strat["tp2_mult"]),
                min_confidence=float(strat["min_confidence"]),
                active=bool(int(strat["active"])),
                realized_pnl=float(r.get("realized_pnl") or 0.0),
            ))
        return out

    async def _close(
        self,
        pos: _Position,
        *,
        reason: str,
        close_price: float,
        ts_ms: int | None = None,
    ) -> None:
        # Remove from in-memory indexes first so re-entrant callers don't
        # double-close.
        sig_list = self._open_by_signal.get(pos.signal_id)
        if sig_list and pos in sig_list:
            sig_list.remove(pos)
            if not sig_list:
                self._open_by_signal.pop(pos.signal_id, None)
        sym_list = self._open_by_symbol.get(pos.symbol)
        if sym_list and pos in sym_list:
            sym_list.remove(pos)
            if not sym_list:
                self._open_by_symbol.pop(pos.symbol, None)

        pnl_pct = 0.0
        if pos.entry_price > 0:
            # Short P&L %: (entry - close) / entry
            pnl_pct = (pos.entry_price - close_price) / pos.entry_price * 100.0
        pnl_usd = round(pos.size_usd * pnl_pct / 100.0, 4)
        try:
            await self._repo.close_paper_trade(
                pos.trade_id,
                close_price=close_price,
                close_reason=reason,
                pnl_usd=pnl_usd,
                pnl_pct=round(pnl_pct, 4),
                closed_ms=ts_ms or now_ms(),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("paper_close_failed",
                        extra={"trade_id": pos.trade_id, "err": repr(exc)})
            return
        log.info("paper_trade_closed",
                 extra={"trade_id": pos.trade_id, "reason": reason,
                        "pnl_usd": pnl_usd, "pnl_pct": round(pnl_pct, 3)})
