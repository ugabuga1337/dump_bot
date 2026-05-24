"""Signals package."""

from .anti_spam import AntiSpam
from .outcome_tracker import OutcomeTracker
from .paper_trader import PaperTrader
from .scoring import ConfidenceScorer, ScoredDecision

__all__ = [
    "AntiSpam",
    "ConfidenceScorer",
    "OutcomeTracker",
    "PaperTrader",
    "ScoredDecision",
]
