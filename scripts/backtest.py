#!/usr/bin/env python3
"""Convenience entrypoint for ``python scripts/backtest.py DOGEUSDT --days 14``."""

from __future__ import annotations

import sys

if __name__ == "__main__":
    from backtesting.replay import main

    sys.exit(main())
