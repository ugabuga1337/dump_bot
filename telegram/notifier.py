"""Async Telegram notifier.

Pure aiohttp client — no python-telegram-bot dependency. Sends are queued and
flushed by a single background task so the engine never blocks on Telegram
latency. Handles 429 backoff and network errors gracefully.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from config import TelegramConfig

log = logging.getLogger("telegram")


class TelegramNotifier:
    def __init__(self, cfg: TelegramConfig) -> None:
        self._cfg = cfg
        self._session: aiohttp.ClientSession | None = None
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=512)
        self._worker: asyncio.Task | None = None
        self._sent = 0
        self._dropped = 0
        self._failed = 0

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "configured": self._cfg.configured,
            "sent": self._sent,
            "dropped": self._dropped,
            "failed": self._failed,
            "pending": self._queue.qsize(),
        }

    async def start(self) -> None:
        if not self._cfg.configured:
            log.warning("telegram_disabled_or_unconfigured")
            return
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15.0),
                connector=aiohttp.TCPConnector(limit=2),
            )
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run(), name="telegram-worker")

    async def stop(self) -> None:
        if self._worker:
            self._worker.cancel()
            try:
                await self._worker
            except (asyncio.CancelledError, Exception):
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._worker = None

    async def send(self, text: str) -> None:
        if not self._cfg.configured:
            return
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            self._dropped += 1
            log.warning("telegram_queue_full_drop")

    async def _run(self) -> None:
        while True:
            text = await self._queue.get()
            try:
                await self._send_one(text)
                self._sent += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._failed += 1
                log.warning("telegram_send_failed", extra={"err": repr(exc)})
            finally:
                self._queue.task_done()

    async def _send_one(self, text: str) -> None:
        if not self._session:
            return
        url = f"https://api.telegram.org/bot{self._cfg.bot_token}/sendMessage"
        payload = {
            "chat_id": self._cfg.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        # Up to 3 retries with backoff on 5xx/429
        for attempt in range(3):
            async with self._session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return
                if resp.status == 429:
                    body = await resp.json()
                    retry_after = float(body.get("parameters", {}).get("retry_after", 5.0))
                    await asyncio.sleep(min(60.0, retry_after))
                    continue
                if 500 <= resp.status < 600:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue
                text_body = await resp.text()
                raise RuntimeError(f"telegram error {resp.status}: {text_body[:200]}")
        raise RuntimeError("telegram send failed after retries")
