"""
Central logging for LeadGen API (stdout → Render logs).

Env:
  LOG_LEVEL — DEBUG | INFO | WARNING | ERROR (default INFO)
"""

from __future__ import annotations

import logging
import os
import sys
import time


class _UtcFormatter(logging.Formatter):
    """Timestamps in UTC (suffix Z) for Render / log aggregators."""

    converter = time.gmtime


class _FlushingStreamHandler(logging.StreamHandler):
    """Flush after each record so Render / Docker show lines immediately."""

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


def configure_app_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        level = logging.INFO

    fmt = "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%SZ"
    handler = _FlushingStreamHandler(sys.stdout)
    handler.setFormatter(_UtcFormatter(fmt, datefmt))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Reduce noise from HTTP clients used by Supabase / OpenAI
    for noisy in ("httpx", "httpcore", "urllib3", "hpack", "http11"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
