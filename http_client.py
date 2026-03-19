"""
http_client.py — Shared HTTP session for the leadgen pipeline.

Provides a single `requests.Session` with:
  - Unified User-Agent and Accept-Language headers
  - Automatic retry with exponential back-off on transient failures
    (connection error, read timeout, 429, 500, 502, 503, 504)
  - Connection pooling (keeps sockets alive across calls in the same session)
  - Configurable per-call timeout default

Usage (in enricher.py, job_checker.py, etc.):

    from http_client import get_session, DEFAULT_TIMEOUT

    session = get_session()          # shared singleton
    r = session.get(url, timeout=DEFAULT_TIMEOUT)

The session is module-level and thread-safe for concurrent to_thread() calls.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Shared headers ─────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Retry policy ───────────────────────────────────────────────────────────────
# Retries ONCE on transient failures only. Enrichment is best-effort — we do not
# want 3 retries × 8s read timeout = 24s per stuck site blocking a thread slot.
_RETRY_POLICY = Retry(
    total=1,
    backoff_factor=0.3,
    status_forcelist={429, 500, 502, 503, 504},
    allowed_methods={"GET", "HEAD"},
    raise_on_status=False,
)

# ── Timeout defaults ───────────────────────────────────────────────────────────
# Always use (connect_timeout, read_timeout) tuples, not a single value.
# A single int only sets the read timeout — connect phase is unbounded.
DEFAULT_TIMEOUT  = (5, 10)   # (connect 5s, read 10s)  — general use
FAST_TIMEOUT     = (3, 6)    # (connect 3s, read 6s)   — ATS slug probes
ENRICH_TIMEOUT   = (5, 8)    # (connect 5s, read 8s)   — homepage enrichment
DOC_TIMEOUT      = (5, 15)   # (connect 5s, read 15s)  — slower document pages

# ── Session singleton ──────────────────────────────────────────────────────────
_session: requests.Session | None = None


def get_session() -> requests.Session:
    """
    Returns the shared pipeline HTTP session.
    Creates it on first call; reuses it on subsequent calls.
    Thread-safe: requests.Session is safe for concurrent reads once configured.
    """
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        adapter = HTTPAdapter(
            max_retries=_RETRY_POLICY,
            pool_connections=20,   # number of connection pools
            pool_maxsize=50,       # max connections per pool
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _session = s
    return _session
