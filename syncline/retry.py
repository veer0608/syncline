"""Retrying one request: backoff for server errors, patience for rate limits.

Three kinds of failure, three answers:
  - network error or 5xx: the server may recover, so back off exponentially with jitter
  - 429, or 403 with no quota left: GitHub says when to come back, so wait exactly that long
  - any other 4xx: retrying cannot help, so fail at once
"""
import http.client
import random
import time
import urllib.error


class GaveUp(Exception):
    """Retries exhausted, or the wait asked for is longer than we are willing to sit through."""


class HTTPFailure(Exception):
    def __init__(self, resp):
        super().__init__(f"HTTP {resp.status}: {resp.body[:200]!r}")
        self.resp = resp


# http.client.HTTPException covers a body cut off mid-read (IncompleteRead) and a
# server that hangs up (RemoteDisconnected). The first live sync died on the former.
RETRYABLE_ERRORS = (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException)


class Clock:
    """Real time. Tests pass a fake with the same two methods."""

    def now(self):
        return time.time()

    def sleep(self, seconds):
        time.sleep(seconds)


def is_rate_limited(resp):
    if resp.status == 429:
        return True
    return resp.status == 403 and (
        resp.headers.get("x-ratelimit-remaining") == "0" or "retry-after" in resp.headers
    )


def rate_limit_wait(resp, now):
    """Seconds GitHub asks us to wait: Retry-After if given, else until the quota resets."""
    if "retry-after" in resp.headers:
        return float(resp.headers["retry-after"])
    if "x-ratelimit-reset" in resp.headers:
        return max(0.0, float(resp.headers["x-ratelimit-reset"]) - now) + 1
    return 60.0  # GitHub's documented default for secondary limits with no header


def backoff(attempt, base=1.0, cap=60.0, rng=random.random):
    """Full jitter: anywhere from 0 to base * 2^attempt, capped."""
    return rng() * min(cap, base * 2 ** attempt)


def call_with_retry(do, *, clock, max_tries=6, max_wait=3600.0, log=print):
    """Run `do()` until it returns a 2xx Response, following the rules above."""
    for attempt in range(1, max_tries + 1):
        try:
            resp = do()
        except RETRYABLE_ERRORS as e:
            reason, wait = f"network error {e}", backoff(attempt)
        else:
            if resp.status < 400:
                return resp
            if is_rate_limited(resp):
                reason, wait = f"rate limited ({resp.status})", rate_limit_wait(resp, clock.now())
                if wait > max_wait:
                    raise GaveUp(f"rate limit wait of {wait:.0f}s exceeds max_wait {max_wait:.0f}s")
            elif resp.status >= 500:
                reason, wait = f"server error {resp.status}", backoff(attempt)
            else:
                raise HTTPFailure(resp)
        if attempt == max_tries:
            raise GaveUp(f"gave up after {max_tries} tries, last: {reason}")
        log(f"  {reason}, waiting {wait:.1f}s (try {attempt} of {max_tries})")
        clock.sleep(wait)
