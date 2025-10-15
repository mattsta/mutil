import asyncio
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class RateLimit:
    """Async sliding-window rate limiter.

    Purpose: Allow at most `rate` events in any sliding window of
    `bucket_seconds` seconds. If the limit would be exceeded, callers of
    `await guard()` will wait until at least one prior event expires from the
    window, then proceed.

    Guarantees:
    - No more than `rate` events are allowed in any continuous
      `bucket_seconds` interval (sliding window semantics).
    - Callers that would exceed the limit will sleep up to the exact time
      needed for the oldest in-window event to expire, then continue.
    - Behavior is deterministic relative to `time.monotonic()`.

    Implementation notes:
    - A deque stores timestamps of admitted events. It is purged of timestamps
      older than `bucket_seconds` before admitting a new event.
    - An asyncio.Lock protects concurrent access so multiple tasks can call
      `guard()` safely without overshooting the limit.

    Example: rate limit 1 print every 5 seconds
        rl = RateLimit(1, 5)
        while True:
            await rl.guard()
            print("A log line, once every 5 seconds...")
    """

    # default maximum requests in the bucket (rate 120 with bucket 60 is "120 requests per minute")
    rate: int = 120

    # default temporal duration of the bucket in seconds
    bucket_seconds: int = 60

    # last time bucket was filled (unused, kept for compatibility)
    updated: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        # Sliding window of request timestamps
        self._timestamps: deque[float] = deque(maxlen=self.rate)
        # Protects concurrent access to timestamps
        self._lock = asyncio.Lock()

    async def guard(self):
        """Return immediately if not all capacity is used.
        If all capacity is used, sleeps the task until available."""

        # If we have ALL our tokens, mark this as the START time, so
        # if we eat all our tokens, we must wait a MINIMUM of
        # self.started + bucket_seconds before continuing.

        waited = False
        while True:
            async with self._lock:
                now = time.monotonic()
                # purge expired timestamps
                while (
                    self._timestamps
                    and (now - self._timestamps[0]) >= self.bucket_seconds
                ):
                    self._timestamps.popleft()

                if len(self._timestamps) < self.rate:
                    self._timestamps.append(now)
                    return waited

                # need to wait until the oldest expires
                oldest = self._timestamps[0]
                sleep_for = max(0.0, self.bucket_seconds - (now - oldest))

            # sleep outside the lock
            if sleep_for > 0:
                waited = True
                await asyncio.sleep(sleep_for)
            else:
                # just loop to re-check
                await asyncio.sleep(0)
