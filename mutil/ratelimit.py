import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class RateLimit:
    """Provides a coroutine to `await` which will block if
    called more than 'rate' times in 'bucket_seconds' seconds.

    Accuracy for rate limit intervals is the accuracy of time.monotonic()
    (typically nanosecond accuracy).

    NOTE: We use a queue of each requested timestamp to figure out
          the sliding windows for when requests can resume again.

          So if you set your rate to 20 million per second, we'll store
          20 million timestamps.

          Under normal conditions your rate limits will be like 90 in 15 minutes
          or 1 per second or 300 per 5 minutes, etc, so the memory used by
          the timestamps in the queue won't be overwhelming.


    Example of rate limit 1 print every 5 seconds:
    rl = RateLimit(1, 5)
    while True:
        await rl.guard()
        print("A log line, once every 5 seconds...")
    """

    # default maximum requests in the bucket (rate 120 with bucket 60 is "120 requests per minute")
    rate: int = 120

    # default temporal duration of the bucket in seconds
    bucket_seconds: int = 60

    # last time bucket was filled
    updated: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        # current available capacity
        # We record the time of each request as a queue entry, then
        # remove them as their usage expiration goes away.
        # (no reason this is asyncio.queue, could be regular queue actually)
        self.q = asyncio.Queue(maxsize=self.rate)

    async def guard(self):
        """Return immediately if not all capacity is used.
        If all capacity is used, sleeps the task until available."""

        # If we have ALL our tokens, mark this as the START time, so
        # if we eat all our tokens, we must wait a MINIMUM of
        # self.started + bucket_seconds before continuing.

        if self.q.full():
            # Remove the oldest entry then calculate the next time we
            # can do a single request.
            oldest = self.q.get_nowait()

            now = time.monotonic()
            diff = now - oldest

            if diff < self.bucket_seconds:
                # failure! sleep until the known-good expiry then loop again.
                await asyncio.sleep(self.bucket_seconds - diff)
                assert (
                    time.monotonic() - oldest >= self.bucket_seconds
                ), "Sleep was too fast?"

            # add this new event to the time-of-submission entries
            self.q.put_nowait(time.monotonic())
            return True  # signals we had to wait for time to elapse

        # Not currently full, so add this event to time-of-submission entries
        # and continue immediately
        self.q.put_nowait(time.monotonic())
        return False  # means we didn't have to wait
