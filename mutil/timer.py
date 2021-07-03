import time
from loguru import logger


class Timer:
    """Helper context manager to automatically print elapsed wall clock time.

    Use as:
        with Timer("Completed!"):
            time.sleep(3)
    """

    def __init__(self, name=""):
        self.name = name

    def __enter__(self):
        # Note: don't use time.clock() or time.process_time()
        #       because those don't record time during sleep calls,
        #       but we need to record sleeps for when we're waiting
        #       for async operations (network replies, etc)
        self.start = time.perf_counter()

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start

        # using "opt(depth=1)" so the CALLER's mod/func/line info is shown
        # instead of all logging show mutil.Timer.__exit__ as the log line.
        if self.name:
            logger.opt(depth=1).info("[{}] Duration: {:,.4f}", self.name, self.interval)
        else:
            logger.opt(depth=1).info("Duration: {:,.4f}", self.interval)
