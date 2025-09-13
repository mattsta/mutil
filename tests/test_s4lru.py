from loguru import logger

from mutil.s4lru import S4LRU
from mutil.timer import Timer

# Note: to view output, run as: pytest -s

# These tests are for manually checking the levels are
# incrementing and evicting properly and there isn't much (any?)
# automated assetation of valid states currently.


def test_lru_basic():
    try:
        s = S4LRU("pickle", 300)
        with Timer("Complete run"):
            for i in range(100):
                with Timer(f"Batch {i}..."):
                    for j in range(700):
                        if True:
                            s.increment(f"a{j}", 1)
                            s.increment(f"b{j}", 2)
                            s.increment(f"c{j}", 3)
                        else:
                            s.increment("a", 1)
                            s.increment("b", 2)
                            s.increment("c", 3)
                    s.flushCounters()
    except:
        logger.exception("whaaaa")

    s.repr(logger=logger.info)

    while s.evictCheck():
        logger.info("Eviction Cycle!")
        removed = s.evict()
        logger.info(f"Removed {len(removed)}")
        s.repr(logger=logger.info)


def test_lru_random():
    try:
        import random

        s2 = S4LRU("pickle-boosted", 322619594 // 2, 16)
        with Timer("Complete run"):
            for i in range(100):
                with Timer(f"Batch {i}..."):
                    for j in range(7):
                        run = random.sample(range(3), k=2)
                        if 0 in run:
                            s2.incrementBoost(f"a{j}", random.randint(1 << 14, 1 << 25))
                        if 1 in run:
                            s2.incrementBoost(f"b{j}", random.randint(1 << 14, 1 << 25))
                        if 2 in run:
                            s2.incrementBoost(f"c{j}", random.randint(1 << 14, 1 << 25))
                        s2.flushCounters()
    except:
        logger.exception("whaaaa")

    s2.repr(logger=logger.info)

    while s2.evictCheck():
        logger.info("Eviction Cycle!")
        removed = s2.evict()
        logger.info(f"Removed {len(removed)}")
        s2.repr(logger=logger.info)
