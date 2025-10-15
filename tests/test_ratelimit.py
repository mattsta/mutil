import asyncio
import time
import unittest

from mutil.ratelimit import RateLimit


class TestRateLimit(unittest.TestCase):
    def test_rate_limit(self):
        async def test_logic():
            # Test that the rate limit is not exceeded
            rl = RateLimit(10, 1)
            for _ in range(10):
                self.assertFalse(await rl.guard())

            # Test that the rate limit is exceeded
            start_time = time.monotonic()
            self.assertTrue(await rl.guard())
            end_time = time.monotonic()
            self.assertGreaterEqual(end_time - start_time, 1)

            # Test with multiple concurrent tasks
            rl = RateLimit(10, 1)
            tasks = [rl.guard() for _ in range(20)]
            results = await asyncio.gather(*tasks)
            self.assertEqual(results.count(True), 10)

        asyncio.run(test_logic())

    def test_expiry_and_burst(self):
        async def test_logic():
            # Allow 3 per 0.5s; verify bursts and expiry timing
            rl = RateLimit(3, 0.5)
            # 3 immediate allowed
            for _ in range(3):
                self.assertFalse(await rl.guard())
            # 4th should wait ~0.5s
            t0 = time.monotonic()
            self.assertTrue(await rl.guard())
            t1 = time.monotonic()
            self.assertGreaterEqual(t1 - t0, 0.49)

            # After ~0.5s from first event, another should be allowed immediately
            await asyncio.sleep(0.51)
            self.assertFalse(await rl.guard())

        asyncio.run(test_logic())

    def test_concurrency_multi_batch(self):
        async def test_logic():
            rl = RateLimit(5, 0.5)
            # First batch of 10 concurrent calls
            results1 = await asyncio.gather(*[rl.guard() for _ in range(10)])
            # 5 should proceed immediately (False), 5 should wait (True)
            self.assertEqual(results1.count(False), 5)
            self.assertEqual(results1.count(True), 5)

            # Immediately issue another batch; half should wait again
            results2 = await asyncio.gather(*[rl.guard() for _ in range(10)])
            # Because timestamps include waited calls, capacity is still bounded
            self.assertEqual(results2.count(False), 0)  # window still full
            self.assertEqual(results2.count(True), 10)

            # After window elapses, capacity replenishes
            await asyncio.sleep(0.51)
            results3 = await asyncio.gather(*[rl.guard() for _ in range(5)])
            self.assertEqual(results3.count(False), 5)

        asyncio.run(test_logic())


if __name__ == "__main__":
    unittest.main()
