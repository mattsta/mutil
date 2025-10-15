import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock

from mutil.dcache import FetchCache


class TestFetchCache(unittest.TestCase):
    def test_cache_hit_and_miss(self):
        async def test_logic():
            session = MagicMock()
            session.get = AsyncMock()
            session.get.return_value.text = AsyncMock(return_value="test_content")

            # Test cache miss
            fc = FetchCache(session, "http://test.com", "test_file")
            content = await fc.get()
            self.assertEqual(content, "test_content")
            self.assertTrue(os.path.exists(fc.filepath))

            # Test cache hit
            content = await fc.get()
            self.assertEqual(content, "test_content")
            session.get.assert_called_once()

            fc.filepath.unlink()

        asyncio.run(test_logic())

    def test_cache_expiration(self):
        async def test_logic():
            session = MagicMock()
            session.get = AsyncMock()
            session.get.return_value.text = AsyncMock(return_value="test_content")

            # Test cache expiration
            fc = FetchCache(
                session, "http://test.com", "test_file", refreshMinutes=1 / 60
            )
            await fc.get()
            self.assertTrue(os.path.exists(fc.filepath))

            await asyncio.sleep(2)

            await fc.get()
            self.assertEqual(session.get.call_count, 2)

            fc.filepath.unlink()

        asyncio.run(test_logic())


if __name__ == "__main__":
    unittest.main()
