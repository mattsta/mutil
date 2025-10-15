import unittest

from mutil.dualcache import DualCache


class TestDualCache(unittest.TestCase):
    def test_cache_consistency(self):
        cache = DualCache("test_cache")

        # Test adding an item
        cache["key1"] = "value1"
        self.assertEqual(cache["key1"], "value1")
        self.assertEqual(cache.checkDisk("key1"), "value1")

        # Test updating an item
        cache["key1"] = "new_value"
        self.assertEqual(cache["key1"], "new_value")
        self.assertEqual(cache.checkDisk("key1"), "new_value")

        # Test deleting an item
        del cache["key1"]
        self.assertIsNone(cache.get("key1"))
        self.assertIsNone(cache.checkDisk("key1"))

        cache.destroy()


if __name__ == "__main__":
    unittest.main()
