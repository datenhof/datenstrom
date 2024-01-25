import unittest

from datenstrom.common.cache import TTLCache


class Timer:
    def __init__(self, auto=False):
        self.auto = auto
        self.time = 0

    def __call__(self):
        if self.auto:
            self.time += 1
        return self.time

    def tick(self):
        self.time += 1


class TTLCacheTest(unittest.TestCase):
    def test_base_ttl(self):
        cache = TTLCache(maxsize=2, ttl=2, timer=Timer())
        self.assertEqual(0, cache.timer())
        self.assertEqual(2, cache.ttl)

        cache[1] = None
        self.assertIn(1, cache)
        self.assertEqual(None, cache[1])
        self.assertEqual(1, len(cache))
        self.assertEqual({1}, set(cache))

        cache.timer.tick()
        self.assertEqual(None, cache[1])
        self.assertEqual(1, len(cache))
        self.assertEqual({1}, set(cache))

        cache[2] = None
        self.assertEqual(None, cache[1])
        self.assertEqual(None, cache[2])
        self.assertEqual(2, len(cache))
        self.assertEqual({1, 2}, set(cache))

        cache.timer.tick()
        self.assertNotIn(1, cache)
        self.assertEqual(None, cache[2])
        self.assertEqual(1, len(cache))
        self.assertEqual({2}, set(cache))

        cache[3] = 3
        self.assertNotIn(1, cache)
        self.assertEqual(None, cache[2])
        self.assertEqual(3, cache[3])
        self.assertEqual(2, len(cache))
        self.assertEqual({2, 3}, set(cache))

        cache.timer.tick()
        self.assertNotIn(1, cache)
        self.assertNotIn(2, cache)
        self.assertEqual(3, cache[3])
        self.assertEqual(1, len(cache))
        self.assertEqual({3}, set(cache))

        cache.timer.tick()
        self.assertNotIn(1, cache)
        self.assertNotIn(2, cache)
        self.assertNotIn(3, cache)

        with self.assertRaises(KeyError):
            del cache[1]
        with self.assertRaises(KeyError):
            cache.pop(2)
        with self.assertRaises(KeyError):
            del cache[3]

        self.assertEqual(0, len(cache))
        self.assertEqual(set(), set(cache))

    def test_base_none_ttl(self):
        cache = TTLCache(maxsize=2, ttl=2, none_ttl=1, timer=Timer())
        self.assertEqual(0, cache.timer())
        self.assertEqual(2, cache.ttl)

        cache[1] = 1
        cache[2] = None
        self.assertIn(1, cache)
        self.assertIn(2, cache)
        self.assertEqual(1, cache[1])
        self.assertEqual(None, cache[2])
        self.assertEqual(2, len(cache))
        self.assertEqual({1, 2}, set(cache))

        cache.timer.tick()
        print(cache)
        print(cache.__dict__)
        self.assertNotIn(2, cache)
        self.assertEqual(1, cache[1])
        self.assertEqual({1}, set(cache))
        self.assertEqual(1, len(cache))
        

        cache[3] = None
        cache[4] = 4
        self.assertEqual(4, cache[4])
        self.assertEqual(None, cache[3])
        self.assertEqual(2, len(cache))
        self.assertEqual({4, 3}, set(cache))

        cache.timer.tick()
        self.assertEqual(4, cache[4])
        self.assertEqual(1, len(cache))
        self.assertEqual({4}, set(cache))
