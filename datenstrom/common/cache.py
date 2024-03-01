import time
import collections
import requests
from typing import Optional, Dict

from cachetools import Cache, cached, cachedmethod # noqa
from cachetools import _TimedCache
from cachetools.keys import hashkey


class TTLCache(_TimedCache):
    """LRU Cache implementation with per-item time-to-live (TTL) value."""

    class _Link:

        __slots__ = ("key", "expires", "next", "prev")

        def __init__(self, key=None, expires=None):
            self.key = key
            self.expires = expires

        def __reduce__(self):
            return TTLCache._Link, (self.key, self.expires)

        def unlink(self):
            next = self.next
            prev = self.prev
            prev.next = next
            next.prev = prev

    def __init__(self, maxsize, ttl, none_ttl=None, timer=time.monotonic, getsizeof=None):
        _TimedCache.__init__(self, maxsize, timer, getsizeof)
        self.__root = root = TTLCache._Link()
        root.prev = root.next = root
        self.__links = collections.OrderedDict()
        self.__ttl = ttl
        self.__none_ttl = none_ttl if none_ttl is not None else ttl

    def __contains__(self, key):
        try:
            link = self.__links[key]  # no reordering
        except KeyError:
            return False
        else:
            return self.timer() < link.expires

    def __getitem__(self, key, cache_getitem=Cache.__getitem__):
        try:
            link = self.__getlink(key)
        except KeyError:
            expired = False
        else:
            expired = not (self.timer() < link.expires)
        if expired:
            return self.__missing__(key)
        else:
            return cache_getitem(self, key)

    def __setitem__(self, key, value, cache_setitem=Cache.__setitem__):
        with self.timer as time:
            self.expire(time)
            cache_setitem(self, key, value)
        try:
            link = self.__getlink(key)
        except KeyError:
            self.__links[key] = link = TTLCache._Link(key)
        else:
            link.unlink()
        if value is None:
            link.expires = time + self.__none_ttl
        else:
            link.expires = time + self.__ttl
        link.next = root = self.__root
        link.prev = prev = root.prev
        prev.next = root.prev = link

    def __delitem__(self, key, cache_delitem=Cache.__delitem__):
        cache_delitem(self, key)
        link = self.__links.pop(key)
        link.unlink()
        if not (self.timer() < link.expires):
            raise KeyError(key)

    def __iter__(self):
        root = self.__root
        curr = root.next
        while curr is not root:
            # "freeze" time for iterator access
            with self.timer as time:
                if time < curr.expires:
                    yield curr.key
            curr = curr.next

    def __setstate__(self, state):
        self.__dict__.update(state)
        root = self.__root
        root.prev = root.next = root
        for link in sorted(self.__links.values(), key=lambda obj: obj.expires):
            link.next = root
            link.prev = prev = root.prev
            prev.next = root.prev = link
        self.expire(self.timer())

    @property
    def ttl(self):
        """The time-to-live value of the cache's items."""
        return self.__ttl

    def expire(self, time=None):
        """Remove expired items from the cache."""
        if time is None:
            time = self.timer()
        root = self.__root
        curr = root.next
        links = self.__links
        cache_delitem = Cache.__delitem__
        # while curr is not root and not (time < curr.expires):
        #     cache_delitem(self, curr.key)
        #     del links[curr.key]
        #     next = curr.next
        #     curr.unlink()
        #     curr = next
        while curr is not root:
            if time >= curr.expires:
                # delete items that are not expired
                cache_delitem(self, curr.key)
                del links[curr.key]
                next = curr.next
                curr.unlink()
                curr = next
            else:
                curr = curr.next

    def popitem(self):
        """Remove and return the `(key, value)` pair least recently used that
        has not already expired.

        """
        with self.timer as time:
            self.expire(time)
            try:
                key = next(iter(self.__links))
            except StopIteration:
                raise KeyError("%s is empty" % type(self).__name__) from None
            else:
                return (key, self.pop(key))

    def __getlink(self, key):
        value = self.__links[key]
        self.__links.move_to_end(key)
        return value


class CachedRequestClient:
    def __init__(self, maxsize: int, ttl: int, none_ttl: Optional[int] = None):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl, none_ttl=none_ttl)

    def request(self, url: str, method: str = "GET", result: str = "text",
                params: Optional[Dict[str, str]] = None,
                headers: Optional[Dict[str, str]] = None,
                **kwargs):
        hashable_params = frozenset(params.items()) if params else None
        hashable_headers = frozenset(headers.items()) if headers else None
        key = hashkey(url, method, result, hashable_params, hashable_headers)
        try:
            #cache hit
            return self.cache[key]
        except KeyError:
            # cache miss
            pass

        try:
            response = requests.request(method, url, **kwargs)
        except requests.RequestException:
            print(f"Request to {url} failed", flush=True)
            result = None
        else:
            if response.status_code < 200 or response.status_code >= 300:
                result = None
            else:
                if result == "json":
                    try:
                        result = response.json()
                    except requests.JSONDecodeError:
                        print(f"Failed to decode JSON from {url}", flush=True)
                        result = None
                else:
                    result = response.text

        self.cache[key] = result
        return result

    def get(self, url: str,
            params: Optional[Dict[str, str]] = None,
            headers: Optional[Dict[str, str]] = None,
            **kwargs):
        return self.request(url, method="GET", result="text", params=params, headers=headers, **kwargs)

    def get_json(self, url: str,
                 params: Optional[Dict[str, str]] = None,
                 headers: Optional[Dict[str, str]] = None,
                 **kwargs):
        return self.request(url, method="GET", result="json", params=params, headers=headers, **kwargs)
