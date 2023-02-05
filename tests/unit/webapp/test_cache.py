from mitzu.webapp.cache import LocalCache
from tests.unit.webapp.fixtures import InMemoryCache
from time import sleep


def test_local_cache_expiration():
    in_memory_cache = InMemoryCache()
    local_cache = LocalCache(in_memory_cache)

    local_cache.put("a", "b", expire=0.5)

    assert in_memory_cache.get("a") == "b"

    # Cache expires
    sleep(1)
    in_memory_cache.put("a", "c")

    assert local_cache.get("a") == "c"


def test_local_cache_list_keys():
    in_memory_cache = InMemoryCache()
    local_cache = LocalCache(in_memory_cache)

    local_cache.put("a", "b", expire=0.5)

    assert in_memory_cache.get("a") == "b"

    # Cache expires
    sleep(1)
    in_memory_cache.put("a", "c")
    keys = local_cache.list_keys()

    assert len(keys) == 1
    assert local_cache.get("a") == "c"
