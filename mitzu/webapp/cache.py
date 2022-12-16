import mitzu.webapp.configs as configs
import redis
import diskcache
from typing import Any, Optional, List
from abc import ABC
from dataclasses import dataclass, field


class MitzuCache(ABC):
    def put(self, key: str, val: Any, expire: Optional[float] = None) -> None:
        raise NotImplementedError()

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        raise NotImplementedError()

    def clear(self, key: str) -> None:
        raise NotImplementedError()

    def clear_all(self, prefix: Optional[str]) -> None:
        for key in self.list_keys(prefix):
            self.clear(key)

    def list_keys(self, prefix: Optional[str]) -> List[str]:
        raise NotImplementedError()


@dataclass(frozen=True)
class DiskMitzuCache(MitzuCache):

    _disk_cache: diskcache.Cache = field(
        default_factory=lambda: diskcache.Cache(configs.DISK_CACHE_PATH)
    )

    def put(self, key: str, val: Any, expire: Optional[float] = None):
        self._disk_cache.add(key, value=val, expire=expire)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._disk_cache.get(key, default=default)

    def clear(self, key: str) -> None:
        self._disk_cache.pop(key)

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        keys = self._disk_cache.iterkeys()
        return [k for k in keys if k.startswith(prefix) or prefix is None]

    def get_disk_cache(self) -> diskcache.Cache:
        return self._disk_cache


@dataclass(init=False, frozen=True)
class RedisMitzuCache(MitzuCache):

    _redis: redis.Redis

    def __init__(self, redis_cache: Optional[redis.Redis] = None) -> None:
        super().__init__()

        if redis_cache is not None:
            object.__setattr__(self, "_redis", redis_cache)
        else:
            if configs.REDIS_URL is None:
                raise ValueError(
                    "REDIS_URL env variable is not set, can't create redis cache."
                )
            object.__setattr__(
                self,
                "_redis",
                redis.Redis(host=configs.REDIS_URL, port=configs.REDIS_PORT),
            )

    def put(self, key: str, val: Any, expire: Optional[float] = None):
        self._redis.set(name=key, value=val, ex=expire)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        res = self._redis.get(name=key)
        if res is None and default is not None:
            return default
        return res

    def clear(self, key: str) -> None:
        self._redis.delete(key)

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        if not prefix:
            prefix = "*"
        return [k for k in self._redis.scan_iter(f"prefix:{prefix}")]
