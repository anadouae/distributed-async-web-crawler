"""URL deduplication via Redis SET (or in-memory fallback for tests)."""

from __future__ import annotations

import hashlib
from typing import Protocol

from redis.asyncio import Redis


class DedupStore(Protocol):
    async def seen(self, url: str) -> bool: ...
    async def mark(self, url: str) -> bool: ...
    async def count(self) -> int: ...


def _url_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


class RedisDedup:
    """
    Redis SET-based URL deduplication.

    Complexity:
      - mark / seen: O(1) average (SISMEMBER / SADD)
      - count: O(1) SCARD
    Memory: ~32–64 bytes per URL hash; millions of URLs fit in modest Redis RAM.
    """

    def __init__(self, redis: Redis, key: str = "crawler:visited") -> None:
        self._redis = redis
        self._key = key

    async def seen(self, url: str) -> bool:
        return bool(await self._redis.sismember(self._key, _url_id(url)))

    async def mark(self, url: str) -> bool:
        """Return True if the URL was newly marked (not previously seen)."""
        added = await self._redis.sadd(self._key, _url_id(url))
        return bool(added)

    async def count(self) -> int:
        return int(await self._redis.scard(self._key))


class InMemoryDedup:
    """Thread-safe-enough asyncio-local set for unit tests / demo mode."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def seen(self, url: str) -> bool:
        return _url_id(url) in self._seen

    async def mark(self, url: str) -> bool:
        uid = _url_id(url)
        if uid in self._seen:
            return False
        self._seen.add(uid)
        return True

    async def count(self) -> int:
        return len(self._seen)
