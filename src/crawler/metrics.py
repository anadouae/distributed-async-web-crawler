"""Redis-backed atomic metrics counters for crawl observability."""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis


class Metrics:
    """
    Atomic counters stored in a Redis HASH.

    Complexity: each increment is O(1) HINCRBY; snapshot is O(F) where F is
    the fixed field count (~10). Safe across many workers without locks.
    """

    FIELDS = (
        "enqueued",
        "dequeued",
        "fetched_ok",
        "fetched_error",
        "dedup_hits",
        "robots_blocked",
        "retries",
        "pages_stored",
        "links_extracted",
    )

    def __init__(self, redis: Redis, key: str = "crawler:metrics") -> None:
        self._redis = redis
        self._key = key

    async def incr(self, field: str, amount: int = 1) -> int:
        if field not in self.FIELDS:
            raise ValueError(f"Unknown metric field: {field}")
        return int(await self._redis.hincrby(self._key, field, amount))

    async def snapshot(self) -> dict[str, int]:
        raw: dict[Any, Any] = await self._redis.hgetall(self._key)
        # redis-py may return bytes or str depending on decode_responses
        out: dict[str, int] = {f: 0 for f in self.FIELDS}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else str(k)
            out[key] = int(v)
        return out

    async def reset(self) -> None:
        await self._redis.delete(self._key)
