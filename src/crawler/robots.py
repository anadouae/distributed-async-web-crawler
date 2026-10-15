"""Basic robots.txt respect with Redis/TTL caching."""

from __future__ import annotations

import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RobotsCache:
    """
    Fetch and cache robots.txt per host.

    Complexity: first check per host is one HTTP GET + parse O(R) rules;
    subsequent checks are O(1) cache hit + O(R) can_fetch. Redis cache key
    TTL avoids stampedes across workers.
    """

    def __init__(
        self,
        redis: Redis | None = None,
        *,
        cache_key: str = "crawler:robots",
        user_agent: str = "*",
        ttl_seconds: int = 3600,
        enabled: bool = True,
        fetcher=None,
    ) -> None:
        self._redis = redis
        self._cache_key = cache_key
        self._ua = user_agent
        self._ttl = ttl_seconds
        self._enabled = enabled
        self._fetcher = fetcher  # async callable(url) -> str | None
        self._local: dict[str, RobotFileParser] = {}

    async def allowed(self, url: str) -> bool:
        if not self._enabled:
            return True
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if not host:
            return False

        rp = self._local.get(host)
        if rp is None:
            rp = await self._load(host, parsed.scheme or "https")
            self._local[host] = rp

        try:
            return bool(rp.can_fetch(self._ua, url))
        except Exception:
            logger.debug("robots can_fetch failed for %s — allowing", url, exc_info=True)
            return True

    async def _load(self, host: str, scheme: str) -> RobotFileParser:
        robots_url = f"{scheme}://{host}/robots.txt"
        body: str | None = None

        if self._redis is not None:
            cached = await self._redis.hget(self._cache_key, host)
            if cached is not None:
                body = cached.decode() if isinstance(cached, bytes) else str(cached)

        if body is None and self._fetcher is not None:
            try:
                body = await self._fetcher(robots_url)
            except Exception:
                logger.debug("robots fetch failed for %s", robots_url, exc_info=True)
                body = ""

        if body is None:
            body = ""

        if self._redis is not None and body is not None:
            await self._redis.hset(self._cache_key, host, body)
            await self._redis.expire(self._cache_key, self._ttl)

        rp = RobotFileParser()
        rp.parse(body.splitlines() if body else [])
        return rp
