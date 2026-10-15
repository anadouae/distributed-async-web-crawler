"""URL frontier — Redis list queue with depth/domain guards."""

from __future__ import annotations

from collections import defaultdict
from typing import Protocol
from urllib.parse import urldefrag, urlparse

from redis.asyncio import Redis

from crawler.dedup import DedupStore
from crawler.metrics import Metrics
from crawler.models import CrawlJob
from crawler.security import UnsafeURLError, validate_crawl_url


def normalize_url(url: str) -> str:
    """Strip fragment, lowercase scheme/host, drop trailing slash on path-only roots."""
    url, _frag = urldefrag(url.strip())
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{scheme}://{netloc}{path}{query}"


class Frontier(Protocol):
    async def enqueue(self, job: CrawlJob) -> bool: ...
    async def dequeue(self, timeout: float = 1.0) -> CrawlJob | None: ...
    async def size(self) -> int: ...


class RedisFrontier:
    """
    Redis LIST-backed URL frontier (LPUSH / BRPOP).

    Complexity:
      - enqueue: O(1) LPUSH + O(1) dedup SISMEMBER/SADD + optional domain counter
      - dequeue: O(1) BRPOP (blocks up to timeout)
      - size: O(1) LLEN

    Scaling: shard by domain hash across N Redis queues, or use Redis Streams
    with consumer groups for at-least-once delivery across worker fleets.
    Target >10k in-flight: pair with a process-local asyncio.Semaphore and
    horizontally scale worker replicas; Redis LIST throughput is typically
    100k+ ops/sec on a single instance.
    """

    def __init__(
        self,
        redis: Redis,
        dedup: DedupStore,
        metrics: Metrics,
        *,
        queue_key: str = "crawler:queue",
        max_depth: int = 3,
        max_pages_per_domain: int = 100,
        allowed_domains: list[str] | None = None,
        allow_private_networks: bool = False,
    ) -> None:
        self._redis = redis
        self._dedup = dedup
        self._metrics = metrics
        self._queue_key = queue_key
        self._max_depth = max_depth
        self._max_pages_per_domain = max_pages_per_domain
        self._allowed = {d.lower() for d in (allowed_domains or [])}
        self._allow_private = allow_private_networks
        self._domain_counts: dict[str, int] = defaultdict(int)
        self._domain_key_prefix = f"{queue_key}:domain:"

    def _domain_allowed(self, domain: str) -> bool:
        if not self._allowed:
            return True
        return domain in self._allowed or any(domain.endswith(f".{d}") for d in self._allowed)

    async def _domain_count(self, domain: str) -> int:
        key = f"{self._domain_key_prefix}{domain}"
        val = await self._redis.get(key)
        if val is None:
            return 0
        return int(val)

    async def _incr_domain(self, domain: str) -> int:
        key = f"{self._domain_key_prefix}{domain}"
        return int(await self._redis.incr(key))

    async def enqueue(self, job: CrawlJob) -> bool:
        """
        Enqueue if URL is new, within depth, and under domain quota.

        Returns True when the job was accepted onto the queue.
        """
        job.url = normalize_url(job.url)
        try:
            validate_crawl_url(job.url, allow_private=self._allow_private, resolve=False)
        except UnsafeURLError:
            return False
        if job.depth > self._max_depth:
            return False
        if not self._domain_allowed(job.domain):
            return False

        # Dedup before domain quota so retries of seen URLs don't burn budget
        if not await self._dedup.mark(job.url):
            await self._metrics.incr("dedup_hits")
            return False

        count = await self._domain_count(job.domain)
        if count >= self._max_pages_per_domain:
            return False

        await self._incr_domain(job.domain)
        await self._redis.lpush(self._queue_key, job.to_json())
        await self._metrics.incr("enqueued")
        return True

    async def dequeue(self, timeout: float = 1.0) -> CrawlJob | None:
        """
        Blocking pop from the right of the list.

        BRPOP returns (key, value) or None on timeout.
        """
        result = await self._redis.brpop(self._queue_key, timeout=timeout)
        if result is None:
            return None
        _key, raw = result
        await self._metrics.incr("dequeued")
        return CrawlJob.from_json(raw)

    async def size(self) -> int:
        return int(await self._redis.llen(self._queue_key))


class InMemoryFrontier:
    """Deque-based frontier for unit tests without Redis."""

    def __init__(
        self,
        dedup: DedupStore,
        metrics: Metrics | None = None,
        *,
        max_depth: int = 3,
        max_pages_per_domain: int = 100,
        allowed_domains: list[str] | None = None,
        allow_private_networks: bool = False,
    ) -> None:
        from collections import deque

        self._q: deque[CrawlJob] = deque()
        self._dedup = dedup
        self._metrics = metrics
        self._max_depth = max_depth
        self._max_pages_per_domain = max_pages_per_domain
        self._allowed = {d.lower() for d in (allowed_domains or [])}
        self._allow_private = allow_private_networks
        self._domain_counts: dict[str, int] = defaultdict(int)

    def _domain_allowed(self, domain: str) -> bool:
        if not self._allowed:
            return True
        return domain in self._allowed or any(domain.endswith(f".{d}") for d in self._allowed)

    async def enqueue(self, job: CrawlJob) -> bool:
        job.url = normalize_url(job.url)
        try:
            validate_crawl_url(job.url, allow_private=self._allow_private, resolve=False)
        except UnsafeURLError:
            return False
        if job.depth > self._max_depth:
            return False
        if not self._domain_allowed(job.domain):
            return False
        if not await self._dedup.mark(job.url):
            if self._metrics:
                await self._metrics.incr("dedup_hits")
            return False
        if self._domain_counts[job.domain] >= self._max_pages_per_domain:
            return False
        self._domain_counts[job.domain] += 1
        self._q.appendleft(job)
        if self._metrics:
            await self._metrics.incr("enqueued")
        return True

    async def dequeue(self, timeout: float = 1.0) -> CrawlJob | None:
        if not self._q:
            return None
        job = self._q.pop()
        if self._metrics:
            await self._metrics.incr("dequeued")
        return job

    async def size(self) -> int:
        return len(self._q)
