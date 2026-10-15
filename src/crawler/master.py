"""Master process — seeds frontier, spawns worker pool, reports metrics."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from redis.asyncio import Redis

from crawler.config import Settings, get_settings
from crawler.dedup import InMemoryDedup, RedisDedup
from crawler.fetcher import AiohttpFetcher, MockFetcher
from crawler.frontier import InMemoryFrontier, RedisFrontier
from crawler.metrics import Metrics
from crawler.models import CrawlJob
from crawler.robots import RobotsCache
from crawler.storage import InMemoryStore, PostgresStore
from crawler.worker import Worker

logger = logging.getLogger(__name__)


class Master:
    """
    Orchestrates the crawl: seed → worker pool → graceful shutdown.

    Horizontal scale recipe:
      1. Increase CRAWLER_MAX_CONCURRENCY (semaphore) toward fd/memory limits
      2. Increase CRAWLER_WORKER_COUNT (coroutine tasks per process)
      3. Run multiple Master/worker containers against shared Redis + Postgres
      4. Shard Redis queues by domain if a single LIST becomes hot
    Combined, steps 1–3 readily exceed 10k concurrent in-flight fetches.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._redis: Redis | None = None
        self._store: Any = None
        self._fetcher: Any = None
        self._workers: list[Worker] = []
        self._tasks: list[asyncio.Task] = []

    async def setup(self) -> tuple[Any, Any, Any, Metrics, RobotsCache]:
        s = self.settings
        if s.demo_mode:
            # Fully offline: no Redis/Postgres/network required.
            logger.info("demo mode — in-memory frontier, MockFetcher, InMemoryStore")
            metrics: Any = _LocalMetrics()
            frontier: Any = InMemoryFrontier(
                InMemoryDedup(),
                metrics,
                max_depth=s.max_depth,
                max_pages_per_domain=s.max_pages_per_domain,
                allowed_domains=s.allowed_domains,
                allow_private_networks=True,  # demo fixtures are public; allow local debug URLs
            )
            self._store = InMemoryStore()
            self._fetcher = MockFetcher()
        else:
            self._redis = Redis.from_url(
                s.redis_url,
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            await self._redis.ping()
            metrics = Metrics(self._redis, s.metrics_key)
            frontier = RedisFrontier(
                self._redis,
                RedisDedup(self._redis, s.visited_key),
                metrics,
                queue_key=s.queue_key,
                max_depth=s.max_depth,
                max_pages_per_domain=s.max_pages_per_domain,
                allowed_domains=s.allowed_domains,
                allow_private_networks=s.allow_private_networks,
            )
            self._store = PostgresStore(s.postgres_dsn)
            self._fetcher = AiohttpFetcher(
                user_agent=s.user_agent,
                timeout=s.http_timeout,
                max_retries=s.max_retries,
                base_backoff=s.base_backoff_seconds,
                allow_private_networks=s.allow_private_networks,
            )

        await self._store.setup()

        async def robots_fetch(url: str) -> str:
            resp = await self._fetcher.fetch(url)
            if resp.error or resp.status_code >= 400:
                return ""
            return resp.body.decode("utf-8", errors="replace")

        robots = RobotsCache(
            self._redis,
            cache_key=s.robots_cache_key,
            user_agent=s.user_agent,
            enabled=s.respect_robots and not s.demo_mode,
            fetcher=robots_fetch,
        )
        return frontier, self._fetcher, self._store, metrics, robots

    async def seed(self, frontier: Any, urls: list[str] | None = None) -> int:
        from crawler.security import UnsafeURLError, validate_crawl_url

        seeds = urls or self.settings.seed_urls
        accepted = 0
        allow_private = self.settings.allow_private_networks or self.settings.demo_mode
        for url in seeds:
            try:
                validate_crawl_url(url, allow_private=allow_private, resolve=True)
            except UnsafeURLError as exc:
                logger.warning("rejecting seed URL %s: %s", url, exc)
                continue
            ok = await frontier.enqueue(CrawlJob(url=url, depth=0))
            if ok:
                accepted += 1
                logger.info("seeded %s", url)
        return accepted

    async def run(self, urls: list[str] | None = None) -> dict[str, int]:
        s = self.settings
        frontier, fetcher, store, metrics, robots = await self.setup()
        await self.seed(frontier, urls)

        sem = asyncio.Semaphore(s.max_concurrency)
        self._workers = [
            Worker(
                frontier=frontier,
                fetcher=fetcher,
                store=store,
                robots=robots,
                metrics=metrics,
                semaphore=sem,
                max_depth=s.max_depth,
                max_retries=s.max_retries,
            )
            for _ in range(s.worker_count)
        ]
        self._tasks = [
            asyncio.create_task(w.run(worker_id=i), name=f"worker-{i}")
            for i, w in enumerate(self._workers)
        ]

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._signal_stop)
            except NotImplementedError:
                pass  # Windows / restricted envs

        final: dict[str, int] = {}
        try:
            if s.run_seconds > 0:
                await asyncio.sleep(s.run_seconds)
            self._signal_stop()
            done, pending = await asyncio.wait(self._tasks, timeout=5.0)
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            _ = done
        finally:
            final = await metrics.snapshot()
            logger.info("metrics: %s", final)
            if isinstance(store, InMemoryStore):
                logger.info("demo pages stored: %s", await store.count())
            await self.shutdown()
        return final

    def _signal_stop(self) -> None:
        for w in self._workers:
            w.request_stop()

    async def shutdown(self) -> None:
        for w in self._workers:
            w.request_stop()
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._fetcher:
            await self._fetcher.close()
        if self._store:
            await self._store.close()
        await self._close_redis()

    async def _close_redis(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None


class _LocalMetrics:
    """Minimal metrics stand-in when Redis is down in demo mode."""

    def __init__(self) -> None:
        self._data = {f: 0 for f in Metrics.FIELDS}

    async def incr(self, field: str, amount: int = 1) -> int:
        self._data[field] = self._data.get(field, 0) + amount
        return self._data[field]

    async def snapshot(self) -> dict[str, int]:
        return dict(self._data)

    async def reset(self) -> None:
        for k in self._data:
            self._data[k] = 0
