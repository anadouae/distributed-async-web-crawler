"""Crawl worker — dequeue, robots check, fetch, parse, store, enqueue children."""

from __future__ import annotations

import asyncio
import logging

from crawler.fetcher import Fetcher, FetchResponse
from crawler.frontier import Frontier
from crawler.metrics import Metrics
from crawler.models import CrawlJob, PageResult
from crawler.parser import parse_page
from crawler.robots import RobotsCache
from crawler.storage import PageStore

logger = logging.getLogger(__name__)


class Worker:
    """
    Single logical worker that pulls jobs and processes them under a semaphore.

    Design for >10k in-flight:
      - Raise `max_concurrency` (semaphore permits) per process toward OS/fd limits
      - Scale `worker_count` (coroutine pool size) or run N container replicas
      - Keep Redis BRPOP short so idle workers don't hold connections forever
      - Prefer connection pooling for HTTP (aiohttp) and Postgres

    Critical path complexity per job:
      dequeue O(1) → robots O(R) → fetch O(RTT) → parse O(N) → save O(1)
      → enqueue children O(C) where C is link count (bounded by max_pages_per_domain)
    """

    def __init__(
        self,
        *,
        frontier: Frontier,
        fetcher: Fetcher,
        store: PageStore,
        robots: RobotsCache,
        metrics: Metrics,
        semaphore: asyncio.Semaphore,
        max_depth: int = 3,
        max_retries: int = 3,
    ) -> None:
        self._frontier = frontier
        self._fetcher = fetcher
        self._store = store
        self._robots = robots
        self._metrics = metrics
        self._sem = semaphore
        self._max_depth = max_depth
        self._max_retries = max_retries
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self, worker_id: int = 0) -> None:
        logger.info("worker-%s started", worker_id)
        while not self._stop.is_set():
            job = await self._frontier.dequeue(timeout=0.5)
            if job is None:
                # Yield so in-memory frontiers don't busy-spin while idle
                await asyncio.sleep(0.05)
                continue
            async with self._sem:
                await self.process(job)
        logger.info("worker-%s stopped", worker_id)

    async def process(self, job: CrawlJob) -> PageResult:
        """Process one job; exposed for unit tests."""
        if not await self._robots.allowed(job.url):
            await self._metrics.incr("robots_blocked")
            result = PageResult(
                url=job.url,
                status_code=0,
                title=None,
                content_type=None,
                body_text=None,
                links=[],
                depth=job.depth,
                error="robots.txt disallowed",
            )
            await self._store.save(result)
            await self._metrics.incr("pages_stored")
            return result

        resp = await self._fetcher.fetch(job.url)
        if resp.error or resp.status_code == 0:
            return await self._handle_failure(job, resp)

        if resp.status_code >= 400:
            await self._metrics.incr("fetched_error")
            result = PageResult(
                url=job.url,
                status_code=resp.status_code,
                title=None,
                content_type=resp.headers.get("content-type"),
                body_text=None,
                links=[],
                depth=job.depth,
                error=f"HTTP {resp.status_code}",
            )
            await self._store.save(result)
            await self._metrics.incr("pages_stored")
            return result

        content_type = resp.headers.get("content-type", "")
        title: str | None = None
        body_text: str | None = None
        links: list[str] = []

        if "html" in content_type or resp.body[:100].lstrip().lower().startswith(
            (b"<!doctype", b"<html")
        ):
            try:
                title, body_text, links = parse_page(job.url, resp.body)
                await self._metrics.incr("links_extracted", len(links))
            except Exception as exc:
                logger.warning("parse failed for %s: %s", job.url, exc)
                body_text = resp.body.decode("utf-8", errors="replace")[:5000]

        await self._metrics.incr("fetched_ok")
        result = PageResult(
            url=job.url,
            status_code=resp.status_code,
            title=title,
            content_type=content_type or None,
            body_text=body_text,
            links=links,
            depth=job.depth,
        )
        await self._store.save(result)
        await self._metrics.incr("pages_stored")

        if job.depth < self._max_depth:
            for link in links:
                child = CrawlJob(url=link, depth=job.depth + 1, parent_url=job.url)
                await self._frontier.enqueue(child)

        return result

    async def _handle_failure(self, job: CrawlJob, resp: FetchResponse) -> PageResult:
        await self._metrics.incr("fetched_error")
        if job.retries < self._max_retries:
            await self._metrics.incr("retries")
            retry = CrawlJob(
                url=job.url,
                depth=job.depth,
                retries=job.retries + 1,
                parent_url=job.parent_url,
            )
            # Re-enqueue: dedup already marked URL, so we need a bypass.
            # Workers re-queue via a dedicated retry path that skips remounting
            # into the visited set by using a lightweight direct push when available.
            await self._requeue(retry)

        result = PageResult(
            url=job.url,
            status_code=resp.status_code,
            title=None,
            content_type=None,
            body_text=None,
            links=[],
            depth=job.depth,
            error=resp.error or "fetch failed",
        )
        await self._store.save(result)
        await self._metrics.incr("pages_stored")
        return result

    async def _requeue(self, job: CrawlJob) -> None:
        """Best-effort retry enqueue that tolerates already-marked URLs."""
        # Prefer direct LPUSH when RedisFrontier is in use
        redis = getattr(self._frontier, "_redis", None)
        queue_key = getattr(self._frontier, "_queue_key", None)
        if redis is not None and queue_key is not None:
            await redis.lpush(queue_key, job.to_json())
            await self._metrics.incr("enqueued")
            return
        # In-memory frontier: append without remounting dedup
        q = getattr(self._frontier, "_q", None)
        if q is not None:
            q.appendleft(job)
            await self._metrics.incr("enqueued")
