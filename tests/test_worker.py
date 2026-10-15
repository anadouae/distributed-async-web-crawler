"""Worker process: fetch, parse, store, enqueue children, robots, retries."""

from __future__ import annotations

import asyncio

import pytest

from crawler.fetcher import MockFetcher
from crawler.frontier import InMemoryFrontier
from crawler.models import CrawlJob
from crawler.robots import RobotsCache
from crawler.storage import InMemoryStore
from crawler.worker import Worker


@pytest.mark.asyncio
async def test_worker_process_parses_and_enqueues_children(
    frontier_mem, mock_fetcher, store, metrics, robots_open
):
    await frontier_mem.enqueue(CrawlJob(url="https://example.com/", depth=0))
    # clear queue — process takes an explicit job
    job = await frontier_mem.dequeue()
    assert job is not None

    worker = Worker(
        frontier=frontier_mem,
        fetcher=mock_fetcher,
        store=store,
        robots=robots_open,
        metrics=metrics,
        semaphore=asyncio.Semaphore(4),
        max_depth=2,
    )
    result = await worker.process(job)
    assert result.status_code == 200
    assert result.title == "Example Domain"
    assert await store.count() == 1
    assert await frontier_mem.size() >= 1  # about, blog, sister enqueued
    snap = await metrics.snapshot()
    assert snap["fetched_ok"] == 1
    assert snap["pages_stored"] == 1
    assert snap["links_extracted"] >= 2


@pytest.mark.asyncio
async def test_worker_respects_robots(dedup_mem, metrics, store):
    frontier = InMemoryFrontier(dedup_mem, metrics, max_depth=1)
    fetcher = MockFetcher()

    class BlockAll(RobotsCache):
        async def allowed(self, url: str) -> bool:
            return False

    worker = Worker(
        frontier=frontier,
        fetcher=fetcher,
        store=store,
        robots=BlockAll(enabled=True),
        metrics=metrics,
        semaphore=asyncio.Semaphore(1),
    )
    result = await worker.process(CrawlJob(url="https://example.com/", depth=0))
    assert result.error and "robots" in result.error
    snap = await metrics.snapshot()
    assert snap["robots_blocked"] == 1
    assert len(fetcher.calls) == 0


@pytest.mark.asyncio
async def test_worker_retry_on_fetch_failure(dedup_mem, metrics, store, robots_open):
    frontier = InMemoryFrontier(dedup_mem, metrics, max_depth=1)
    fetcher = MockFetcher(fail_urls={"https://example.com/fail"})
    # Pre-mark so enqueue path already consumed; process uses requeue bypass
    await frontier.enqueue(CrawlJob(url="https://example.com/fail", depth=0))
    job = await frontier.dequeue()

    worker = Worker(
        frontier=frontier,
        fetcher=fetcher,
        store=store,
        robots=robots_open,
        metrics=metrics,
        semaphore=asyncio.Semaphore(1),
        max_retries=2,
    )
    result = await worker.process(job)
    assert result.error
    snap = await metrics.snapshot()
    assert snap["fetched_error"] >= 1
    assert snap["retries"] >= 1
    assert await frontier.size() == 1  # requeued


@pytest.mark.asyncio
async def test_robots_cache_parses_disallow(redis):
    async def fetcher(url: str) -> str:
        return "User-agent: *\nDisallow: /private\n"

    cache = RobotsCache(redis, enabled=True, user_agent="TestBot", fetcher=fetcher)
    assert await cache.allowed("https://example.com/") is True
    assert await cache.allowed("https://example.com/private/secret") is False


@pytest.mark.asyncio
async def test_end_to_end_demo_pool(metrics, robots_open):
    """Small worker pool crawls mocked graph until frontier drains."""
    from crawler.dedup import InMemoryDedup

    dedup = InMemoryDedup()
    frontier = InMemoryFrontier(dedup, metrics, max_depth=2, max_pages_per_domain=20)
    store = InMemoryStore()
    fetcher = MockFetcher()
    await frontier.enqueue(CrawlJob(url="https://example.com/", depth=0))

    sem = asyncio.Semaphore(8)
    workers = [
        Worker(
            frontier=frontier,
            fetcher=fetcher,
            store=store,
            robots=robots_open,
            metrics=metrics,
            semaphore=sem,
            max_depth=2,
        )
        for _ in range(3)
    ]

    async def run_until_idle(w: Worker, wid: int) -> None:
        idle = 0
        while idle < 5:
            job = await frontier.dequeue(timeout=0.05)
            if job is None:
                idle += 1
                await asyncio.sleep(0.02)
                continue
            idle = 0
            async with sem:
                await w.process(job)

    await asyncio.gather(*(run_until_idle(w, i) for i, w in enumerate(workers)))
    assert await store.count() >= 3
    snap = await metrics.snapshot()
    assert snap["fetched_ok"] >= 3
