"""Deduplication via Redis SET and in-memory store."""

from __future__ import annotations

import pytest

from crawler.dedup import InMemoryDedup
from crawler.models import CrawlJob


@pytest.mark.asyncio
async def test_inmemory_mark_is_idempotent():
    d = InMemoryDedup()
    assert await d.mark("https://example.com/a") is True
    assert await d.mark("https://example.com/a") is False
    assert await d.seen("https://example.com/a") is True
    assert await d.count() == 1


@pytest.mark.asyncio
async def test_redis_dedup(dedup_redis):
    assert await dedup_redis.mark("https://example.com/x") is True
    assert await dedup_redis.seen("https://example.com/x") is True
    assert await dedup_redis.mark("https://example.com/x") is False
    assert await dedup_redis.mark("https://example.com/y") is True
    assert await dedup_redis.count() == 2


@pytest.mark.asyncio
async def test_frontier_dedup_hit_increments_metrics(frontier_mem, metrics):
    job = CrawlJob(url="https://example.com/dup")
    assert await frontier_mem.enqueue(job) is True
    assert await frontier_mem.enqueue(CrawlJob(url="https://example.com/dup")) is False
    snap = await metrics.snapshot()
    assert snap["dedup_hits"] >= 1
    assert snap["enqueued"] == 1
