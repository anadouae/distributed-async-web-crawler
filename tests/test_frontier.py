"""Frontier enqueue/dequeue, depth limits, domain quotas, normalization."""

from __future__ import annotations

import pytest

from crawler.frontier import InMemoryFrontier, normalize_url
from crawler.models import CrawlJob


def test_normalize_url_strips_fragment_and_lowercases():
    assert normalize_url("HTTPS://Example.COM/Path/#section") == "https://example.com/Path"
    assert normalize_url("https://example.com/") == "https://example.com/"
    assert normalize_url("https://example.com/a/?q=1") == "https://example.com/a?q=1"


@pytest.mark.asyncio
async def test_enqueue_dequeue_roundtrip(frontier_mem):
    job = CrawlJob(url="https://example.com/page", depth=0)
    assert await frontier_mem.enqueue(job) is True
    assert await frontier_mem.size() == 1
    got = await frontier_mem.dequeue()
    assert got is not None
    assert got.url == "https://example.com/page"
    assert await frontier_mem.size() == 0


@pytest.mark.asyncio
async def test_enqueue_rejects_over_depth(frontier_mem):
    assert await frontier_mem.enqueue(CrawlJob(url="https://example.com/deep", depth=99)) is False


@pytest.mark.asyncio
async def test_enqueue_respects_allowed_domains(dedup_mem, metrics):
    frontier = InMemoryFrontier(
        dedup_mem,
        metrics,
        allowed_domains=["allowed.test"],
        max_depth=2,
    )
    assert await frontier.enqueue(CrawlJob(url="https://allowed.test/a")) is True
    assert await frontier.enqueue(CrawlJob(url="https://other.test/a")) is False


@pytest.mark.asyncio
async def test_domain_page_limit(dedup_mem, metrics):
    frontier = InMemoryFrontier(dedup_mem, metrics, max_pages_per_domain=2, max_depth=5)
    assert await frontier.enqueue(CrawlJob(url="https://example.com/1")) is True
    assert await frontier.enqueue(CrawlJob(url="https://example.com/2")) is True
    assert await frontier.enqueue(CrawlJob(url="https://example.com/3")) is False


@pytest.mark.asyncio
async def test_redis_frontier_enqueue_dequeue(frontier_redis):
    assert await frontier_redis.enqueue(CrawlJob(url="https://example.com/r")) is True
    assert await frontier_redis.size() == 1
    job = await frontier_redis.dequeue(timeout=1)
    assert job is not None
    assert job.url == "https://example.com/r"


@pytest.mark.asyncio
async def test_enqueue_rejects_private_urls(dedup_mem, metrics):
    frontier = InMemoryFrontier(dedup_mem, metrics, allow_private_networks=False)
    assert await frontier.enqueue(CrawlJob(url="http://127.0.0.1/")) is False
    assert await frontier.enqueue(CrawlJob(url="http://192.168.0.1/")) is False
    assert await frontier.enqueue(CrawlJob(url="ftp://example.com/")) is False
    assert await frontier.enqueue(CrawlJob(url="https://example.com/ok")) is True
