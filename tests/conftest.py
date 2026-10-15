"""Shared fixtures — fakeredis + in-memory stores."""

from __future__ import annotations

import pytest
import pytest_asyncio
from fakeredis import aioredis as fakeredis

from crawler.dedup import InMemoryDedup, RedisDedup
from crawler.fetcher import MockFetcher
from crawler.frontier import InMemoryFrontier, RedisFrontier
from crawler.metrics import Metrics
from crawler.robots import RobotsCache
from crawler.storage import InMemoryStore


@pytest_asyncio.fixture
async def redis():
    client = fakeredis.FakeRedis(decode_responses=False)
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture
async def metrics(redis):
    m = Metrics(redis, "test:metrics")
    await m.reset()
    return m


@pytest.fixture
def dedup_mem():
    return InMemoryDedup()


@pytest_asyncio.fixture
async def dedup_redis(redis):
    return RedisDedup(redis, "test:visited")


@pytest.fixture
def store():
    return InMemoryStore()


@pytest.fixture
def mock_fetcher():
    return MockFetcher()


@pytest_asyncio.fixture
async def frontier_mem(dedup_mem, metrics):
    return InMemoryFrontier(dedup_mem, metrics, max_depth=3, max_pages_per_domain=50)


@pytest_asyncio.fixture
async def frontier_redis(redis, dedup_redis, metrics):
    return RedisFrontier(
        redis,
        dedup_redis,
        metrics,
        queue_key="test:queue",
        max_depth=3,
        max_pages_per_domain=50,
    )


@pytest.fixture
def robots_open():
    return RobotsCache(enabled=False)
