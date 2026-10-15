"""Integration tests against live Redis and PostgreSQL.

Marked `integration`. Skips gracefully when services are unreachable.
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy.exc import SQLAlchemyError

from crawler.dedup import RedisDedup
from crawler.frontier import RedisFrontier
from crawler.metrics import Metrics
from crawler.models import CrawlJob, PageResult
from crawler.storage import PostgresStore

pytestmark = pytest.mark.integration

REDIS_URL = os.environ.get(
    "CRAWLER_REDIS_URL",
    "redis://:crawler_redis_secret@localhost:6379/0",
)
POSTGRES_DSN = os.environ.get(
    "CRAWLER_POSTGRES_DSN",
    "postgresql+asyncpg://crawler:crawler@localhost:5432/crawler",
)


async def _redis_reachable(url: str) -> bool:
    client = Redis.from_url(url, decode_responses=False, socket_connect_timeout=2, socket_timeout=2)
    try:
        await client.ping()
        return True
    except Exception:
        return False
    finally:
        await client.aclose()


async def _postgres_reachable(dsn: str) -> bool:
    store = PostgresStore(dsn)
    try:
        await store.setup()
        await store.count()
        return True
    except (OSError, SQLAlchemyError, Exception):
        return False
    finally:
        await store.close()


@pytest_asyncio.fixture
async def real_redis():
    if not await _redis_reachable(REDIS_URL):
        pytest.skip(f"Redis unreachable at {REDIS_URL}")
    client = Redis.from_url(
        REDIS_URL,
        decode_responses=False,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    prefix = f"itest:{uuid.uuid4().hex[:12]}"
    yield client, prefix
    # Cleanup keys used by this test run
    async for key in client.scan_iter(match=f"{prefix}*"):
        await client.delete(key)
    await client.aclose()


@pytest_asyncio.fixture
async def real_postgres():
    if not await _postgres_reachable(POSTGRES_DSN):
        pytest.skip(f"Postgres unreachable at {POSTGRES_DSN}")
    store = PostgresStore(POSTGRES_DSN)
    await store.setup()
    yield store
    await store.close()


@pytest.mark.asyncio
async def test_redis_frontier_enqueue_dequeue_and_dedup(real_redis):
    client, prefix = real_redis
    queue_key = f"{prefix}:queue"
    visited_key = f"{prefix}:visited"
    metrics_key = f"{prefix}:metrics"

    metrics = Metrics(client, metrics_key)
    dedup = RedisDedup(client, visited_key)
    frontier = RedisFrontier(
        client,
        dedup,
        metrics,
        queue_key=queue_key,
        max_depth=3,
        max_pages_per_domain=100,
    )

    url = f"https://example.com/itest/{prefix}"
    job = CrawlJob(url=url, depth=0)

    assert await frontier.enqueue(job) is True
    assert await frontier.size() == 1
    assert await dedup.seen(url) is True

    # Second enqueue of same URL must be rejected (dedup)
    assert await frontier.enqueue(CrawlJob(url=url, depth=0)) is False
    assert await frontier.size() == 1
    snap = await metrics.snapshot()
    assert snap.get("dedup_hits", 0) >= 1

    got = await frontier.dequeue(timeout=2)
    assert got is not None
    assert got.url == url
    assert await frontier.size() == 0


@pytest.mark.asyncio
async def test_postgres_insert_and_query_pages(real_postgres):
    store = real_postgres
    unique = uuid.uuid4().hex
    url = f"https://example.com/itest-page/{unique}"

    page = PageResult(
        url=url,
        status_code=200,
        title=f"Integration {unique}",
        content_type="text/html",
        body_text="hello integration",
        links=["https://example.com/child"],
        depth=1,
    )
    await store.save(page)

    row = await store.get_by_url(url)
    assert row is not None
    assert row["url"] == url
    assert row["status_code"] == 200
    assert row["title"] == f"Integration {unique}"
    assert row["depth"] == 1
    assert await store.count() >= 1

    # Upsert updates existing url_hash
    updated = PageResult(
        url=url,
        status_code=200,
        title=f"Updated {unique}",
        content_type="text/html",
        body_text="updated body",
        links=[],
        depth=2,
    )
    await store.save(updated)
    row2 = await store.get_by_url(url)
    assert row2 is not None
    assert row2["title"] == f"Updated {unique}"
    assert row2["depth"] == 2
