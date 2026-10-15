"""PostgreSQL persistence via SQLAlchemy async + in-memory store for tests."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Protocol

from sqlalchemy import JSON, DateTime, Integer, MetaData, String, Text, column, func, table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from crawler.models import PageResult

logger = logging.getLogger(__name__)

metadata = MetaData()

pages = table(
    "pages",
    column("id", Integer),
    column("url", String),
    column("url_hash", String),
    column("status_code", Integer),
    column("title", String),
    column("content_type", String),
    column("body_text", Text),
    column("links", JSON),
    column("depth", Integer),
    column("crawled_at", DateTime(timezone=True)),
    column("error", Text),
)


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS pages (
    id            SERIAL PRIMARY KEY,
    url           TEXT NOT NULL,
    url_hash      CHAR(64) NOT NULL UNIQUE,
    status_code   INTEGER NOT NULL,
    title         TEXT,
    content_type  TEXT,
    body_text     TEXT,
    links         JSONB NOT NULL DEFAULT '[]'::jsonb,
    depth         INTEGER NOT NULL DEFAULT 0,
    crawled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error         TEXT
);
CREATE INDEX IF NOT EXISTS idx_pages_domain_depth ON pages ((split_part(url, '/', 3)), depth);
"""


class PageStore(Protocol):
    async def setup(self) -> None: ...
    async def save(self, page: PageResult) -> None: ...
    async def get_by_url(self, url: str) -> dict[str, Any] | None: ...
    async def count(self) -> int: ...
    async def close(self) -> None: ...


class PostgresStore:
    """
    Async SQLAlchemy store with upsert on url_hash.

    Complexity: save is O(1) round-trip upsert; count is O(1) approx with
    index-only scan on PK. Connection pool size should match worker concurrency.
    """

    def __init__(self, dsn: str, *, echo: bool = False) -> None:
        self._engine: AsyncEngine = create_async_engine(dsn, echo=echo, pool_pre_ping=True)
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    async def setup(self) -> None:
        async with self._engine.begin() as conn:
            for stmt in CREATE_SQL.strip().split(";"):
                s = stmt.strip()
                if s:
                    await conn.execute(text(s))

    async def save(self, page: PageResult) -> None:
        row = page.to_row()
        stmt = pg_insert(pages).values(
            url=row["url"],
            url_hash=row["url_hash"],
            status_code=row["status_code"],
            title=row["title"],
            content_type=row["content_type"],
            body_text=row["body_text"],
            links=row["links"],
            depth=row["depth"],
            crawled_at=func.now(),
            error=row["error"],
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["url_hash"],
            set_={
                "status_code": stmt.excluded.status_code,
                "title": stmt.excluded.title,
                "content_type": stmt.excluded.content_type,
                "body_text": stmt.excluded.body_text,
                "links": stmt.excluded.links,
                "depth": stmt.excluded.depth,
                "crawled_at": func.now(),
                "error": stmt.excluded.error,
            },
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(stmt)

    async def get_by_url(self, url: str) -> dict[str, Any] | None:
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        async with self._session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT url, url_hash, status_code, title, content_type, body_text, "
                    "links, depth, crawled_at, error FROM pages WHERE url_hash = :h"
                ),
                {"h": url_hash},
            )
            row = result.mappings().first()
            return dict(row) if row else None

    async def count(self) -> int:
        async with self._session_factory() as session:
            result = await session.execute(text("SELECT COUNT(*) FROM pages"))
            return int(result.scalar_one())

    async def close(self) -> None:
        await self._engine.dispose()


class InMemoryStore:
    """List-backed store for unit tests and demo mode without Postgres."""

    def __init__(self) -> None:
        self.pages: list[dict[str, Any]] = []
        self._by_hash: dict[str, int] = {}

    async def setup(self) -> None:
        return None

    async def save(self, page: PageResult) -> None:
        row = page.to_row()
        # JSON-serialize links for parity with JSONB
        row["links"] = list(row["links"])
        h = row["url_hash"]
        if h in self._by_hash:
            self.pages[self._by_hash[h]] = row
        else:
            self._by_hash[h] = len(self.pages)
            self.pages.append(row)

    async def get_by_url(self, url: str) -> dict[str, Any] | None:
        h = hashlib.sha256(url.encode()).hexdigest()
        idx = self._by_hash.get(h)
        if idx is None:
            return None
        return dict(self.pages[idx])

    async def count(self) -> int:
        return len(self.pages)

    async def close(self) -> None:
        return None

    def dump_json(self) -> str:
        return json.dumps(self.pages, indent=2, default=str)
