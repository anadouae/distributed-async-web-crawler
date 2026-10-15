"""Domain models for crawl jobs and stored pages."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse


@dataclass(slots=True)
class CrawlJob:
    """A single URL unit of work on the frontier queue."""

    url: str
    depth: int = 0
    retries: int = 0
    parent_url: str | None = None
    enqueued_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @property
    def domain(self) -> str:
        return urlparse(self.url).netloc.lower()

    @property
    def url_hash(self) -> str:
        return hashlib.sha256(self.url.encode()).hexdigest()

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str | bytes) -> CrawlJob:
        data = json.loads(raw)
        return cls(**data)


@dataclass(slots=True)
class PageResult:
    """Structured crawl result ready for Postgres persistence."""

    url: str
    status_code: int
    title: str | None
    content_type: str | None
    body_text: str | None
    links: list[str]
    depth: int
    crawled_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    error: str | None = None

    def to_row(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "url_hash": hashlib.sha256(self.url.encode()).hexdigest(),
            "status_code": self.status_code,
            "title": self.title,
            "content_type": self.content_type,
            "body_text": (self.body_text or "")[:50_000],
            "links": self.links[:500],
            "depth": self.depth,
            "crawled_at": self.crawled_at,
            "error": self.error,
        }
