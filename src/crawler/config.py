"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Crawler settings — secrets and overrides via env vars or `.env` (never commit `.env`)."""

    model_config = SettingsConfigDict(env_prefix="CRAWLER_", env_file=".env", extra="ignore")

    # Redis — use password form when requirepass is set, e.g.
    # redis://:crawler_redis_secret@localhost:6379/0
    redis_url: str = "redis://localhost:6379/0"
    queue_key: str = "crawler:queue"
    visited_key: str = "crawler:visited"
    metrics_key: str = "crawler:metrics"
    robots_cache_key: str = "crawler:robots"

    # PostgreSQL
    postgres_dsn: str = "postgresql+asyncpg://crawler:crawler@localhost:5432/crawler"

    # Concurrency — semaphore + worker pool targets >10k in-flight at scale
    max_concurrency: int = Field(default=256, ge=1, le=50_000)
    worker_count: int = Field(default=8, ge=1, le=10_000)
    http_timeout: float = 15.0
    max_retries: int = 3
    base_backoff_seconds: float = 0.5

    # Crawl policy
    max_depth: int = 3
    max_pages_per_domain: int = 100
    allowed_domains: list[str] = Field(default_factory=list)
    user_agent: str = "DistributedAsyncCrawler/1.0 (+https://github.com/anadouae/distributed-async-web-crawler)"
    respect_robots: bool = True

    # Security — block private/loopback SSRF targets unless explicitly allowed
    allow_private_networks: bool = False

    # Demo / seed — demo_mode defaults False (real Redis/Postgres/HTTP)
    seed_urls: list[str] = Field(
        default_factory=lambda: [
            "https://example.com/",
            "https://example.org/",
            "https://example.net/",
        ]
    )
    demo_mode: bool = False  # when True, use in-memory mock HTTP responses
    run_seconds: float = 30.0  # graceful stop after N seconds (0 = run forever)

    @model_validator(mode="before")
    @classmethod
    def _alias_allow_private(cls, data: Any) -> Any:
        """Accept unprefixed ALLOW_PRIVATE_NETWORKS as well as CRAWLER_ prefix."""
        if not isinstance(data, dict):
            return data
        if data.get("allow_private_networks") is not None:
            return data
        raw = os.environ.get("ALLOW_PRIVATE_NETWORKS")
        if raw is not None:
            out = dict(data)
            out["allow_private_networks"] = raw
            return out
        return data

    @field_validator("allow_private_networks", "demo_mode", "respect_robots", mode="before")
    @classmethod
    def _parse_bool(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on")
        return v


def get_settings() -> Settings:
    return Settings()
