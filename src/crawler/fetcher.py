"""HTTP fetch with retry/backoff and optional demo mock transport."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

import aiohttp
import httpx

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FetchResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    body: bytes
    error: str | None = None


class Fetcher(Protocol):
    async def fetch(self, url: str) -> FetchResponse: ...
    async def close(self) -> None: ...


class AiohttpFetcher:
    """
    Concurrent HTTP GET via aiohttp with exponential backoff.

    Complexity per URL: O(1) network RTT + O(B) body buffer where B is
    response size. Retries add up to max_retries extra RTTs with backoff
    2^attempt * base. Cap concurrency externally with asyncio.Semaphore
    so total in-flight stays bounded (design target >10k across fleet).
    """

    def __init__(
        self,
        *,
        user_agent: str,
        timeout: float = 15.0,
        max_retries: int = 3,
        base_backoff: float = 0.5,
        session: aiohttp.ClientSession | None = None,
        allow_private_networks: bool = False,
    ) -> None:
        self._ua = user_agent
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_retries = max_retries
        self._base_backoff = base_backoff
        self._session = session
        self._owns_session = session is None
        self._allow_private = allow_private_networks

    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"User-Agent": self._ua},
            )
            self._owns_session = True
        return self._session

    async def fetch(self, url: str) -> FetchResponse:
        from crawler.security import UnsafeURLError, validate_crawl_url

        try:
            validate_crawl_url(url, allow_private=self._allow_private, resolve=True)
        except UnsafeURLError as exc:
            return FetchResponse(
                url=url,
                status_code=0,
                headers={},
                body=b"",
                error=f"ssrf_blocked: {exc}",
            )

        session = await self._ensure()
        last_error: str | None = None
        for attempt in range(self._max_retries + 1):
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    body = await resp.read()
                    headers = {k: v for k, v in resp.headers.items()}
                    if resp.status >= 500 and attempt < self._max_retries:
                        last_error = f"HTTP {resp.status}"
                        await asyncio.sleep(self._base_backoff * (2**attempt))
                        continue
                    return FetchResponse(
                        url=str(resp.url),
                        status_code=resp.status,
                        headers=headers,
                        body=body,
                    )
            except (TimeoutError, aiohttp.ClientError) as exc:
                last_error = str(exc)
                if attempt < self._max_retries:
                    await asyncio.sleep(self._base_backoff * (2**attempt))
                    continue
        return FetchResponse(
            url=url,
            status_code=0,
            headers={},
            body=b"",
            error=last_error or "unknown fetch error",
        )

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()


class HttpxFetcher:
    """Alternate transport using httpx.AsyncClient (same retry contract)."""

    def __init__(
        self,
        *,
        user_agent: str,
        timeout: float = 15.0,
        max_retries: int = 3,
        base_backoff: float = 0.5,
        allow_private_networks: bool = False,
    ) -> None:
        self._client = httpx.AsyncClient(
            headers={"User-Agent": user_agent},
            timeout=timeout,
            follow_redirects=True,
        )
        self._max_retries = max_retries
        self._base_backoff = base_backoff
        self._allow_private = allow_private_networks

    async def fetch(self, url: str) -> FetchResponse:
        from crawler.security import UnsafeURLError, validate_crawl_url

        try:
            validate_crawl_url(url, allow_private=self._allow_private, resolve=True)
        except UnsafeURLError as exc:
            return FetchResponse(
                url=url, status_code=0, headers={}, body=b"", error=f"ssrf_blocked: {exc}"
            )

        last_error: str | None = None
        for attempt in range(self._max_retries + 1):
            try:
                resp = await self._client.get(url)
                if resp.status_code >= 500 and attempt < self._max_retries:
                    last_error = f"HTTP {resp.status_code}"
                    await asyncio.sleep(self._base_backoff * (2**attempt))
                    continue
                return FetchResponse(
                    url=str(resp.url),
                    status_code=resp.status_code,
                    headers=dict(resp.headers),
                    body=resp.content,
                )
            except (TimeoutError, httpx.HTTPError) as exc:
                last_error = str(exc)
                if attempt < self._max_retries:
                    await asyncio.sleep(self._base_backoff * (2**attempt))
                    continue
        return FetchResponse(url=url, status_code=0, headers={}, body=b"", error=last_error)

    async def close(self) -> None:
        await self._client.aclose()


# Demo HTML fixtures for offline / CI demo mode
_DEMO_PAGES: dict[str, str] = {
    "https://example.com/": """<!DOCTYPE html><html><head><title>Example Domain</title></head>
<body><h1>Example Domain</h1>
<a href="https://example.com/about">About</a>
<a href="https://example.com/blog">Blog</a>
<a href="https://example.org/">Sister</a>
</body></html>""",
    "https://example.com/about": """<!DOCTYPE html><html><head><title>About</title></head>
<body><p>About page</p><a href="https://example.com/">Home</a></body></html>""",
    "https://example.com/blog": """<!DOCTYPE html><html><head><title>Blog</title></head>
<body><p>Blog</p>
<a href="https://example.com/blog/1">Post 1</a>
<a href="https://example.com/blog/2">Post 2</a>
</body></html>""",
    "https://example.com/blog/1": """<!DOCTYPE html><html><head><title>Post 1</title></head>
<body><p>Post one</p></body></html>""",
    "https://example.com/blog/2": """<!DOCTYPE html><html><head><title>Post 2</title></head>
<body><p>Post two</p></body></html>""",
    "https://example.org/": """<!DOCTYPE html><html><head><title>Example Org</title></head>
<body><a href="https://example.org/docs">Docs</a>
<a href="https://example.com/">Cross</a></body></html>""",
    "https://example.org/docs": """<!DOCTYPE html><html><head><title>Docs</title></head>
<body><p>Documentation</p></body></html>""",
    "https://example.net/": """<!DOCTYPE html><html><head><title>Example Net</title></head>
<body><p>Net root</p><a href="https://example.net/a">A</a></body></html>""",
    "https://example.net/a": """<!DOCTYPE html><html><head><title>A</title></head>
<body><p>Page A</p></body></html>""",
}


class MockFetcher:
    """In-memory HTTP stub for demo mode and deterministic tests."""

    def __init__(
        self,
        pages: dict[str, str] | None = None,
        *,
        fail_urls: set[str] | None = None,
    ) -> None:
        self._pages = pages if pages is not None else dict(_DEMO_PAGES)
        self._fail = fail_urls or set()
        self.calls: list[str] = []

    async def fetch(self, url: str) -> FetchResponse:
        from crawler.frontier import normalize_url

        url = normalize_url(url)
        self.calls.append(url)
        if url in self._fail:
            return FetchResponse(url=url, status_code=0, headers={}, body=b"", error="mock failure")
        html = self._pages.get(url)
        if html is None:
            return FetchResponse(
                url=url,
                status_code=404,
                headers={"content-type": "text/plain"},
                body=b"not found",
            )
        return FetchResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/html; charset=utf-8"},
            body=html.encode(),
        )

    async def close(self) -> None:
        return None

    def add_page(self, url: str, html: str) -> None:
        self._pages[url] = html
