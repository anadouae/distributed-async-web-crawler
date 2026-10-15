"""HTML parsing — title, text, and link extraction."""

from __future__ import annotations

from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.frontier import normalize_url


def extract_links(base_url: str, html: str | bytes) -> list[str]:
    """
    Extract absolute http(s) links from HTML.

    Complexity: O(N) over DOM nodes via BeautifulSoup/lxml; typically
    dominated by parse cost, not link filtering. Output is de-duplicated
    while preserving first-seen order.
    """
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    links: list[str] = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith(("#", "mailto:", "javascript:", "tel:")):
            continue
        absolute = normalize_url(urljoin(base_url, href))
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


def extract_title(html: str | bytes) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True) or None
    return None


def extract_text(html: str | bytes, limit: int = 20_000) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = " ".join(soup.get_text(separator=" ", strip=True).split())
    return text[:limit]


def parse_page(url: str, html: str | bytes) -> tuple[str | None, str, list[str]]:
    """Return (title, body_text, links)."""
    return extract_title(html), extract_text(html), extract_links(url, html)
