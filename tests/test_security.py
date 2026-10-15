"""Unit tests for URL validation and SSRF private-network blocking."""

from __future__ import annotations

import pytest

from crawler.security import UnsafeURLError, validate_crawl_url


def test_http_https_allowed():
    assert validate_crawl_url("https://example.com/path", resolve=False) == "https://example.com/path"
    assert validate_crawl_url("http://example.org/", resolve=False) == "http://example.org/"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",
        "file:///etc/passwd",
        "javascript:alert(1)",
        "example.com/no-scheme",
        "",
    ],
)
def test_rejects_non_http_schemes(url: str):
    with pytest.raises(UnsafeURLError):
        validate_crawl_url(url, resolve=False)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/",
        "http://127.0.0.1:8080/admin",
        "http://10.0.0.5/internal",
        "http://172.16.1.1/",
        "http://192.168.1.10/",
        "http://localhost/",
        "http://localhost.localdomain/x",
    ],
)
def test_blocks_private_literal_targets(url: str):
    with pytest.raises(UnsafeURLError):
        validate_crawl_url(url, allow_private=False, resolve=False)


def test_allow_private_networks_override():
    url = "http://127.0.0.1/health"
    assert validate_crawl_url(url, allow_private=True, resolve=False) == url
