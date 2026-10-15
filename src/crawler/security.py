"""URL validation and SSRF guards for crawl targets."""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse

# RFC1918 + loopback — blocked unless ALLOW_PRIVATE_NETWORKS / CRAWLER_ALLOW_PRIVATE_NETWORKS
_PRIVATE_NETWORKS = (
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
)

_BLOCKED_HOSTNAMES = frozenset({"localhost", "localhost.localdomain"})


class UnsafeURLError(ValueError):
    """Raised when a URL fails scheme or private-network policy checks."""


def _hostname_looks_private(hostname: str) -> bool:
    host = hostname.lower().rstrip(".")
    if host in _BLOCKED_HOSTNAMES or host.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(ip in net for net in _PRIVATE_NETWORKS)


def _resolved_ips_private(hostname: str) -> bool:
    """Resolve hostname; treat resolution failure as non-private (fetch will fail later)."""
    try:
        infos = socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        try:
            ip = ipaddress.ip_address(sockaddr[0])
        except ValueError:
            continue
        if any(ip in net for net in _PRIVATE_NETWORKS):
            return True
    return False


def is_private_target(hostname: str, *, resolve: bool = True) -> bool:
    """Return True if hostname is loopback/RFC1918 (literal or DNS-resolved)."""
    if not hostname:
        return True
    if _hostname_looks_private(hostname):
        return True
    if resolve:
        return _resolved_ips_private(hostname)
    return False


def validate_crawl_url(url: str, *, allow_private: bool = False, resolve: bool = True) -> str:
    """
    Validate a seed/crawl URL.

    - Scheme must be http or https
    - Host must not target private ranges (127/10/172.16/192.168) unless allow_private

    Returns the stripped URL on success; raises UnsafeURLError otherwise.
    """
    raw = (url or "").strip()
    if not raw:
        raise UnsafeURLError("empty URL")

    parsed = urlparse(raw)
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        raise UnsafeURLError(f"unsupported scheme {scheme!r}; only http/https allowed")

    hostname = parsed.hostname
    if not hostname:
        raise UnsafeURLError("URL missing hostname")

    if not allow_private and is_private_target(hostname, resolve=resolve):
        raise UnsafeURLError(
            f"refusing private/loopback target {hostname!r} "
            "(set ALLOW_PRIVATE_NETWORKS=true or CRAWLER_ALLOW_PRIVATE_NETWORKS=true to override)"
        )

    return raw
