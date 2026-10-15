"""CLI entrypoint: `python -m crawler` or `crawler` console script."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from crawler import __version__
from crawler.config import Settings
from crawler.master import Master


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="crawler",
        description="Distributed Asynchronous Web Crawler (Master-Worker)",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument(
        "--demo",
        action="store_true",
        help="Run offline demo with mocked HTTP (no live network required)",
    )
    p.add_argument("--seed", nargs="*", default=None, help="Seed URL(s) to crawl")
    p.add_argument("--workers", type=int, default=None, help="Coroutine worker pool size")
    p.add_argument("--concurrency", type=int, default=None, help="Max in-flight fetches")
    p.add_argument("--max-depth", type=int, default=None)
    p.add_argument("--seconds", type=float, default=None, help="Run duration before stop")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    settings = Settings()
    if args.demo:
        settings.demo_mode = True
        settings.respect_robots = False
        settings.run_seconds = args.seconds if args.seconds is not None else 5.0
        settings.worker_count = args.workers or 4
        settings.max_concurrency = args.concurrency or 32
        settings.max_depth = args.max_depth if args.max_depth is not None else 2
    else:
        if args.workers is not None:
            settings.worker_count = args.workers
        if args.concurrency is not None:
            settings.max_concurrency = args.concurrency
        if args.max_depth is not None:
            settings.max_depth = args.max_depth
        if args.seconds is not None:
            settings.run_seconds = args.seconds

    master = Master(settings)
    try:
        metrics = asyncio.run(master.run(urls=args.seed))
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("interrupted")
        return 130
    except Exception:
        logging.getLogger(__name__).exception("crawler failed")
        return 1

    print("=== Crawl metrics ===")
    for k, v in sorted(metrics.items()):
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
