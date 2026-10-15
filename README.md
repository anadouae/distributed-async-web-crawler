# Distributed Asynchronous Web Crawler

Staff-grade **Master–Worker** crawler in Python 3.11+ using **asyncio**, a **Redis** job queue, and **PostgreSQL** result storage. Designed so a single process can hold thousands of in-flight fetches (semaphore + worker pool), and a fleet can scale past **10k** concurrent crawls.

**Author:** Douae El Assal · **License:** MIT

## Architecture

```mermaid
flowchart TB
  subgraph Master
    SEED[Seed URLs]
    ORCH[Orchestrator]
    POOL[Worker Pool<br/>N coroutines]
    SEM[asyncio.Semaphore<br/>max in-flight]
  end

  subgraph Redis
    Q[(LIST frontier<br/>LPUSH / BRPOP)]
    VIS[(SET visited<br/>URL hashes)]
    MET[(HASH metrics)]
    ROB[(HASH robots cache)]
  end

  subgraph Workers
    W1[dequeue]
    W2[robots.txt]
    W3[HTTP fetch<br/>aiohttp + backoff]
    W4[parse HTML]
    W5[enqueue children]
  end

  PG[(PostgreSQL<br/>pages table)]

  SEED --> ORCH --> POOL
  POOL --> SEM
  SEM --> W1
  W1 --> Q
  W1 --> W2 --> ROB
  W2 --> W3
  W3 --> W4 --> W5
  W5 --> Q
  W5 --> VIS
  W4 --> PG
  W1 --> MET
  W3 --> MET
```

### Scaling past 10k in-flight

| Lever | Knob | Notes |
| --- | --- | --- |
| Process concurrency | `CRAWLER_MAX_CONCURRENCY` | Caps simultaneous HTTP GETs via `asyncio.Semaphore` |
| Worker pool | `CRAWLER_WORKER_COUNT` | Number of dequeue/process coroutines per process |
| Horizontal replicas | `docker compose up --scale crawler=N` | Shared Redis queue + Postgres; workers are stateless |
| Redis | Redis 7+ | LIST/SET ops are O(1); shard by domain if one list heats up |
| OS limits | `ulimit -n`, HTTP connector limit | Raise file descriptors when concurrency ≫ 1k |

## Critical-path complexity

| Function | Complexity | Notes |
| --- | --- | --- |
| `RedisFrontier.enqueue` | **O(1)** | Normalize → `SISMEMBER`/`SADD` → domain `INCR` → `LPUSH` |
| `RedisFrontier.dequeue` | **O(1)** | `BRPOP` with timeout; no scan of the queue |
| `AiohttpFetcher.fetch` | **O(RTT · retries)** | Exponential backoff `base · 2^attempt`; body buffer O(B) |
| `RedisDedup.mark/seen` | **O(1)** avg | SHA-256 URL id in a Redis SET; memory ~32–64 B/URL |
| `parse_page` / `extract_links` | **O(N)** | DOM size; links capped when enqueued by domain quota |

## Features

- URL frontier with depth + per-domain caps and optional allow-list
- Basic **robots.txt** respect (cached per host)
- **Dedup** via Redis SET of URL hashes
- Retry with exponential backoff on 5xx / network errors
- Structured page rows in Postgres (SQLAlchemy async + `asyncpg`)
- Atomic **metrics** counters (enqueued, fetched, dedup hits, robots blocks, …)
- **SSRF guards** — seed/fetch URLs must be `http`/`https`; private ranges blocked by default
- **`--demo`** — optional offline mock HTTP only (not the default)

## Quick start — real stack

Secrets live in environment variables (or a local `.env`). Copy the example and adjust:

```bash
cp .env.example .env
# edit .env — never commit it
```

```bash
# Install
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .

# Start Redis + Postgres (password-protected Redis in compose)
docker compose up -d redis postgres

# Live crawl against real Redis + Postgres + public HTTP
export CRAWLER_REDIS_URL=redis://:crawler_redis_secret@localhost:6379/0
export CRAWLER_POSTGRES_DSN=postgresql+asyncpg://crawler:crawler@localhost:5432/crawler
python -m crawler --seed https://example.com/ --workers 8 --concurrency 64 --seconds 30
```

Or run the full compose stack (crawler defaults to **real** mode, not demo):

```bash
docker compose up --build
```

### Offline demo (optional)

`--demo` / `CRAWLER_DEMO_MODE=true` is **offline-only** — mocked HTTP and in-memory stores when you have no Redis/Postgres/network. Prefer the real stack above for portfolio / production use.

```bash
python -m crawler --demo --seconds 5 -v
```

## Security notes

| Control | Default | Override |
| --- | --- | --- |
| Secrets | From env / `.env` only (`.env` gitignored) | See `.env.example` |
| Redis auth | Password URL `redis://:SECRET@host:6379/0` | `CRAWLER_REDIS_URL` |
| Seed / crawl schemes | `http` and `https` only | — |
| Private networks | **Blocked** (`127/8`, `10/8`, `172.16/12`, `192.168/16`, `localhost`) | `ALLOW_PRIVATE_NETWORKS=true` or `CRAWLER_ALLOW_PRIVATE_NETWORKS=true` |
| `demo_mode` | **`false`** | `--demo` flag or `CRAWLER_DEMO_MODE=true` |

## Configuration

All settings use the `CRAWLER_` prefix (see `src/crawler/config.py`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `CRAWLER_REDIS_URL` | `redis://localhost:6379/0` | Queue / dedup / metrics (use password form when `requirepass` is set) |
| `CRAWLER_POSTGRES_DSN` | `postgresql+asyncpg://crawler:crawler@localhost:5432/crawler` | Results DSN |
| `CRAWLER_MAX_CONCURRENCY` | `256` | Semaphore permits |
| `CRAWLER_WORKER_COUNT` | `8` | Coroutine workers |
| `CRAWLER_MAX_DEPTH` | `3` | Link-follow depth |
| `CRAWLER_MAX_PAGES_PER_DOMAIN` | `100` | Soft domain budget |
| `CRAWLER_DEMO_MODE` | `false` | Mock HTTP + in-memory store (offline only) |
| `CRAWLER_RESPECT_ROBOTS` | `true` | Honor robots.txt |
| `CRAWLER_ALLOW_PRIVATE_NETWORKS` | `false` | Allow crawl of private/loopback hosts |

## Tests & CI

```bash
# Unit tests (fakeredis / in-memory — always)
pytest -q -m "not integration"

# Integration against live Redis + Postgres (skips if unreachable)
export CRAWLER_REDIS_URL=redis://:crawler_redis_secret@localhost:6379/0
export CRAWLER_POSTGRES_DSN=postgresql+asyncpg://crawler:crawler@localhost:5432/crawler
pytest -q -m integration

# Everything
pytest -q
ruff check src tests
```

GitHub Actions:

- **unit** — ruff + `pytest -m "not integration"` on Python 3.11 / 3.12
- **integration** — optional job with Redis + Postgres service containers; `pytest -m integration`

## Project layout

```
src/crawler/
  config.py      # pydantic-settings (env / .env)
  security.py    # scheme + SSRF private-range checks
  models.py      # CrawlJob, PageResult
  frontier.py    # Redis / in-memory URL queue
  dedup.py       # Redis SET / in-memory
  fetcher.py     # aiohttp, httpx, MockFetcher
  robots.py      # robots.txt cache
  parser.py      # BeautifulSoup extraction
  storage.py     # Postgres + InMemoryStore
  metrics.py     # Redis HASH counters
  worker.py      # process one job
  master.py      # seed + worker pool
  cli.py         # entrypoint
tests/           # unit + integration (test_integration_real.py)
.env.example     # secrets template (copy to .env)
Dockerfile
docker-compose.yml
```

## License

MIT © Douae El Assal
