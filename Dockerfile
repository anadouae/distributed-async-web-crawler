FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1 \
    CRAWLER_REDIS_URL=redis://redis:6379/0 \
    CRAWLER_POSTGRES_DSN=postgresql+asyncpg://crawler:crawler@postgres:5432/crawler

CMD ["python", "-m", "crawler", "--demo", "--seconds", "10", "-v"]
