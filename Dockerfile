FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONFAULTHANDLER=1 \
    MALLOC_ARENA_MAX=2 \
    TZ=UTC

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates curl tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for layer caching.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Drop pytest from the runtime image to keep RAM/space small (kept in requirements for dev).
RUN pip uninstall -y pytest pytest-asyncio || true

COPY . /app

RUN mkdir -p /data
VOLUME ["/data"]

# Engine and dashboard run as separate services in docker-compose.
# By default the image runs the engine; docker-compose overrides for dashboard.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "main"]
