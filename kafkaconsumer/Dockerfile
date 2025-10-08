# kafkaconsumer/Dockerfile
# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.11-slim

############################
# Base (shared)
############################
FROM python:${PYTHON_VERSION} AS base
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Tiny utilities (ps for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

# Non-root user & workdir
RUN useradd -ms /bin/bash appuser
WORKDIR /app

############################
# Test image (installs test deps + runs pytest)
############################
FROM base AS test

# Dependencies
COPY requirements.txt /app/requirements.txt
# If you have requirements-dev.txt, uncomment the next line and install it too
# COPY requirements-dev.txt /app/requirements-dev.txt
RUN python -m pip install --upgrade pip && \
    pip install -r /app/requirements.txt && \
    # If you don't maintain a requirements-dev.txt, install the test tools here:
    pip install pytest pytest-asyncio pytest-cov httpx

# App + tests
COPY kafkaconsumer.py /app/kafkaconsumer.py
# Ensure your test file is copied (adjust name/path if different)
COPY test_kafkaconsumer.py /app/test_kafkaconsumer.py

# Permissions
RUN chown -R appuser:appuser /app
USER appuser

ENV COVERAGE_FILE=/coverage/.coverage.kafkaconsumer
CMD pytest -q \
  --maxfail=1 \
  --cov=kafkaconsumer \
  --cov-report=xml:/coverage/kafkaconsumer-coverage.xml \
  --cov-report=term-missing
# Default command for the test image
# CMD ["pytest", "-vv", "--cov=/app", "--cov-report=term-missing", "/app/test_kafkaconsumer.py"]

############################
# Runtime image (lean, prod)
############################
FROM base AS runtime

# Default envs (override in compose/env)
ENV KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    KAFKA_TOPIC=trades \
    KAFKA_GROUP=trade-consumer-A \
    MONGO_URI=mongodb://127.0.0.1:27017 \
    MONGO_DB=trade_store \
    MONGO_COLL=trades \
    POLL_TIMEOUT_MS=1000 \
    MAX_RECORDS=1000 \
    BULK_CHUNK=500 \
    GRACE_TIMEOUT_S=15.0


# Install only runtime deps
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# App code
COPY kafkaconsumer.py /app/kafkaconsumer.py
# install tini
RUN apt-get update && apt-get install -y --no-install-recommends tini \
  && rm -rf /var/lib/apt/lists/*

# Permissions
RUN chown -R appuser:appuser /app
USER appuser

# Simple liveness check: ensure the Python process (PID 1) is running
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ps -o comm= 1 | grep -q python || exit 1

# make tini the entrypoint so it becomes PID 1 and forwards signals
ENTRYPOINT ["/usr/bin/tini","--"]
# Run consumer
CMD ["python", "/app/kafkaconsumer.py"]