# TradeData/Dockerfile
# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.11-slim

# ------------------------------
# Base image (shared settings)
# ------------------------------
FROM python:${PYTHON_VERSION} AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    KAFKA_TOPIC=trades

# Minimal tools (curl used by healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Non-root user and workdir
RUN useradd -ms /bin/bash appuser
WORKDIR /app

# ------------------------------
# Test stage (dev deps + tests)
# ------------------------------
FROM base AS test

# Install runtime + dev/test deps
COPY requirements.txt requirements-dev.txt /app/
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Copy all sources (so tests can import your app)
# If your tests live in tests/, ensure it’s included.
COPY . /app

# Permissions
RUN chown -R appuser:appuser /app
USER appuser

# Default command for this stage: run test suite
# CMD ["pytest", "-vv", "--cov=.", "--cov-report=term-missing"]
ENV COVERAGE_FILE=/coverage/.coverage.tradedata
CMD pytest -q \
  --maxfail=1 \
  --cov=TradeData \
  --cov-report=xml:/coverage/tradedata-coverage.xml \
  --cov-report=term-missing

# ------------------------------
# Runtime stage (lean, prod)
# ------------------------------
FROM base AS runtime

# Install only runtime deps
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy just what you need to run the app (keeps image small)
COPY main.py /app/main.py

# Permissions
RUN chown -R appuser:appuser /app
USER appuser

# Expose API port
EXPOSE 8001

# Simple healthcheck against /health
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8001/health || exit 1

# Run server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]