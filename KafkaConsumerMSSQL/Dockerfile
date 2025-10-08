# kafkaconsumermssql/Dockerfile
# syntax=docker/dockerfile:1.7

########################
# 1) Base (define this FIRST)
########################
FROM python:3.11-slim AS base
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
    
# --- System deps + MS ODBC 18 + tools (Debian 12/bookworm) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
   curl gnupg2 apt-transport-https ca-certificates \
   unixodbc-dev libgssapi-krb5-2 procps \
  && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 mssql-tools18 \
  && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# --- Non-root user & workdir ---
RUN useradd -ms /bin/bash appuser
WORKDIR /app


#####################################
# Test target (runs pytest)         #
#####################################
FROM base AS test

# Python deps (runtime + test tools)
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    pip install --no-cache-dir pytest pytest-asyncio pytest-cov

# App + tests
COPY IngestSQL.py /app/IngestSQL.py
# Make sure this file exists at build context root; rename if needed:
COPY test_ingestsql.py /app/test_ingestsql.py

ENV COVERAGE_FILE=/coverage/.coverage.kafkaconsumermssql
CMD pytest -q \
  --maxfail=1 \
  --cov=kafkaconsumermssql \
  --cov-report=xml:/coverage/kafkaconsumermssql-coverage.xml \
  --cov-report=term-missing
# Run test suite with coverage
# CMD ["pytest", "-vv", "--cov=/app", "--cov-report=term-missing", "/app/test_ingestsql.py"]

########################
# 2) Runtime (your original)
########################
FROM base AS runtime
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# --- App code ---
COPY IngestSQL.py /app/IngestSQL.py

RUN chown -R appuser:appuser /app
USER appuser

# Optional defaults (override in compose/env)
ENV KAFKA_BOOTSTRAP=redpanda:9092 \
    KAFKA_TOPIC=trades \
    KAFKA_GROUP=trade-consumer-B

# Simple liveness check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ps -o comm= 1 | grep -q python || exit 1

CMD ["python", "/app/IngestSQL.py"]
