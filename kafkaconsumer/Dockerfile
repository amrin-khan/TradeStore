# consumer-mongo/Dockerfile

FROM python:3.11-slim
ENV DEBIAN_FRONTEND=noninteractive

# Basic env + pip behavior
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Tiny utilities:
# - procps: provides `ps` for the healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

# Non-root user & workdir
RUN useradd -ms /bin/bash appuser
WORKDIR /app

# --- Dependencies (use a requirements.txt for caching/repro) ---
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
 && pip install -r /app/requirements.txt

# --- App code ---
COPY kafkaconsumer.py /app/kafkaconsumer.py
RUN chown -R appuser:appuser /app
USER appuser

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

# Simple liveness check: ensure the Python process (PID 1) is running
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ps -o comm= 1 | grep -q python || exit 1

CMD ["python", "/app/kafkaconsumer.py"]
