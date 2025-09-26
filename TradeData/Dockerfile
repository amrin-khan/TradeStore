# TradeData/Dockerfile
FROM python:3.11-slim

# Basic env + faster pip
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

# Install deps
# - fastapi (includes pydantic)
# - uvicorn[standard] (uvloop, httptools, etc.)
# - aiokafka (pure Python; no librdkafka needed)
RUN pip install --upgrade pip \
 && pip install "fastapi>=0.110" "uvicorn[standard]>=0.29" "aiokafka>=0.10"

# Copy app code
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

