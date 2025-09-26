# input-feed/Dockerfile
FROM python:3.11-slim

# Basic env + pip behavior
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1


# Install curl for debugging/health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash appuser
WORKDIR /app

# --- Dependencies (use requirements.txt for caching/repro) ---
# If you prefer requests instead of httpx, swap it in requirements.txt
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# --- App code ---
COPY InputFeed.py /app/InputFeed.py
RUN chown -R appuser:appuser /app
USER appuser

# Default envs (override in compose/CLI)
ENV API_URL=http://tradedata:8001/trades \
    FEED_RATE=10 \
    BATCH_SIZE=1 \
    DURATION_SEC=60

# One-shot by default; container exits when feed.py finishes
CMD ["python", "/app/InputFeed.py"]