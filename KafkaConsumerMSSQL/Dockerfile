# consumer-mssql/Dockerfile

FROM python:3.11-slim
ENV DEBIAN_FRONTEND=noninteractive

ENV PYTHONDONTWRITEBYTECODE=1 \
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

# --- Python deps (pin via requirements.txt for caching) ---
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
