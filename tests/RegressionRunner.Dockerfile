# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1
WORKDIR /app

# OS deps for pyodbc
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl gnupg2 ca-certificates unixodbc-dev libgssapi-krb5-2 \
 && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

# Python deps needed by the tests only
RUN python -m pip install -U pip && \
    pip install --no-cache-dir \
      pytest pytest-asyncio pytest-cov pytest-rerunfailures \
      httpx pymongo pyodbc

# Only the tests are needed in this image
COPY tests /app/tests
COPY pytest.ini /app/pytest.ini

# Default command runs the regression marker only
CMD ["pytest", "-m", "regression", "-vv", "--maxfail=1", "--reruns", "1", "--reruns-delay", "1"]
