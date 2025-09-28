import os
import time
import uuid
import pytest
import httpx


try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None

try:
    import pyodbc  # requires msodbcsql18 in container
except Exception:  # pragma: no cover
    pyodbc = None


# ----- markers -----
def pytest_configure(config):
    config.addinivalue_line("markers", "regression: end-to-end regression tests")
    config.addinivalue_line("markers", "integration: real services")


# ----- env config with sensible defaults for docker-compose -----
TRADEDATA_URL = os.getenv("TRADEDATA_URL", "http://tradedata:8001")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "trade_store")
MONGO_COLL = os.getenv("MONGO_COLL", "trades")
MSSQL_CONN_STRING = os.getenv("MSSQL_CONN_STRING")  # e.g. "Driver={ODBC Driver 18 for SQL Server};Server=mssql,1433;Database=master;UID=sa;PWD=Str0ng!Passw0rd;TrustServerCertificate=Yes;"


@pytest.fixture(scope="session")
def base_url() -> str:
    return TRADEDATA_URL.rstrip("/")


@pytest.fixture(scope="session")
def http():
    with httpx.Client(timeout=10.0) as s:
        yield s


@pytest.fixture(scope="session")
def mongo():
    if MongoClient is None:
        pytest.skip("pymongo not installed in this test image")
    cli = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    # trigger connection early
    cli.admin.command("ping")
    try:
        yield cli[MONGO_DB][MONGO_COLL]
    finally:
        cli.close()


@pytest.fixture(scope="session")
def mssql():
    pwd = os.environ["MSSQL_SA_PASSWORD"]
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=mssql,1433;"
        "Database=master;"
        f"UID=sa;PWD={pwd};"
        "TrustServerCertificate=Yes;"
    )
    cn = pyodbc.connect(conn_str, autocommit=True)
    yield cn
    cn.close()


@pytest.fixture
def new_trade_id():
    # unique per test so we don't collide with concurrent/incremental runs
    return f"R-{uuid.uuid4().hex[:10]}"


# ----- small poll helper -----
@pytest.fixture
def wait_until():
    """
    Polls fn() until it returns truthy or timeout.
    Accepts optional desc just for readability in call sites.
    """
    def _wait(fn, timeout: float = 8.0, interval: float = 0.25, desc: str | None = None, **_):
        end = time.time() + timeout
        last = None
        while time.time() < end:
            last = fn()
            if last:
                return last
            time.sleep(interval)
        return last
    return _wait