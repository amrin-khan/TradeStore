Test Design Document — TradeStore

1) Scope & Objectives
Goal: Prove that trades posted to TradeData are validated, published to Kafka, and correctly persisted by:
KafkaConsumer (Mongo) → writes to trade_store.trades
KafkaConsumerMSSQL → writes to dbo.trades (SQL Server)
Covers: unit, contract/schema, integration (with real services), E2E, non-functional (perf & security smoke), and CI gates.

2) System Under Test (SUT)
Services
TradeData (HTTP API, produces to Kafka topic trades)
kafkaconsumer (consumes trades, writes Mongo)
kafkaconsumermssql (consumes trades, writes MSSQL)
Infra: Redpanda (Kafka), MongoDB, MSSQL (with mssql-init schema)

3) Quality Criteria / Acceptance
Validation: API rejects malformed trades with 4xx + error body.
Publish: Valid trades are produced to trades within 250ms (avg in local).
Mongo: Document appears with all required fields; idempotency (resend same trade/version) does not duplicate.
MSSQL: Row exists with PK (trade_id, version, book_id); types correct; idempotency enforced by PK.
Resilience: If Mongo/MSSQL is briefly unavailable, consumers retry and eventually persist.
Security: No HIGH/CRITICAL image vulns (Trivy) and no vulnerable Python deps (pip-audit strict).
Performance: 100 trades processed E2E in < 10s in CI (smoke).
Definition of Done (DoD): All tests below green on PR; CI gates pass; coverage ≥ 80% on TradeData app logic.

4) Test Strategy & Types
4.1 Unit Tests (fast, isolated)
TradeData
Schema & validation functions (dates, version rules, “expired” logic)
Producer wrapper (mock Kafka producer called exactly once with correct payload)
Consumers
Deserialization and mapping from trade JSON → storage model
Idempotency logic / upsert queries
Tooling: pytest, pytest-cov, unittest.mock.
4.2 Contract/Schema Tests
Shared Pydantic model (or JSON Schema) that both producers & consumers import.
Tests fail if payload shape changes (backward compatibility guard).
4.3 Integration Tests (compose profile: test)
Spin up real Redpanda, Mongo, MSSQL, and test containers (your compose already has *-tests placeholders).
Scenarios:
Happy path: POST trade → appears in Mongo and MSSQL.
Duplicate: Re-POST same (trade_id, version, book_id) → no duplicate in Mongo, PK conflict avoided in MSSQL.
Bad payload: API returns 400; nothing reaches Kafka.
Consumer down: Stop consumer → POST → start consumer → messages drain and persist.
MSSQL unavailable: Simulate (stop service) then restart; row eventually exists.
4.4 End-to-End Smoke (regression profile)
Use regression-runner to submit N trades and verify counts/aggregations.
4.5 Non-functional
Performance (smoke): 100 trades E2E under threshold (record timings).
Security: pip-audit --strict, Trivy fail on HIGH,CRITICAL.
5) Test Data & Fixtures
Sample trade (valid)
{
  "trade_id": "T-1001",
  "counterparty_id": "CP-1",
  "book_id": "B-1",
  "version": 1,
  "maturity_date": "2026-01-15",
  "created_date": "2025-09-28",
  "expired": false
}
Pytest fixtures (sketch)
client → HTTP client for TradeData (FastAPI TestClient if applicable).
kafka_producer / kafka_consumer → aiokafka fixtures (or HTTP to TradeData only and assert side effects).
mongo_db → pymongo client to trade_store.
sql_conn → pyodbc or sqlcmd wrapper.

6) Directory & Commands
tests/
  unit/
    test_validation.py
    test_producer.py
    consumers/
      test_mapper_mongo.py
      test_mapper_mssql.py
  contract/
    test_schema.py
  integration/
    test_e2e_happy.py
    test_idempotency.py
    test_retries.py
  perf/
    test_throughput_smoke.py
Local run:
# Unit + contract (fast)
pytest -q tests/unit tests/contract --maxfail=1 --disable-warnings --cov=TradeData

# Full integration (requires compose up):
docker compose up -d redpanda mongo mssql tradedata kafkaconsumer kafkaconsumermssql
pytest -q tests/integration --maxfail=1 --disable-warnings

7) Example Tests (snippets)
7.1 TradeData validation (unit)
# tests/unit/test_validation.py
import pytest
from tradedata.models import Trade  # your Pydantic model

def test_trade_schema_valid():
    t = Trade(
        trade_id="T-1", counterparty_id="CP-1", book_id="B-1",
        version=1, maturity_date="2026-01-01", created_date="2025-01-01",
        expired=False
    )
    assert t.trade_id == "T-1"

def test_trade_schema_invalid_future_created_date():
    with pytest.raises(Exception):
        Trade(
          trade_id="T-1", counterparty_id="CP-1", book_id="B-1",
          version=1, maturity_date="2026-01-01", created_date="2999-01-01",
          expired=False
        )
7.2 Producer called with correct key/value (unit)
# tests/unit/test_producer.py
from unittest.mock import MagicMock
from tradedata.service import publish_trade

def test_publish_calls_producer_with_key_and_value():
    producer = MagicMock()
    trade = {"trade_id":"T-1","version":1,"book_id":"B-1"}
    publish_trade(producer, trade)
    key = f"{trade['trade_id']}:{trade['version']}:{trade['book_id']}".encode()
    producer.send.assert_called_once_with("trades", value=trade, key=key)
7.3 Contract/schema stays compatible
# tests/contract/test_schema.py
from shared.schema import TradePayload  # imported by all services

def test_required_fields_present():
    fields = set(TradePayload.model_fields.keys())
    assert {"trade_id","version","book_id","counterparty_id","expired"}.issubset(fields)
7.4 E2E happy path (integration)
# tests/integration/test_e2e_happy.py
import time, requests, pymongo
import pyodbc

API = "http://localhost:8001/trades"

def test_e2e_trade_persists_mongo_and_mssql():
    payload = {
      "trade_id": "T-2001", "counterparty_id":"CP-9", "book_id":"B-9",
      "version":1, "maturity_date":"2026-01-15", "created_date":"2025-09-28",
      "expired": False
    }
    r = requests.post(API, json=payload, timeout=5)
    assert r.status_code in (200, 201)

    # Allow small propagation delay
    time.sleep(2)

    # Mongo
    mongo = pymongo.MongoClient("mongodb://localhost:27017")
    doc = mongo["trade_store"]["trades"].find_one({"trade_id":"T-2001","version":1,"book_id":"B-9"})
    assert doc is not None

    # MSSQL
    conn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=master;UID=sa;PWD=YourStrong!Pass;TrustServerCertificate=Yes;")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dbo.trades WHERE trade_id=? AND [version]=? AND book_id=?", "T-2001", 1, "B-9")
    assert cur.fetchone()[0] == 1

8) CI Integration (what you already have + small add-ons)
Run unit/contract in a Python job (matrix optional).
Spin up compose and run integration tests (you already bring up infra and services).
Security gates: pip-audit --strict, Trivy HIGH,CRITICAL.
Artifacts: upload logs and pytest JUnit XML if you like.
Example CI steps to add (after compose up):
- name: Run integration tests
  run: |
    python -m pip install -r tests/requirements.txt || true
    pytest -q tests/integration --maxfail=1 --disable-warnings --junitxml=pytest-integration.xml

- name: Upload test results
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: pytest-integration
    path: pytest-integration.xml

9) Risks & Mitigations
Flaky tests from eventual consistency → use small waits/retries and health checks (you already have health checks).
Port conflicts locally → allow ports to be configurable via env.
Secrets leakage → never print MSSQL pwd; mask via GitHub Secrets (already done).

10) Open Items / Next Iteration
Contract tests using pact (or similar) to formalize producer↔consumer contract.
Performance baseline & trend (store simple metrics artifact).
Add migrations/DDL checks as tests (ensure schema exists).