import asyncio
import inspect
import httpx
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from fastapi import status

import main 
from main import app, trade_store, key_for, Trade


@pytest.fixture(autouse=True)
def _clear_store_each_test():
    trade_store.clear()     # reset global in-memory store
    yield
    trade_store.clear()     # (optional) also clean after, for safety
    
# 1) Patch AIOKafkaProducer class globally so lifespan startup (if it runs) never dials Kafka
@pytest.fixture(scope="session", autouse=True)
def patch_kafka_producer_session():
    with patch("main.AIOKafkaProducer") as FakeProducer:
        inst = AsyncMock()
        inst.start.return_value = None
        inst.stop.return_value = None
        inst.send_and_wait.return_value = type(
            "MD", (), {"topic": "trades", "partition": 0, "offset": 0}
        )
        FakeProducer.return_value = inst
        yield

# 2) Force producer=None before each test so the “producer not ready” case is deterministic
@pytest.fixture(autouse=True)
def force_producer_none():
    main.producer = None
    yield
    main.producer = None

# 3) httpx client that works across versions (0.27 and 0.28+) and avoids lifespan if supported
@pytest_asyncio.fixture
async def client():
    transport_kwargs = {}
    if "lifespan" in inspect.signature(httpx.ASGITransport).parameters:
        transport_kwargs["lifespan"] = "off"  # don’t run startup/shutdown

    transport = httpx.ASGITransport(app=app, **transport_kwargs)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
# ---------- Basic health ----------

@pytest.mark.asyncio
async def test_health_endpoint(client):
    r = await client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["producer"] in (True, False)


# ---------- Positive cases ----------

@pytest.mark.asyncio
async def test_post_trade_success_happy_path(client):
    trade_data = {
        "trade_id": "T1",
        "version": "1",
        "counterparty_id": "CP-1",
        "book_id": "B1",
        "maturity_date": "2025-12-31",
        "created_date": "2025-09-27",
        "expired": "N",
    }
    mock_producer = AsyncMock()
    mock_producer.send_and_wait.return_value = type(
        "MD", (), {"topic": "trades", "partition": 0, "offset": 1}
    )
    with patch("main.producer", mock_producer):
        resp = await client.post("/trades", json=trade_data)

    assert resp.status_code == status.HTTP_202_ACCEPTED
    out = resp.json()
    assert out["status"] == "received_and_published"
    assert out["kafka"]["topic"] == "trades"
    assert len(trade_store) == 1
    assert trade_store[0]["expired"] is False  # normalized


@pytest.mark.asyncio
@pytest.mark.parametrize("expired_in, expected_bool", [
    ("Y", True), ("N", False), ("y", True), ("n", False),
    ("Yes", True), ("No", False), ("TRUE", True), ("false", False), ("1", True), ("0", False),
])
async def test_post_trade_expired_variants(client, expired_in, expected_bool):
    payload = {
        "trade_id": "T2",
        "version": "7",
        "counterparty_id": "CPTY",
        "book_id": "BOOK",
        "maturity_date": "2026-01-01",
        "created_date": "2025-09-27",
        "expired": expired_in,
    }
    # Note: your endpoint only accepts "Y"/"N" and throws 400 otherwise.
    # To demonstrate *intended* normalization, we keep variants limited to Y/N case-insensitive.
    # For non-Y/N inputs below we assert 400 (negative case).
    if expired_in.upper() in {"Y", "N"}:
        mp = AsyncMock()
        mp.send_and_wait.return_value = type("MD", (), {"topic": "trades", "partition": 0, "offset": 2})
        with patch("main.producer", mp):
            r = await client.post("/trades", json=payload)
        assert r.status_code == 202
        assert trade_store[-1]["expired"] is (expired_in.upper() == "Y")
    else:
        # Current implementation *rejects* anything not in {"Y","N"}
        with patch("main.producer", AsyncMock()):
            r = await client.post("/trades", json=payload)
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_get_trades_after_multiple_posts(client):
    mp = AsyncMock()
    mp.send_and_wait.side_effect = [
        type("MD", (), {"topic": "trades", "partition": 1, "offset": 11}),
        type("MD", (), {"topic": "trades", "partition": 1, "offset": 12}),
    ]
    with patch("main.producer", mp):
        for i in range(2):
            r = await client.post("/trades", json={
                "trade_id": f"T{i}",
                "version": str(i),
                "counterparty_id": "CPX",
                "book_id": "B9",
                "maturity_date": "2027-01-01",
                "created_date": "2025-09-27",
                "expired": "N",
            })
            assert r.status_code == 202

    r = await client.get("/trades")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 2
    assert {t["trade_id"] for t in body} == {"T0", "T1"}


# ---------- Negative cases ----------

@pytest.mark.asyncio
async def test_post_trade_invalid_expired_400(client):
    bad = {
        "trade_id": "T_BAD",
        "version": "1",
        "counterparty_id": "CP-2",
        "book_id": "B2",
        "maturity_date": "2025-12-31",
        "created_date": "2025-09-27",
        "expired": "MAYBE",
    }
    with patch("main.producer", AsyncMock()):
        r = await client.post("/trades", json=bad)
    assert r.status_code == 400
    assert "expired must be" in r.text


@pytest.mark.asyncio
async def test_post_trade_producer_not_ready_503(client):
    ok = {
        "trade_id": "T3",
        "version": "3",
        "counterparty_id": "CP3",
        "book_id": "B3",
        "maturity_date": "2025-12-31",
        "created_date": "2025-09-27",
        "expired": "N",
    }
    # No patch -> producer remains None in this test context
    r = await client.post("/trades", json=ok)
    assert r.status_code == 503
    assert "producer not ready" in r.text.lower()


@pytest.mark.asyncio
async def test_post_trade_publish_failure_503(client):
    ok = {
        "trade_id": "T4",
        "version": "4",
        "counterparty_id": "CP4",
        "book_id": "B4",
        "maturity_date": "2025-12-31",
        "created_date": "2025-09-27",
        "expired": "N",
    }
    mp = AsyncMock()
    mp.send_and_wait.side_effect = RuntimeError("boom")
    with patch("main.producer", mp):
        r = await client.post("/trades", json=ok)
    assert r.status_code == 503
    assert "Kafka publish failed" in r.text


@pytest.mark.asyncio
async def test_validation_missing_fields_422(client):
    # Missing required fields -> Pydantic should 422
    r = await client.post("/trades", json={"trade_id": "T5"})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_validation_type_errors_422(client):
    # version should be str in your model; send an object
    bad = {
        "trade_id": "T6",
        "version": {"bad": "type"},
        "counterparty_id": "CP6",
        "book_id": "B6",
        "maturity_date": "2025-12-31",
        "created_date": "2025-09-27",
        "expired": "N",
    }
    r = await client.post("/trades", json=bad)
    assert r.status_code == 422


# ---------- Edge-y inputs ----------

@pytest.mark.asyncio
async def test_unicode_and_large_strings(client):
    long_str = "𝕋" * 2000
    ok = {
        "trade_id": long_str,
        "version": "1",
        "counterparty_id": "काउंटरपार्टी-✅",
        "book_id": "📚-B1",
        "maturity_date": "2028-01-01",
        "created_date": "2025-09-27",
        "expired": "Y",
    }
    mp = AsyncMock()
    mp.send_and_wait.return_value = type("MD", (), {"topic": "trades", "partition": 2, "offset": 42})
    with patch("main.producer", mp):
        r = await client.post("/trades", json=ok)
    assert r.status_code == 202
    assert trade_store[-1]["expired"] is True
    # Ensure it didn’t explode on key serialization
    key = key_for(Trade(**ok))
    assert isinstance(key, (bytes, bytearray))
    assert key.startswith(long_str.encode("utf-8")[:10])  # sanity


@pytest.mark.asyncio
async def test_key_format_function():
    t = Trade(
        trade_id="T9", version="1", counterparty_id="CPX", book_id="B9",
        maturity_date="2099-12-31", created_date="2025-09-27", expired="N"
    )
    k = key_for(t).decode()
    assert k == "T9|CPX|B9"


@pytest.mark.asyncio
async def test_basic_concurrency_posts(client):
    mp = AsyncMock()
    mp.send_and_wait.return_value = type("MD", (), {"topic": "trades", "partition": 0, "offset": 99})
    with patch("main.producer", mp):
        async def post_one(i):
            return await client.post("/trades", json={
                "trade_id": f"T{i}",
                "version": str(i),
                "counterparty_id": "CP",
                "book_id": "B",
                "maturity_date": "2030-01-01",
                "created_date": "2025-09-27",
                "expired": "N",
            })
        rs = await asyncio.gather(*[post_one(i) for i in range(5)])
    assert all(r.status_code == 202 for r in rs)
    assert len(trade_store) == 5
