import asyncio
import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
import pytest_asyncio

import IngestSQL as mod  # the module under test


# -----------------------------
# Unit tests: _parse_date
# -----------------------------
def test_parse_date_variants():
    assert mod._parse_date("2024-09-27") == dt.date(2024, 9, 27)  # ISO
    assert mod._parse_date("2024-09-27T10:20:30") == dt.date(2024, 9, 27)
    assert mod._parse_date("27/09/2024") == dt.date(2024, 9, 27)  # DMY
    assert mod._parse_date("09/27/2024") == dt.date(2024, 9, 27)  # MDY
    # unix seconds
    ts = dt.datetime(2024, 9, 27, 12, 0, 0).timestamp()
    assert mod._parse_date(ts) == dt.date(2024, 9, 27)
    # datetime object
    assert mod._parse_date(dt.datetime(2024, 9, 27, 8)) == dt.date(2024, 9, 27)
    # date object
    assert mod._parse_date(dt.date(2024, 9, 27)) == dt.date(2024, 9, 27)


def test_parse_date_null_and_invalid(caplog):
    assert mod._parse_date(None) is None
    assert mod._parse_date("") is None
    assert mod._parse_date("null") is None
    assert mod._parse_date("None") is None
    caplog.clear()
    assert mod._parse_date("totally-not-a-date") is None


# -----------------------------
# Unit tests: json_deserializer
# -----------------------------
def test_json_deserializer_variants():
    assert mod.json_deserializer(None) is None
    assert mod.json_deserializer(b"") is None
    assert mod.json_deserializer("   ") is None
    assert mod.json_deserializer(b'{"a":1}') == {"a": 1}
    assert mod.json_deserializer('{"a":2}') == {"a": 2}

    bad = mod.json_deserializer(b"not-json")
    assert bad["__parse_error__"] == "JSONDecodeError"
    assert "__raw__" in bad


# -----------------------------
# Helpers: fake pyodbc and cursor
# -----------------------------
class FakeCursor:
    def __init__(self):
        self._rowcount_for_update = 1
        self.executed = []

    def execute(self, sql, params=()):
        self.executed.append((sql.strip(), tuple(params)))
        # For UPDATE, return an object with rowcount attribute
        if sql.strip().upper().startswith("UPDATE"):
            obj = SimpleNamespace(rowcount=self._rowcount_for_update)
            return obj
        return self  # for INSERT/SELECT chaining

    def fetchone(self):
        # default: no existing row
        return (None,)

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


# -----------------------------
# Async tests: insert_record
# -----------------------------
@pytest.mark.asyncio
async def test_insert_record_inserts_when_no_existing(monkeypatch):
    cur = FakeCursor()
    conn = FakeConnection(cur)

    def fake_connect(_):
        return conn

    monkeypatch.setattr(mod.pyodbc, "connect", fake_connect, raising=True)

    record = {
        "trade_id": "T1",
        "version": "1",
        "counterparty_id": "CP",
        "book_id": "B1",
        "maturity_date": "2025-12-31",
        "created_date": "2025-01-01",
        "expired": False,
    }

    await mod.insert_record(record)

    # Should have SELECT then INSERT; commit once
    sqls = [s for (s, _) in cur.executed]
    assert any(s.upper().startswith("SELECT") for s in sqls)
    assert any(s.upper().startswith("INSERT INTO TRADES") for s in sqls)
    assert conn.committed is True
    assert conn.closed is True


@pytest.mark.asyncio
async def test_insert_record_rejects_lower_version(monkeypatch):
    class Cur(FakeCursor):
        def fetchone(self):
            return (5,)  # existing_version=5

    cur = Cur()
    conn = FakeConnection(cur)

    def fake_connect(_):
        return conn

    monkeypatch.setattr(mod.pyodbc, "connect", fake_connect, raising=True)

    record = {
        "trade_id": "T1",
        "version": "3",  # lower than 5
        "counterparty_id": "CP",
        "book_id": "B1",
        "maturity_date": "2026-01-01",
        "created_date": "2025-01-01",
        "expired": True,
    }

    await mod.insert_record(record)

    # No INSERT/UPDATE; and commit should not be called
    sqls = [s for (s, _) in cur.executed]
    assert any(s.upper().startswith("SELECT") for s in sqls)
    assert not any(s.upper().startswith("INSERT INTO TRADES") for s in sqls)
    assert not any(s.upper().startswith("UPDATE TRADES") for s in sqls)
    assert conn.committed is False


@pytest.mark.asyncio
async def test_insert_record_updates_when_same_version(monkeypatch):
    class Cur(FakeCursor):
        def __init__(self):
            super().__init__()
            self._rowcount_for_update = 1  # UPDATE succeeds
        def fetchone(self):
            return (7,)  # existing=7

    cur = Cur()
    conn = FakeConnection(cur)
    monkeypatch.setattr(mod.pyodbc, "connect", lambda _: conn, raising=True)

    record = {
        "trade_id": "T1",
        "version": "7",  # same as existing
        "counterparty_id": "CP",
        "book_id": "B1",
        "maturity_date": "2026-01-01",
        "created_date": "2025-01-01",
        "expired": False,
    }

    await mod.insert_record(record)
    sqls = [s for (s, _) in cur.executed]
    assert any(s.upper().startswith("SELECT") for s in sqls)
    assert any(s.upper().startswith("UPDATE TRADES") for s in sqls)
    assert not any(s.upper().startswith("INSERT INTO TRADES") for s in sqls)
    assert conn.committed is True


@pytest.mark.asyncio
async def test_insert_record_update_then_insert_when_rowcount_zero(monkeypatch):
    class Cur(FakeCursor):
        def __init__(self):
            super().__init__()
            self._rowcount_for_update = 0  # UPDATE matches 0 rows → fallback insert
        def fetchone(self):
            return (7,)

    cur = Cur()
    conn = FakeConnection(cur)
    monkeypatch.setattr(mod.pyodbc, "connect", lambda _: conn, raising=True)

    record = {
        "trade_id": "T2",
        "version": "7",
        "counterparty_id": "CP",
        "book_id": "B9",
        "maturity_date": "2027-09-27",
        "created_date": "2025-09-27",
        "expired": True,
    }

    await mod.insert_record(record)
    sqls = [s for (s, _) in cur.executed]
    assert any(s.upper().startswith("UPDATE TRADES") for s in sqls)
    assert any(s.upper().startswith("INSERT INTO TRADES") for s in sqls)
    assert conn.committed is True


@pytest.mark.asyncio
async def test_insert_record_invalid_incoming_version_no_commit(monkeypatch):
    cur = FakeCursor()
    conn = FakeConnection(cur)
    monkeypatch.setattr(mod.pyodbc, "connect", lambda _: conn, raising=True)

    record = {
        "trade_id": "T3",
        "version": "NaN",
        "counterparty_id": "CP",
        "book_id": "B",
        "maturity_date": "2028-01-01",
        "created_date": "2025-09-27",
        "expired": False,
    }

    await mod.insert_record(record)

    # SELECT happens, but then it returns; no commit/insert/update
    sqls = [s for (s, _) in cur.executed]
    assert any(s.upper().startswith("SELECT") for s in sqls)
    assert not any(s.upper().startswith("INSERT") for s in sqls)
    assert not any(s.upper().startswith("UPDATE") for s in sqls)
    assert conn.committed is False


# -----------------------------
# Async tests: consume()
# -----------------------------
class FakeMsg:
    def __init__(self, topic, partition, offset, key, value):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value


class FakeConsumer:
    """
    Minimal async-iterable consumer with start/stop/commit.
    It doesn't apply value_deserializer; we feed msg.value directly in tests.
    """
    def __init__(self, msgs):
        self.msgs = msgs
        self.started = False
        self.stopped = False
        self.commit = AsyncMock()

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    def __aiter__(self):
        async def gen():
            for m in self.msgs:
                yield m
        return gen()


@pytest_asyncio.fixture
async def good_record():
    return {
        "trade_id": "GX",
        "version": "1",
        "counterparty_id": "CP",
        "book_id": "B",
        "maturity_date": "2030-01-01",
        "created_date": "2025-01-01",
        "expired": False,
    }


@pytest.mark.asyncio
async def test_consume_happy_and_skips_and_db_fail(monkeypatch, good_record):
    # Messages to simulate:
    # 1) tombstone (None) -> commit
    # 2) parse error dict -> commit
    # 3) good record -> insert_record called + commit
    # 4) good record but insert throws -> NO commit
    tomb = FakeMsg("trades", 0, 1, b"k1", None)
    parse_err = FakeMsg("trades", 0, 2, b"k2", {"__parse_error__": "JSONDecodeError", "__raw__": "not-json"})
    good_ok = FakeMsg("trades", 0, 3, b"k3", dict(good_record))
    good_bad = FakeMsg("trades", 0, 4, b"k4", dict(good_record))

    fake = FakeConsumer([tomb, parse_err, good_ok, good_bad])

    # Patch AIOKafkaConsumer to return our fake instance
    with patch.object(mod, "AIOKafkaConsumer", lambda *a, **k: fake):
        # Patch insert_record: first succeed, then raise
        ok = AsyncMock(return_value=None)
        bad = AsyncMock(side_effect=RuntimeError("db down"))
        seq = [ok, bad]

        async def insert_seq(val):
            fn = seq.pop(0)
            return await fn(val)

        monkeypatch.setattr(mod, "insert_record", insert_seq, raising=True)

        await mod.consume()

    # Assertions:
    assert fake.started is True
    assert fake.stopped is True

    # First two commits for skipped messages, one commit for good_ok (total 3)
    # The bad one should NOT commit
    assert fake.commit.await_count == 3
    # Ensure commit ordering roughly matches message order (by offset)
    # (We check the count only; commit args are empty in our fake.)
