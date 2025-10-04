# kafkaconsumer.py
import os
import asyncio
import json
import signal
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import InsertOne
from contextlib import suppress
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError

# ------------ Config ------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "trades")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP", "trade-consumer-A")

MONGO_URI  = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB   = os.getenv("MONGO_DB", "trade_store")
MONGO_COLL = os.getenv("MONGO_COLL", "trades")

POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))
MAX_RECORDS     = int(os.getenv("MAX_RECORDS", "1000"))
BULK_CHUNK      = int(os.getenv("BULK_CHUNK", "500"))
GRACE_TIMEOUT_S = float(os.getenv("GRACE_TIMEOUT_S", "15.0"))
# -------------------------------

shutdown = asyncio.Event()

# ---------- helpers ----------
def safe_parse_json(raw: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        s = raw.decode("utf-8", errors="replace").strip()
        if not s or (s[0] not in "{["):
            return None
        return json.loads(s)
    except Exception:
        return None

def normalize(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    if "counter_party_id" in out and "counterparty_id" not in out:
        out["counterparty_id"] = out.pop("counter_party_id")
    if "expired" in out and isinstance(out["expired"], str):
        out["expired"] = out["expired"].strip().upper() in {"Y","YES","TRUE","1"}
    return out

# ---------- main consumer ----------
async def consumer_task():
    mcli = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=3000, uuidRepresentation="standard", maxPoolSize=50, connectTimeoutMS=3000)
    try:
        # Time-bound the ping so it can be interrupted and tests can simulate failures
        await asyncio.wait_for(mcli.admin.command("ping"), timeout=3.0)
    except (asyncio.TimeoutError, ServerSelectionTimeoutError) as e:
        print(f"[fatal] Mongo unreachable at {MONGO_URI}: {e}")
        mcli.close()
        return
    except Exception as e:
        print(f"[fatal] Mongo init failed: {type(e).__name__}: {e}")
        mcli.close()
        return
    coll = mcli[MONGO_DB][MONGO_COLL]
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,       # manual commit after DB write
        session_timeout_ms=60000,
        heartbeat_interval_ms=20000,
        request_timeout_ms=70000,
        max_poll_interval_ms=300000,
    )
    await consumer.start()
    try:
        while not shutdown.is_set():
            batch_map = await consumer.getmany(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_RECORDS)
            if not batch_map:
                await asyncio.sleep(0)  # let heartbeats run
                continue

            ops: List[InsertOne] = []
            polled = 0

            for tp, recs in batch_map.items():
                polled += len(recs)
                for msg in recs:
                    if not msg.value:
                        continue
                    doc = safe_parse_json(msg.value)
                    if not doc:
                        print(f"[skip] non-JSON {tp.topic}[{tp.partition}]@{msg.offset}")
                        continue
                    doc = normalize(doc)

                    # UUID for _id
                    doc["_id"] = f"{tp.topic}:{tp.partition}:{msg.offset}"

                    # Add ingestion datetime
                    doc["ingest_time"] = datetime.utcnow()
                    print(f"[msg] {tp.topic}[{tp.partition}]@{msg.offset} id={doc['_id']} {doc['ingest_time']}")
                    ops.append(InsertOne(doc))

            if not ops:
                await consumer.commit()
                continue

            try:
                for i in range(0, len(ops), BULK_CHUNK):
                    batch = ops[i:i+BULK_CHUNK]
                    # time-bound the DB write
                    await asyncio.wait_for(coll.bulk_write(batch, ordered=False), timeout=5.0)
                    await asyncio.sleep(0)
                # time-bound the commit too
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(consumer.commit(), timeout=3.0)
                print(f"[ok] polled={polled} inserted={len(ops)} committed (or timed out)")
            except BulkWriteError as bwe:
                print(f"[mongo] BulkWriteError: {bwe.details}")
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(consumer.commit(), timeout=3.0)
            except Exception as e:
                print(f"[err] mongo write failed: {type(e).__name__}: {e}")
                await asyncio.sleep(0.2)
    finally:
        try:
            await asyncio.wait_for(consumer.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            print("[warn] consumer.stop timeout")
        mcli.close()
        print("[done] consumer stopped; mongo closed")

# ---------- signals / entry ----------
async def main():
    loop = asyncio.get_running_loop()
    def _trigger(sig: signal.Signals):
        print(f"[signal] {sig.name}")
        shutdown.set()

    try:
        loop.add_signal_handler(signal.SIGINT,  _trigger, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _trigger, signal.SIGTERM)
    except NotImplementedError:
        signal.signal(signal.SIGINT,  lambda *_: _trigger(signal.SIGINT))
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, lambda *_: _trigger(signal.SIGTERM))

    task = asyncio.create_task(consumer_task(), name="consumer")
    await shutdown.wait()

    try:
        await asyncio.wait_for(task, timeout=GRACE_TIMEOUT_S)
    except asyncio.TimeoutError:
        print(f"[warn] grace {GRACE_TIMEOUT_S}s exceeded, cancelling")
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

if __name__ == "__main__":  # pragma: no cover
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover
        pass
