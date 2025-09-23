# main.py -  uvicorn main:app --reload --port 8001
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date
from aiokafka import AIOKafkaProducer
# import asyncio  
import json, os
from contextlib import asynccontextmanager

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades")

# Define the schema for trade data
class Trade(BaseModel):
    trade_id: str
    version: str
    counterparty_id: str
    book_id: str
    maturity_date: str   # could also parse into date
    created_date: str
    expired: str

producer: AIOKafkaProducer | None = None

def key_for(t: Trade) -> bytes:
    return f"{t.trade_id}|{t.counterparty_id}|{t.book_id}".encode("utf-8")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",                     # strongest durability
        linger_ms=10,                   # small batching
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k,
        # aiokafka supports transactional/idempotent config via kwargs in newer Kafka libs; optional
    )
    await producer.start()
    try:
        print("Started producer")
        yield
    finally:
        print("Stopping producer")
        await producer.stop()

app = FastAPI(lifespan=lifespan)

# In-memory store (demo)
trade_store: list[dict] = []

@app.post("/trades", status_code=202)
async def receive_and_publish(trade: Trade):
    """
    Accept a trade JSON, store it (demo), publish to Kafka, and return a response.
    """
    # 1) quick validation on expired
    if trade.expired.upper() not in {"Y", "N"}:
        raise HTTPException(400, "expired must be 'Y' or 'N'")

    # 2) transform payload for storage/publish (normalize expired to bool)
    payload = trade.model_dump(by_alias=True)  # keeps 'counter_party_id' in the dict
    payload["expired"] = True if trade.expired.upper() == "Y" else False

    # (optional) parse/normalize dates here if your downstream expects ISO-8601
    # for k in ("maturity_date", "created_date"):
    #     payload[k] = datetime.strptime(payload[k], "%d/%m/%Y").date().isoformat()

    # 3) "store" locally (demo)
    trade_store.append(payload)
    print("amrin")

    # 4) publish to Kafka
    if producer is None:
        # You can choose to still accept with a different status if Kafka is down
        print("Kafka producer not ready")
        raise HTTPException(503, "Kafka producer not ready")
    try:
          # STRICT header typing: (str, bytes)
        headers = [
            ("content-type", b"application/json"),
            ("schema", b"trade_event_v1"),
        ]
        # quick sanity asserts (remove later)
        assert all(isinstance(k, str) and (v is None or isinstance(v, (bytes, bytearray))) for k, v in headers)
        metadata = await producer.send_and_wait(
            topic=TOPIC,
            value=payload,
            key=key_for(trade),
            headers=headers,
        )
        print(f"published to Kafka topic {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
    except Exception as e:
        print("publish failed")
        # Decide policy: return 503 or 207 (multi-status) or accept but flag failure
        raise HTTPException(503, f"Kafka publish failed: {e}")

    # 5) return normal HTTP response with Kafka metadata
    return {
        "status": "received_and_published",
        "stored_count": len(trade_store),
        "kafka": {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset},
    }

# @app.post("/publish", status_code=202)
# async def publish(event: Trade):
#     if producer is None:
#         raise HTTPException(503, "Producer not ready")
#     # normalize expired to bool
#     if isinstance(event.expired, str):
#         event.expired = event.expired.strip().upper() in {"Y", "YES", "TRUE", "1"}
#     md = await producer.send_and_wait(
#         TOPIC,
#         value=event.model_dump(by_alias=True),
#         key=key_for(event),
#         headers=[(b"content-type", b"application/json"), (b"schema", b"trade_event_v1")],
#     )
#     return {"topic": md.topic, "partition": md.partition, "offset": md.offset}

@app.get("/trades")
async def get_trades():
    """
    Retrieve all stored trades (for testing).
    """
    return trade_store

@app.get("/health")
async def health():
    return {"status": "ok", "producer": producer is not None}
