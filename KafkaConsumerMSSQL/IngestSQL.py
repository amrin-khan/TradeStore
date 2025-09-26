import pyodbc
from aiokafka import AIOKafkaConsumer
import os
import datetime as dt
import asyncio, json, logging
from aiokafka.structs import TopicPartition

from typing import Any, Optional                       
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "trades")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP", "trade-consumer-B")

MSSQL_CONN_STRING = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=mssql,1433;"
    "Database=master;"
    "UID=sa;"
    "PWD=Str0ng!Passw0rd;"
    "TrustServerCertificate=Yes;"
)

_DATE_FORMATS = [
    "%Y-%m-%d",                # 2021-05-20  (ISO date)
    "%Y-%m-%dT%H:%M:%S",       # 2021-05-20T14:30:00
    "%Y-%m-%dT%H:%M:%S.%f",    # 2021-05-20T14:30:00.123
    "%d/%m/%Y",                # 20/05/2021 (DMY — common cause of 22007)
    "%m/%d/%Y",                # 05/20/2021
]


async def insert_record(record):
    maturity_date = _parse_date(record.get("maturity_date"))
    today = dt.date.today()
    # if maturity_date is not None and maturity_date < today:
    #     log.warning("Rejected trade %r: maturity date %r is earlier than today (%r)", record.get("trade_id"), maturity_date, today)
    #     print(f"Rejected trade {record.get('trade_id')}: maturity date {maturity_date} is earlier than today ({today})")
    #     return  # Reject the trade
    def _sync_insert():
        conn = pyodbc.connect(MSSQL_CONN_STRING)
        cursor = conn.cursor()


        cursor.execute("SELECT max(version) as version FROM trades WHERE trade_id=? and book_id=?", (record.get("trade_id"),record.get("book_id")))
        row = cursor.fetchone()
        try:
            incoming_version = int(record.get("version"))
        except Exception:
            log.warning("Invalid incoming version: %r", record.get("version"))
            cursor.close()
            conn.close()
            return
        
        existing_version = None
        if row and row[0] is not None:
            try:
                existing_version = int(row[0])
            except Exception:
                log.warning("Invalid existing version in DB: %r", row[0])
                existing_version = None

        
        if existing_version is not None:
            if incoming_version < existing_version:
                log.warning("Rejected trade %r: incoming version %r is lower than existing version %r", record.get("trade_id"), incoming_version, existing_version)
                print(f"Rejected trade {record.get('trade_id')}: incoming version {incoming_version} is lower than existing version {existing_version}")
                cursor.close()
                conn.close()
                return  # Reject lower version
            # If same version, update; if higher, update (or insert if needed)
            updated = cursor.execute("""
                UPDATE trades
                SET counterparty_id=?, book_id=?, maturity_date=?, created_date=?, expired=?, version=?
                WHERE trade_id=? AND version=?
            """, (
                record.get("counterparty_id"),
                record.get("book_id"),
                maturity_date,
                _parse_date(record.get("created_date")),
                record.get("expired"),
                incoming_version,
                record.get("trade_id"),
                existing_version,
            )).rowcount
            if updated == 0:
                print("Zero updated, inserting new")
                # If no rows updated (shouldn't happen), insert new
                cursor.execute("""
                    INSERT INTO trades (trade_id, version, counterparty_id, book_id, maturity_date, created_date, expired)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get("trade_id"),
                    incoming_version,
                    record.get("counterparty_id"),
                    record.get("book_id"),
                    maturity_date,
                    _parse_date(record.get("created_date")),
                    record.get("expired"),
                ))
        else:
            print("No existing trade, inserting new")
            # No existing trade, insert new
            cursor.execute("""
                INSERT INTO trades (trade_id, version, counterparty_id, book_id, maturity_date, created_date, expired)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get("trade_id"),
                incoming_version,
                record.get("counterparty_id"),
                record.get("book_id"),
                maturity_date,
                _parse_date(record.get("created_date")),
                record.get("expired"),
            ))
    
        conn.commit()
        cursor.close()
        conn.close()

    # Run blocking DB insert in a worker thread
    await asyncio.to_thread(_sync_insert)

def _parse_date(value: Any) -> Optional[dt.date]:
    if value in (None, "", "null", "None"):
        return None
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, (int, float)):
        # interpret as unix seconds if it looks plausible
        try:
            return dt.datetime.utcfromtimestamp(float(value)).date()
        except Exception:
            pass
    s = str(value).strip()
    # try ISO first
    try:
        # dt.fromisoformat handles many variants, including 'YYYY-MM-DD'
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        pass
    # try known formats
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    # last resort: let SQL handle NULL instead of raising
    log.warning("Unparseable date string: %r -> using NULL", value)
    return None

def json_deserializer(v):
    # Handle tombstones and empty payloads
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        s = v.decode("utf-8", errors="replace").strip()
    else:
        s = str(v).strip()
    if not s:          # empty string after strip
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # Return the raw string so you can inspect/route DLQ instead of crashing
        return {"__raw__": s, "__parse_error__": "JSONDecodeError"}

async def consume():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        enable_auto_commit=False,        # commit only after successful insert
        auto_offset_reset="earliest",
        value_deserializer=json_deserializer,
        key_deserializer=lambda k: k.decode("utf-8", "ignore") if k else None,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            tp = TopicPartition(msg.topic, msg.partition)
            val = msg.value

            # Log context for bad records
            if val is None or (isinstance(val, dict) and "__parse_error__" in val):
                log.warning(
                    "Skipping non-JSON/tombstone message t=%s p=%s o=%s key=%r value=%r",
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value
                )
                # Optionally send to a DLQ topic here instead of skipping.
                await consumer.commit()   # commit so we don't re-read the bad one
                continue

            try:
                # TODO: validate schema here before DB insert
                await insert_record(val)  # your function
                await consumer.commit()
            except Exception as e:
                # Don't commit on DB failure; log with full context
                log.exception(
                    "DB insert failed for t=%s p=%s o=%s key=%r val=%r",
                    msg.topic, msg.partition, msg.offset, msg.key, val
                )
                # optional: produce to a DLQ; or break/retry
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())