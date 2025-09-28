import datetime as dt
import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_api_to_kafka_to_mongo(http, base_url, mongo, new_trade_id, wait_until):
    """
    End-to-end:
    TradeData API -> Kafka -> mongo consumer -> document lands in Mongo with normalized fields.
    Requires: redpanda, tradedata, kafkaconsumer, mongo up.
    """
    payload = {
        "trade_id": new_trade_id,
        "version": "1",
        "counterparty_id": "CP-R",
        "book_id": "B-R",
        "maturity_date": "2030-12-31",
        "created_date": dt.date.today().isoformat(),
        "expired": "N",
    }

    # 1) POST to the API
    r = http.post(f"{base_url}/trades", json=payload)
    assert r.status_code == 202, r.text

    # 2) Wait for Mongo doc to appear
    def _find():
        return mongo.find_one({"trade_id": new_trade_id, "book_id": "B-R"})

    doc = wait_until(_find, timeout=10.0, interval=0.3, desc="mongo document")
    assert doc, "expected doc in Mongo, but none found within timeout"

    # 3) Validate core normalization & metadata the consumer adds
    assert doc["trade_id"] == new_trade_id
    assert doc["counterparty_id"] == "CP-R"
    assert doc["book_id"] == "B-R"
    assert isinstance(doc.get("expired"), (bool, int)) and doc["expired"] is False
    assert "_id" in doc
    assert "ingest_time" in doc  # set by consumer
