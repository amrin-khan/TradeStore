from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()

def test_post_trade():
    trade = {
        "trade_id": "T1",
        "version": "1",
        "counterparty_id": "CP-1",
        "book_id": "B1",
        "maturity_date": "2025-12-31",
        "created_date": "2025-09-21",
        "expired": "N"
    }
    response = client.post("/trades", json=trade)
    assert response.status_code == 202
    assert response.json()["status"] == "received_and_published"