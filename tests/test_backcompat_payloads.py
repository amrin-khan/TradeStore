import pytest
import datetime as dt


@pytest.mark.regression
@pytest.mark.integration
def test_backcompat_varied_date_formats_to_mongo(http, base_url, mongo, new_trade_id, wait_until):
    """
    Send a few historically-seen date formats and ensure the pipeline doesn't crash and stores documents.
    We don't over-assert the exact normalized format here (handled by consumer),
    just that docs arrive for each.
    """
    date_samples = [
        "2032-02-01",
        "2032-02-01T12:13:14",
        "01/02/2032",   # DMY
        "02/01/2032",   # MDY
    ]

    created = dt.date.today().isoformat()
    for i, md in enumerate(date_samples):
        tid = f"{new_trade_id}-{i}"
        payload = {
            "trade_id": tid,
            "version": "1",
            "counterparty_id": f"CP-{i}",
            "book_id": "B-DATES",
            "maturity_date": md,
            "created_date": created,
            "expired": "N",
        }
        r = http.post(f"{base_url}/trades", json=payload)
        assert r.status_code == 202, r.text

    # verify all landed
    for i, _ in enumerate(date_samples):
        tid = f"{new_trade_id}-{i}"
        doc = wait_until(lambda: mongo.find_one({"trade_id": tid, "book_id": "B-DATES"}), timeout=12.0, interval=0.3)
        assert doc, f"expected doc for {tid}"
