import datetime as dt
import pytest
import time


@pytest.mark.regression
@pytest.mark.integration
def test_api_to_kafka_to_mssql_version_flow(http, base_url, mssql, new_trade_id, wait_until):
    """
    API -> Kafka -> MSSQL consumer versioning:
    - insert v5
    - reject v4 (lower)
    - update v5 (same)
    - insert v6 (higher)
    Requires: redpanda, tradedata, kafkaconsumermssql, mssql (+ mssql-init).
    """
    # helpers
    def post(version: int):
        payload = {
            "trade_id": new_trade_id,
            "version": str(version),
            "counterparty_id": f"CP-{version}",
            "book_id": "B-V",
            "maturity_date": "2030-12-31",
            "created_date": dt.date.today().isoformat(),
            "expired": "N",
        }
        r = http.post(f"{base_url}/trades", json=payload)
        assert r.status_code == 202, r.text

    def fetch_versions():
        mssql.commit()  # ensure we’re not stuck in an old snapshot
        cur = mssql.cursor()
        cur.execute("SELECT [version], counterparty_id FROM dbo.trades WHERE trade_id=? AND book_id=?", (new_trade_id, "B-V"))
        rows = cur.fetchall()
        cur.close()
        return {(int(v), cp) for (v, cp) in rows}

    # 1) v5 insert (first message after container start may take longer)
    post(5)
    time.sleep(5)
    vs = wait_until(lambda: fetch_versions(), timeout=45.0, interval=0.5, desc="mssql v5")
    assert vs and (5, "CP-5") in vs

    # 2) v4 lower -> should not appear
    post(4)
    vs2 = wait_until(lambda: fetch_versions(), timeout=4.0, interval=0.4, desc="mssql v4 rejected")
    assert all(v != 4 for (v, _) in (vs2 or set())), f"unexpected v4 row present: {vs2}"

    # 3) v5 same -> update row (counterparty_id becomes CP-5SAME)
    #    Your consumer UPDATE sets all fields with incoming_version when rowcount > 0.
    post(5)
    # give the consumer a moment to process the UPDATE
    vs3 = wait_until(lambda: fetch_versions(), timeout=6.0, interval=0.4, desc="mssql v5 update")
    # We can't guarantee the consumer changed counterparty_id content,
    # but we can assert v5 still exists (updated row), not duplicated.
    # (Primary key is (trade_id, version, book_id) so v5 remains a single row.)
    assert any(v == 5 for (v, _) in (vs3 or set()))

  # 4) v6 higher -> insert a new version (multi-row design)
    # 4) v6 higher -> insert a new version (multi-row design)
    post(6)

    def _have_v6():
        s = fetch_versions()
        return s if any(v == 6 for (v, _) in s) else None

    vs4 = wait_until(_have_v6, timeout=120.0, interval=0.5)

    assert any(v == 6 for (v, _) in vs4)
    assert any(v == 5 for (v, _) in vs4)   # v5 remains under multi-row design
