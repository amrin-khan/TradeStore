
import csv
import json
import re
import sys
from typing import Dict, Iterable
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- config ----------
CSV_PATH = "./rawData/TradeData.csv"
ENDPOINT = "http://tradedata:8001/trades"   
BEARER_TOKEN = ""                           
EXTRA_HEADERS = {"X-Source": "csv-import"}  
TIMEOUT_SECS = 15
RETRIES = 3
BACKOFF = 0.5
# ----------------------------

def normalize_header(h):
    """Convert 'Trade Id' -> 'trade_id', 'Counter-Party Id' -> 'counterparty_id'."""
    h = h.lstrip("\ufeff")
    h = h.strip().lower()
    h = re.sub(r"[ \-]+", "_", h)  # replace spaces/dashes with underscores
    return h

def csv_to_json(filepath: str) -> Iterable[Dict[str, str]]:
    with open(filepath, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # normalize headers
        reader.fieldnames = [normalize_header(h) for h in reader.fieldnames]

        for row in reader:
            # strip spaces from values
            yield {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

def build_session() -> requests.Session:
    s = requests.Session()
    # retries for network errors & 5xx
    retry = Retry(
        total=RETRIES,
        backoff_factor=BACKOFF,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

# in InputFeed script (post_json_rows)
def post_json_rows(rows: Iterable[Dict[str, str]]) -> None:
    session = build_session()
    headers = {"Content-Type": "application/json", **EXTRA_HEADERS}
    if BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {BEARER_TOKEN}"

    failures = 0  # <--- add
    for i, row in enumerate(rows, start=1):
        try:
            data = json.dumps(row)
            resp = session.post(ENDPOINT, data=data, headers=headers, timeout=TIMEOUT_SECS)
            if 200 <= resp.status_code < 300:
                print(f"[OK] line {i}: {row.get('trade_id', '')}")
            else:
                failures += 1
                print(f"[ERR] line {i}: HTTP {resp.status_code} -> {resp.text[:200]}", file=sys.stderr)
        except requests.RequestException as e:
            failures += 1
            print(f"[ERR] line {i}: {e}", file=sys.stderr)

    if failures:
        print(f"[FAIL] {failures} errors during ingest", file=sys.stderr)
        sys.exit(1)  # <--- make the container exit non-zero when anything fails

if __name__ == "__main__":
    try:
        rows = csv_to_json(CSV_PATH)
        post_json_rows(rows)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.", file=sys.stderr)
        sys.exit(1)