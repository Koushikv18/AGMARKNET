"""
acquire.py — Paginated data acquisition from data.gov.in AGMARKNET API.

Usage:
    python src/acquire.py               # all states
    python src/acquire.py --dry-run     # fetch 1 page, print schema, exit
    python src/acquire.py --state "Punjab"  # single state

The script partitions output into:
    data/raw/state=<state>/<chunk_N>.csv

Year and month are derived downstream during Spark processing (process.py).
It is fully resumable: already-completed state partitions are skipped.
Rate limiting: exponential back-off on HTTP 429 / 5xx responses.
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from states import STATES

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

API_KEY   = os.getenv("API_KEY", "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b")
DATA_DIR  = Path(os.getenv("DATA_DIR", "data"))
BASE_URL  = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))  # safe page size for public demo key / rate limits
MAX_RETRIES = 6
BACKOFF_BASE = 2            # seconds — doubles on each retry
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

FIELDS = [
    "state", "district", "market", "commodity", "variety",
    "grade", "arrival_date", "min_price", "max_price", "modal_price",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_request(params: dict, retries: int = MAX_RETRIES) -> dict:
    """GET with exponential back-off on rate-limit / server errors."""
    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 500, 502, 503, 504):
                wait = BACKOFF_BASE ** attempt
                log.warning("HTTP %s — retrying in %ds (attempt %d/%d)",
                            resp.status_code, wait, attempt + 1, retries)
                time.sleep(wait)
            else:
                log.error("HTTP %s — %s", resp.status_code, resp.text[:200])
                resp.raise_for_status()
        except requests.RequestException as exc:
            wait = BACKOFF_BASE ** attempt
            log.warning("Request error: %s — retrying in %ds", exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed after {retries} retries for params={params}")


def build_params(state: str, offset: int) -> dict:
    """Construct API query params for a state page (no arrival_date filter)."""
    return {
        "api-key": API_KEY,
        "format": "json",
        "limit": PAGE_SIZE,
        "offset": offset,
        "filters[state]": state,
    }


def records_to_csv_bytes(records: list[dict]) -> bytes:
    """Serialise a list of record dicts to CSV bytes (UTF-8)."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()
    for rec in records:
        # Normalise key names — API may return camelCase or snake_case variants
        row = {f: rec.get(f, rec.get(f.replace("_", ""), "")) for f in FIELDS}
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def partition_done(state: str) -> bool:
    """Return True if a completion sentinel file exists for this state partition."""
    sentinel = DATA_DIR / "raw" / f"state={state}" / ".done"
    return sentinel.exists()


def mark_done(state: str) -> None:
    sentinel = DATA_DIR / "raw" / f"state={state}" / ".done"
    sentinel.touch()


# ── Core acquisition logic ────────────────────────────────────────────────────

def pull_partition(state: str, dry_run: bool = False) -> int:
    """
    Pull all records for a state partition.
    Returns total records written.
    """
    out_dir = DATA_DIR / "raw" / f"state={state}"
    out_dir.mkdir(parents=True, exist_ok=True)

    if partition_done(state) and not dry_run:
        log.info("  SKIP  state=%s (already done)", state)
        return 0

    offset = 0
    chunk_idx = 0
    total = 0

    while True:
        params = build_params(state, offset)
        data = make_request(params)

        records = data.get("records", [])
        count = len(records)

        if count == 0:
            break

        if dry_run:
            # Just print schema and first record, then exit
            log.info("DRY RUN — fields in first record: %s",
                     list(records[0].keys()))
            log.info("DRY RUN — sample record: %s", json.dumps(records[0], indent=2))
            return count

        # Write chunk
        chunk_file = out_dir / f"chunk_{chunk_idx:04d}.csv"
        chunk_file.write_bytes(records_to_csv_bytes(records))
        log.debug("  wrote %s (%d rows)", chunk_file.name, count)

        total += count
        offset += count
        chunk_idx += 1

        # If fewer records returned than page size, we've hit the end
        if count < PAGE_SIZE:
            break

        # Polite delay to avoid hammering the API
        time.sleep(0.3)

    if not dry_run:
        mark_done(state)

    return total


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="AGMARKNET data acquisition")
    p.add_argument("--full", action="store_true",
                   help="Pull all states (default)")
    p.add_argument("--year", type=int, default=None,
                   help="Legacy parameter (year filtering is performed in process.py)")
    p.add_argument("--state", default=None,
                   help="Pull a single state only")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch 1 page, print schema, then exit")
    return p.parse_args()


def main():
    args = parse_args()

    states = [args.state] if args.state else STATES

    log.info("=" * 60)
    log.info("AGMARKNET Acquisition")
    log.info("  States : %d", len(states))
    log.info("  Output : %s", DATA_DIR / "raw")
    if args.year:
        log.info("  Note   : --year %d passed (date filtering is handled in process.py)", args.year)
    log.info("=" * 60)

    grand_total = 0

    with tqdm(total=len(states), desc="Partitions", unit="part") as pbar:
        for state in states:
            pbar.set_postfix(state=state[:12])
            try:
                n = pull_partition(state, dry_run=args.dry_run)
                grand_total += n
                if args.dry_run:
                    log.info("Dry-run complete — exiting.")
                    sys.exit(0)
            except Exception as exc:
                log.error("FAILED  state=%s : %s", state, exc)
            pbar.update(1)

    log.info("Done. Total records acquired: %s", f"{grand_total:,}")


if __name__ == "__main__":
    main()
