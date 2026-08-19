#!/usr/bin/env python3
"""
Process a JSON-Lines property data file.

For each property record:
  1. Check whether ANY broker has a `dateSeen` within 2025-08-12 .. 2026-08-12.
  2. If so, and the record contains a `people_key` (on that broker), export the
     property record to a property CSV using the Datafiniti "Default view" header.
  3. For each people_key found, query the Datafiniti People Data search API with
     query:  keys:"PEOPLE_KEY_HERE"  , num_records: 1.
     If num_found >= 1, export the people record(s) to a people CSV.

Usage:
    export DATAFINITI_TOKEN="your_api_token"
    python process_property_brokers.py \
        --input /path/to/input.jsonl \
        --property-out property_output.csv \
        --people-out people_output.csv
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# Insert your Datafiniti API token here (local to you).
# Alternatively, set the DATAFINITI_TOKEN env var or pass --token.
DATAFINITI_TOKEN = "API_KEY_HERE"

PEOPLE_SEARCH_URL = "https://api.datafiniti.co/v4/people/search"

# Delay (seconds) between People API calls, to stay under rate limits.
API_CALL_DELAY = 0.1

# Limit how many input rows to process. 0 or None means process all rows.
# Can be overridden with --max-rows.
MAX_ROWS = 0

# Name of the input JSON-Lines file, expected in the same folder as this
# script. Can be overridden with --input.
INPUT_FILE = "input.txt_file_here"

DATE_START = datetime(2025, 8, 12, tzinfo=timezone.utc)
DATE_END = datetime(2026, 8, 12, 23, 59, 59, tzinfo=timezone.utc)

# Datafiniti Property Data "Default view" field order.
# https://docs.datafiniti.co/docs/available-views-for-property-data#default-view
PROPERTY_DEFAULT_VIEW = [
    "address", "brokers", "buildingName", "city", "country", "dateAdded",
    "dateUpdated", "deposits", "descriptions", "features", "fees",
    "floorSizeValue", "floorSizeUnit", "imageURLs", "languagesSpoken",
    "latitude", "leasingTerms", "listingName", "longitude", "lotSizeValue",
    "lotSizeUnit", "managedBy", "mlsNumber", "mostRecentSaleListPriceAmount",
    "mostRecentSaleListPriceDate", "mostRecentSoldPriceAmount",
    "mostRecentSoldPriceDate", "mostRecentStatus", "mostRecentStatusDate",
    "neighborhoods", "numBathroom", "numBedroom", "numFloor", "numPeople",
    "numRoom", "numUnit", "parking", "paymentTypes", "people", "petPolicy",
    "phones", "postalCode", "prices", "propertyTaxes", "propertyType",
    "province", "reviews", "rules", "statuses", "taxID", "yearBuilt",
]

# People CSV header (from the supplied header format).
PEOPLE_HEADER = [
    "id", "address", "businessCategories", "businessName", "city", "country",
    "emails", "firstName", "jobTitle", "keys", "lastName", "licenses",
    "linkedInURL", "mostRecentPropertyPurchaseDate",
    "mostRecentPropertyPurchaseKey", "personalEmails", "phones",
    "phoneNumbers", "primaryEmail", "professionalEmails",
    "propertiesOwnedHistory", "postalCode", "province", "sourceURLs",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_date(value):
    """Parse an ISO8601 dateSeen string into an aware datetime, or None."""
    if not value or not isinstance(value, str):
        return None
    v = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        # Fall back to a couple of common formats.
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%d"):
            try:
                dt = datetime.strptime(value.strip(), fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def in_range(dt):
    return dt is not None and DATE_START <= dt <= DATE_END


def iter_brokers(record):
    """Yield each broker entry in `record["brokers"]` as a dict.

    The field is normally a list of broker objects, but individual entries
    (or the whole field) are sometimes serialized as JSON strings rather than
    already-parsed objects, so each entry is parsed independently.
    """
    brokers = record.get("brokers") or []
    if isinstance(brokers, str):
        try:
            brokers = json.loads(brokers)
        except json.JSONDecodeError:
            return
    if not isinstance(brokers, list):
        return
    for b in brokers:
        if isinstance(b, str):
            try:
                b = json.loads(b)
            except json.JSONDecodeError:
                continue
        if isinstance(b, dict):
            yield b


def flatten_value(value):
    """Serialize a field value into a CSV-friendly string.

    Scalars pass through; lists/dicts become compact JSON so nothing is lost.
    """
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def record_to_property_row(record):
    return {f: flatten_value(record.get(f)) for f in PROPERTY_DEFAULT_VIEW}


def people_record_to_row(record):
    return {f: flatten_value(record.get(f)) for f in PEOPLE_HEADER}


def query_people(people_key, token, session, max_retries=3):
    """Query the People search API for a single people_key.

    Returns (num_found, [records]).
    """
    payload = {
        "query": f'keys:"{people_key}"',
        "num_records": 1,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.post(PEOPLE_SEARCH_URL, headers=headers,
                                json=payload, timeout=60)
        except requests.RequestException as exc:
            print(f"  ! request error for {people_key}: {exc} "
                f"(attempt {attempt})", file=sys.stderr)
            time.sleep(2 * attempt)
            continue

        if resp.status_code == 429:  # rate limited
            wait = int(resp.headers.get("Retry-After", 5 * attempt))
            print(f"  ! rate limited, waiting {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            print(f"  ! HTTP {resp.status_code} for {people_key}: "
                f"{resp.text[:200]}", file=sys.stderr)
            return 0, []

        data = resp.json()
        num_found = data.get("num_found", 0)
        records = data.get("records", []) or []
        return num_found, records

    return 0, []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    run_date_str = datetime.now().strftime("%Y-%m-%d")

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input",
                    default=os.path.join(SCRIPT_DIR, INPUT_FILE),
                    help="Input JSON-Lines file (defaults to the INPUT_FILE "
                        "variable in this file, resolved relative to the "
                        "script's folder)")
    ap.add_argument("--property-out",
                    default=f"property_output_{run_date_str}.csv")
    ap.add_argument("--people-out",
                    default=f"people_output_{run_date_str}.csv")
    ap.add_argument("--log-out",
                    default=f"run_log_{run_date_str}.log",
                    help="File to append the run's result summary to")
    ap.add_argument("--token",
                    default=DATAFINITI_TOKEN or os.environ.get("DATAFINITI_TOKEN"),
                    help="Datafiniti API token (defaults to the DATAFINITI_TOKEN "
                        "variable in this file or the env var of the same name)")
    ap.add_argument("--skip-people-api", action="store_true",
                    help="Only build the property CSV; skip People API calls")
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS,
                    help="Limit the number of input rows processed. "
                        "0 or unset means process all rows. "
                        "Defaults to the MAX_ROWS variable in this file.")
    args = ap.parse_args()

    if not args.skip_people_api and not args.token:
        print("ERROR: no API token. Set DATAFINITI_TOKEN or pass --token, "
            "or use --skip-people-api.", file=sys.stderr)
        sys.exit(1)

    session = requests.Session()

    seen_people_keys = set()   # de-dupe People API calls
    total = matched = people_written = 0

    # Append to existing output files; only write the header when the file is
    # new or empty so we don't repeat it on subsequent runs.
    prop_needs_header = (not os.path.exists(args.property_out)
                        or os.path.getsize(args.property_out) == 0)
    people_needs_header = (not os.path.exists(args.people_out)
                        or os.path.getsize(args.people_out) == 0)

    prop_f = open(args.property_out, "a", newline="", encoding="utf-8")
    people_f = open(args.people_out, "a", newline="", encoding="utf-8")
    prop_writer = csv.DictWriter(prop_f, fieldnames=PROPERTY_DEFAULT_VIEW)
    people_writer = csv.DictWriter(people_f, fieldnames=PEOPLE_HEADER)
    if prop_needs_header:
        prop_writer.writeheader()
    if people_needs_header:
        people_writer.writeheader()

    try:
        with open(args.input, encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                if args.max_rows and total >= args.max_rows:
                    break
                total += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"  ! line {lineno}: bad JSON ({exc})",
                        file=sys.stderr)
                    continue

                # A broker must be in the date range AND carry a people_key.
                qualifying_keys = []
                any_in_range = False
                for b in iter_brokers(record):
                    if in_range(parse_date(b.get("dateSeen"))):
                        any_in_range = True
                        pk = b.get("people_key")
                        if pk:
                            qualifying_keys.append(pk)

                # Requirement: broker dateSeen in range AND record has a people_key.
                if not (any_in_range and qualifying_keys):
                    continue

                matched += 1
                prop_writer.writerow(record_to_property_row(record))

                if args.skip_people_api:
                    continue

                for pk in qualifying_keys:
                    if pk in seen_people_keys:
                        continue
                    seen_people_keys.add(pk)
                    num_found, people_records = query_people(
                        pk, args.token, session)
                    time.sleep(API_CALL_DELAY)
                    if num_found >= 1 and people_records:
                        for pr in people_records:
                            people_writer.writerow(people_record_to_row(pr))
                            people_written += 1
                        print(f"  + {pk}: {num_found} found")
                    else:
                        print(f"  - {pk}: none found")
    finally:
        prop_f.close()
        people_f.close()

    summary_lines = [
        f"[{datetime.now().isoformat(timespec='seconds')}] Done.",
        f"  Records read:                 {total}",
        f"  Property records exported:    {matched}",
        f"  Unique people_keys queried:   {len(seen_people_keys)}",
        f"  People records exported:      {people_written}",
        f"  Property CSV: {args.property_out}",
        f"  People CSV:   {args.people_out}",
    ]
    print("\n" + "\n".join(summary_lines))

    with open(args.log_out, "a", encoding="utf-8") as log_f:
        log_f.write("\n".join(summary_lines) + "\n\n")


if __name__ == "__main__":
    main()