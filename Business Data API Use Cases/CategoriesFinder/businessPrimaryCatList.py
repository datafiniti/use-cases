#!/usr/bin/env python3
"""
Discover categories values for:
    country:US AND primaryCategories:"Construction"

Strategy:
- Query 1 record at a time
- Whenever categories are found, add them into:
    AND -categories:("A" OR "B" OR ...)
- Stop when 0 records are returned

CSV output:
categories
"A; B; C"
"D"
"""

import csv
import sys
import time

import requests

API_TOKEN = "Insert_API_Key"

# -----------------------------
# CONFIG

BUSINESS_PAGINATE_URL = "https://api.datafiniti.co/v4/business/paginate"

# Your base query (plus categories:* to ensure the field exists in returned records)
BASE_QUERY = 'country:US AND primaryCategories:"Construction" AND categories:*'

MAX_ITERATIONS = 3000
SLEEP_SECONDS = 0.1
MAX_QUERY_LENGTH = 25000

OUTPUT_CSV = r"C:\\Users\\Leonard\\Documents\\pythonScripts\\BusinessData\\business_categories_construction_us.csv"


# -----------------------------
# HELPERS
# -----------------------------
def extract_records(payload):
    for key in ("records", "data", "businesses", "results"):
        val = payload.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
    return []


def normalize_categories(record):
    """
    categories may be:
    - list[str]
    - str
    - missing / null
    Normalize to list[str].
    """
    val = record.get("categories")

    if val is None:
        return []

    if isinstance(val, str):
        s = val.strip()
        return [s] if s else []

    if isinstance(val, list):
        out = []
        for x in val:
            if isinstance(x, str):
                s = x.strip()
                if s:
                    out.append(s)
        return out

    return []


def escape_query_value(s):
    # Escape for inclusion inside query quotes
    return s.replace("\\", "\\\\").replace('"', '\\"')


def build_exclusion_clause(values):
    """
    Build:
      AND -categories:("A" OR "B" OR "C")
    """
    if not values:
        return ""

    parts = []
    for v in values:
        parts.append('"' + escape_query_value(v) + '"')

    return " AND -categories:(" + " OR ".join(parts) + ")"


def df_paginate_one(query, timeout=60, retries=5, sleep_backoff=1.5):
    """
    Fetch a single record via paginate with page=1&limit=1.
    """
    headers = {
        "Authorization": "Bearer " + API_TOKEN,
        "accept": "application/json",
        "content-type": "application/json",
    }

    params = {"page": 1, "limit": 1}
    body = {"query": query, "format": "JSON"}

    last_err = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                BUSINESS_PAGINATE_URL,
                headers=headers,
                params=params,
                json=body,
                timeout=timeout,
            )

            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_backoff * attempt)
                continue

            resp.raise_for_status()
            payload = resp.json() if resp.content else {}
            return extract_records(payload)

        except Exception as e:
            last_err = e
            time.sleep(sleep_backoff * attempt)

    raise RuntimeError("Failed after retries. Last error: " + str(last_err))


# -----------------------------
# MAIN
# -----------------------------
def main():
    if not API_TOKEN or API_TOKEN == "YOUR_DATAFINITI_API_TOKEN_HERE":
        print("ERROR: Please set your API token in the script.", file=sys.stderr)
        return 2

    found = []       # ordered unique category values discovered
    found_set = set()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["categories"])

        for i in range(1, MAX_ITERATIONS + 1):
            exclusion = build_exclusion_clause(found)
            query = BASE_QUERY + exclusion

            if len(query) > MAX_QUERY_LENGTH:
                print("[done] Query length exceeded safety limit. Stopping.")
                break

            print("\n[iter {}] QUERY: {}".format(i, query))

            records = df_paginate_one(query=query)

            if not records:
                print("[done] 0 records returned. Stopping.")
                break

            cats = normalize_categories(records[0])

            if not cats:
                print("[done] Record returned but categories empty. Stopping.")
                break

            new_cats = []
            for c in cats:
                if c not in found_set:
                    found_set.add(c)
                    found.append(c)
                    new_cats.append(c)

            # One cell per line, joined just like your primaryCategories output
            writer.writerow(["; ".join(cats)])

            print(
                "[iter {}] categories_in_record={} | new_added={} | total_unique={}".format(
                    i, len(cats), len(new_cats), len(found)
                )
            )

            if not new_cats:
                print("[done] No new categories added. Stopping to avoid loop.")
                break

            if SLEEP_SECONDS > 0:
                time.sleep(SLEEP_SECONDS)

    print("\nWrote CSV to:", OUTPUT_CSV)
    print("Total unique categories discovered (deduped for exclusions):", len(found))
    return 0


if __name__ == "__main__":
    sys.exit(main())