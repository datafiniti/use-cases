#!/usr/bin/env python3
"""
Scan a datafiniti property record CSV and get the owner People_key
Then search the people data API for and check if  the record has contact info.

Logic
1) For each row, if mostRecentOwnerPeopleKey is populated, query Datafiniti People Search:
country:US propertiesOwned:{KEY}
2) Skip consecutive duplicate keys.
3) If emails/phones/phoneNumbers exist in the People record → write to output CSV.
4) Include property_firstName & property_lastName from the input CSV.
"""

import csv
import json
import os
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------
# CONFIG
# -----------------------------
INPUT_CSV = r"Example_property_data_wPeopleKeys.csv"
# 1) Summary output (your rescues)
OUTPUT_SUMMARY_CSV = r"people_contact_SUMMARY.csv"

# 2) Full People record export (one row per matched person)
OUTPUT_PEOPLE_RECORDS_CSV = r"People_records_found.csv"

API_TOKEN = "Insert_API_KEY_HERE" #Have a valid Datafiniti API key
PEOPLE_SEARCH_URL = "https://api.datafiniti.co/v4/people/search"

START_ROW = 1 #What row to start within the csv
MAX_ROWS = 0 #How many rows to process. If 0, it will process all

MAX_WORKERS = 3 #number of multi-thread processors
SLEEP_SECONDS = 0.2 #sleep timer between API calls 


# -----------------------------
# HELPERS
# -----------------------------
def is_populated(value):
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def to_json_cell(value):
    """Serialize lists/dicts safely for CSV."""
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def get_owner_name(row, field_variants):
    for field in field_variants:
        val = row.get(field)
        if val and str(val).strip():
            return val
    return ""


def has_email_match(person_record):
    return 1 if is_populated(person_record.get("emails")) else 0


def has_phone_match(person_record):
    if is_populated(person_record.get("phoneNumbers")):
        return 1
    if is_populated(person_record.get("phones")):
        return 1
    return 0


def contact_counts(person_record):
    emails = person_record.get("emails")
    phones = person_record.get("phones")
    phone_numbers = person_record.get("phoneNumbers")

    email_count = len(emails) if isinstance(emails, list) else (1 if is_populated(emails) else 0)
    phones_count = len(phones) if isinstance(phones, list) else (1 if is_populated(phones) else 0)
    phone_numbers_count = (
        len(phone_numbers) if isinstance(phone_numbers, list) else (1 if is_populated(phone_numbers) else 0)
    )

    has_any = is_populated(emails) or is_populated(phones) or is_populated(phone_numbers)
    return has_any, email_count, phones_count, phone_numbers_count


def people_query_for_key(people_key):
    return f'country:US AND (propertiesOwned:"{people_key}" OR keys:"{people_key}")'


# -----------------------------
# THREADED WORKER
# -----------------------------
def worker_search(work_item):
    input_row_index = work_item["input_row_index"]
    api_query = work_item["api_query"]

    print(f"[API CALL] row={input_row_index} query={api_query}")

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {"query": api_query, "num_records": 1}

    session = requests.Session()
    t0 = time.time()

    try:
        resp = session.post(PEOPLE_SEARCH_URL, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("records") or []
        person = records[0] if records else None
        ok = True
        err = ""
    except Exception as e:
        person = None
        ok = False
        err = str(e)
    finally:
        if SLEEP_SECONDS > 0:
            time.sleep(SLEEP_SECONDS)

    dt = time.time() - t0

    return {
        "input_row_index": input_row_index,
        "people_key_val": work_item["people_key"],
        "api_query": api_query,
        "row_context": work_item["row_context"],
        "input_row_full": work_item["input_row_full"],
        "ok": ok,
        "error": err,
        "response_time_sec": dt,
        "person": person,
    }


# -----------------------------
# MAIN
# -----------------------------
def main():
    # Validate config, read CSV, build work list, run threaded API calls,
    # write ordered outputs, export matched rows with appended people contact fields, print metrics.

    if not API_TOKEN or API_TOKEN == "PASTE_YOUR_DATAFINITI_TOKEN_HERE":
        print("ERROR: Please set API_TOKEN to your Datafiniti token in the script.", file=sys.stderr)
        return 2

    os.makedirs(os.path.dirname(OUTPUT_SUMMARY_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_PEOPLE_RECORDS_CSV), exist_ok=True)

    with open(INPUT_CSV, "r", newline="", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        in_headers = reader.fieldnames or []

        key_col = "mostRecentOwnerPeopleKey"
        if key_col not in in_headers:
            print("ERROR: Column 'mostRecentOwnerPeopleKey' not found in input CSV.", file=sys.stderr)
            return 2

        FIRST_NAME_FIELDS = ["property_firstName", "owners.firstName"]
        LAST_NAME_FIELDS = ["property_lastName", "owners.lastName"]

        summary_headers = [
            "input_row_index","property_id","property_address","property_city","property_province",
            "property_postalCode","owners.firstName","owners.lastName","mostRecentOwnerPeopleKey",
            "people_api_query","people_found","people_id","people_key","has_contact_info",
            "num_found_contact_emails","num_found_contact_phones","num_found_contact_phoneNumbers",
            "people_name","response_time_sec","error",
        ]

        # People output = original input headers + appended people contact fields
        people_output_headers = in_headers + ["people.emails", "people.phones", "people.phoneNumbers"]

        work_items = []
        last_key = None
        processed_rows_considered = 0

        for idx, row in enumerate(reader, start=1):
            if idx < START_ROW:
                continue
            if MAX_ROWS and processed_rows_considered >= MAX_ROWS:
                break

            processed_rows_considered += 1

            people_key_val = (row.get(key_col) or "").strip()
            if not people_key_val:
                continue

            if last_key is not None and people_key_val == last_key:
                continue
            last_key = people_key_val

            owner_first = get_owner_name(row, FIRST_NAME_FIELDS)
            owner_last = get_owner_name(row, LAST_NAME_FIELDS)

            ctx = {
                "property_id": row.get("id") or row.get("property_id") or "",
                "property_address": row.get("address") or "",
                "property_city": row.get("city") or "",
                "property_province": row.get("province") or row.get("state") or "",
                "property_postalCode": row.get("postalCode") or "",
                "owners_first": owner_first,
                "owners_last": owner_last,
            }

            api_query = people_query_for_key(people_key_val)

            work_items.append({
                "input_row_index": idx,
                "people_key": people_key_val,
                "api_query": api_query,
                "row_context": ctx,
                "input_row_full": row,
            })

    results_by_row = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_to_row = {ex.submit(worker_search, wi): wi["input_row_index"] for wi in work_items}
        for fut in as_completed(fut_to_row):
            results_by_row[fut_to_row[fut]] = fut.result()

    email_match_count = 0
    phone_match_count = 0

    with open(OUTPUT_SUMMARY_CSV, "w", newline="", encoding="utf-8") as f_sum, open(
        OUTPUT_PEOPLE_RECORDS_CSV, "w", newline="", encoding="utf-8"
    ) as f_people:

        summary_writer = csv.DictWriter(f_sum, fieldnames=summary_headers)
        summary_writer.writeheader()

        people_writer = csv.DictWriter(f_people, fieldnames=people_output_headers)
        people_writer.writeheader()

        for wi in sorted(work_items, key=lambda x: x["input_row_index"]):
            idx = wi["input_row_index"]
            res = results_by_row.get(idx)
            ctx = res["row_context"]
            person = res["person"]

            people_found = 1 if person else 0

            person_id = ""
            person_key = ""
            full_name = ""
            has_contact_info = 0
            email_ct = 0
            phones_ct = 0
            phone_numbers_ct = 0

            if person:
                has_any, email_ct, phones_ct, phone_numbers_ct = contact_counts(person)
                has_contact_info = 1 if has_any else 0

                email_match_count += has_email_match(person)
                phone_match_count += has_phone_match(person)

                person_id = person.get("id") or ""
                person_key = person.get("people_key") or person.get("key") or ""

                first = person.get("firstName") or ""
                last = person.get("lastName") or ""
                full_name = (f"{first} {last}").strip() or person.get("name") or ""

                # Export FULL original input row + appended people contact fields
                if has_any:
                    out_row = dict(res["input_row_full"])
                    out_row["people.emails"] = to_json_cell(person.get("emails"))
                    out_row["people.phones"] = to_json_cell(person.get("phones"))
                    out_row["people.phoneNumbers"] = to_json_cell(person.get("phoneNumbers"))
                    people_writer.writerow(out_row)

            summary_writer.writerow({
                "input_row_index": idx,
                "property_id": ctx.get("property_id", ""),
                "property_address": ctx.get("property_address", ""),
                "property_city": ctx.get("property_city", ""),
                "property_province": ctx.get("property_province", ""),
                "property_postalCode": ctx.get("property_postalCode", ""),
                "owners.firstName": ctx.get("owners_first", ""),
                "owners.lastName": ctx.get("owners_last", ""),
                "mostRecentOwnerPeopleKey": res.get("people_key_val", ""),
                "people_api_query": res.get("api_query", ""),
                "people_found": people_found,
                "people_id": person_id,
                "people_key": person_key,
                "has_contact_info": has_contact_info,
                "num_found_contact_emails": email_ct,
                "num_found_contact_phones": phones_ct,
                "num_found_contact_phoneNumbers": phone_numbers_ct,
                "people_name": full_name,
                "response_time_sec": f'{res.get("response_time_sec", 0.0):.3f}',
                "error": "" if res.get("ok") else res.get("error", ""),
            })

    print("\n==== MATCH COUNTS ====")
    print(f"People records with >=1 email: {email_match_count}")
    print(f"People records with >=1 phone: {phone_match_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())