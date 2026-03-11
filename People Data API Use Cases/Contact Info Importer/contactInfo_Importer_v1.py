import csv
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------
# CONFIG
# -----------------------------
API_TOKEN = "Insert_Your_DF_API_Key"

INPUT_CSV = r"C:\\Users\\Leonard\\Documents\\pythonScripts\\contactInfoMatch\\tulsa_countyOK.csv"

OUTPUT_DETAIL_CSV = r"C:\\Users\\Leonard\\Documents\\pythonScripts\\contactInfoMatch\\people_contact_check_detail_tulsa_countyOK_0302.csv"
OUTPUT_SUMMARY_TXT = r"C:\\Users\\Leonard\\Documents\\pythonScripts\\contactInfoMatch\\people_contact_check_summary_tulsa_countyOK_0302.txt"
OUTPUT_WITH_CONTACT_CSV = r"C:\\Users\\Leonard\\Documents\\pythonScripts\\contactInfoMatch\\people_with_contact_tulsa_countyOK_0302.csv"

THREADS = 3
START_ROW = 1          # set to skip rows if needed
MAX_ROWS = None      # set to limit processing (e.g., 1000)

API_URL = "https://api.datafiniti.co/v4/people/search"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}

REQUEST_TIMEOUT_SEC = 30

# If True, caches People API lookups per peopleKey to reduce duplicate calls/credits
ENABLE_PEOPLEKEY_CACHE = True

# -----------------------------
# OUTPUT HEADERS
# -----------------------------
WITH_CONTACT_HEADERS = [
    "property address",
    "property city",
    "property postal code",
    "property owners first name",
    "property owners last name",
    "property id",
    "property owners people key",
    "owner contact type",  # ✅ NEW: email only / phone only / email and phone
    "matched people emails",
    "matched people phonenumber",
    "matched people id",
]

DETAIL_HEADERS = [
    "property_id",
    "property_address",
    "property_city",
    "property_postalCode",
    "owners_firstName",
    "owners_lastName",
    "owners_peopleKey",
    "people_query",
    "matched_people_id",
    "matched_people_emails",
    "matched_people_phoneNumber",
    "has_email",
    "has_phone",
    "has_contact",
    "contact_type",
    "status",
    "response_time_sec",
]


# -----------------------------
# CACHE
# -----------------------------
_people_cache = {}
_people_cache_lock = threading.Lock()


# -----------------------------
# HELPERS
# -----------------------------
def safe_get(row, key):
    v = row.get(key, "")
    return (v.strip() if isinstance(v, str) else v)


def extract_people_key(row):
    # input key comes from property owners.peopleKey field in property records
    return (safe_get(row, "owners.peopleKey") or safe_get(row, "peopleKey") or "").strip()


def normalize_emails(value):
    if not value:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if v and str(v).strip()]
    return [str(value).strip()] if str(value).strip() else []


def normalize_phones(phones_value, phone_numbers_value):
    out = []

    def add(v):
        if not v:
            return
        if isinstance(v, list):
            for item in v:
                s = str(item).strip()
                if s:
                    out.append(s)
        else:
            s = str(v).strip()
            if s:
                out.append(s)

    add(phones_value)
    add(phone_numbers_value)

    # de-dupe preserving order
    seen = set()
    deduped = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def classify_contact(has_email: int, has_phone: int) -> str:
    """
    Required summary buckets:
    - owners email only
    - owners phone only
    - owners email and phone
    """
    if has_email and has_phone:
        return "owners email and phone"
    if has_email:
        return "owners email only"
    if has_phone:
        return "owners phone only"
    return ""


def people_lookup_by_key(people_key):
    """
    People API lookup uses keys:"<value>" (NOT peopleKey).
    """
    if ENABLE_PEOPLEKEY_CACHE:
        with _people_cache_lock:
            if people_key in _people_cache:
                return _people_cache[people_key]

    query = f'keys:"{people_key}"'
    payload = {"query": query, "num_records": 1}

    # Print the query sent
    print(f"\nQUERY: {query}")

    t0 = time.time()
    try:
        resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT_SEC)
        elapsed = round(time.time() - t0, 3)

        if resp.status_code != 200:
            result = {
                "status": f"http_{resp.status_code}",
                "response_time_sec": elapsed,
                "query": query,
                "matched_people_id": "",
                "emails_list": [],
                "phones_list": [],
                "has_email": 0,
                "has_phone": 0,
                "has_contact": 0,
                "contact_type": "",
            }
        else:
            data = resp.json() or {}
            records = data.get("records") or []

            if not records:
                result = {
                    "status": "no_record",
                    "response_time_sec": elapsed,
                    "query": query,
                    "matched_people_id": "",
                    "emails_list": [],
                    "phones_list": [],
                    "has_email": 0,
                    "has_phone": 0,
                    "has_contact": 0,
                    "contact_type": "",
                }
            else:
                rec = records[0] or {}
                matched_people_id = rec.get("id", "") or ""

                emails_list = normalize_emails(rec.get("emails"))
                phones_list = normalize_phones(rec.get("phones"), rec.get("phoneNumbers"))

                has_email = 1 if emails_list else 0
                has_phone = 1 if phones_list else 0
                has_contact = 1 if (has_email or has_phone) else 0
                contact_type = classify_contact(has_email, has_phone) if has_contact else ""

                result = {
                    "status": "ok",
                    "response_time_sec": elapsed,
                    "query": query,
                    "matched_people_id": matched_people_id,
                    "emails_list": emails_list,
                    "phones_list": phones_list,
                    "has_email": has_email,
                    "has_phone": has_phone,
                    "has_contact": has_contact,
                    "contact_type": contact_type,
                }

    except Exception as e:
        result = {
            "status": f"error:{str(e)}",
            "response_time_sec": 0,
            "query": query,
            "matched_people_id": "",
            "emails_list": [],
            "phones_list": [],
            "has_email": 0,
            "has_phone": 0,
            "has_contact": 0,
            "contact_type": "",
        }

    if ENABLE_PEOPLEKEY_CACHE:
        with _people_cache_lock:
            _people_cache[people_key] = result

    return result


def process_property_row(row):
    """
    Returns:
      - detail_row (always)
      - with_contact_row (only if contact found)
    """
    prop_id = safe_get(row, "id")
    address = safe_get(row, "address")
    city = safe_get(row, "city")
    postal = safe_get(row, "postalCode")
    owner_fn = safe_get(row, "owners.firstName")
    owner_ln = safe_get(row, "owners.lastName")
    people_key = extract_people_key(row)

    if not people_key:
        detail = {
            "property_id": prop_id,
            "property_address": address,
            "property_city": city,
            "property_postalCode": postal,
            "owners_firstName": owner_fn,
            "owners_lastName": owner_ln,
            "owners_peopleKey": "",
            "people_query": "",
            "matched_people_id": "",
            "matched_people_emails": "",
            "matched_people_phoneNumber": "",
            "has_email": 0,
            "has_phone": 0,
            "has_contact": 0,
            "contact_type": "",
            "status": "missing_peopleKey",
            "response_time_sec": 0,
        }
        return detail, None

    match = people_lookup_by_key(people_key)

    emails_str = "; ".join(match["emails_list"])
    phones_str = "; ".join(match["phones_list"])

    detail = {
        "property_id": prop_id,
        "property_address": address,
        "property_city": city,
        "property_postalCode": postal,
        "owners_firstName": owner_fn,
        "owners_lastName": owner_ln,
        "owners_peopleKey": people_key,
        "people_query": match["query"],
        "matched_people_id": match["matched_people_id"],
        "matched_people_emails": emails_str,
        "matched_people_phoneNumber": phones_str,
        "has_email": match["has_email"],
        "has_phone": match["has_phone"],
        "has_contact": match["has_contact"],
        "contact_type": match["contact_type"],
        "status": match["status"],
        "response_time_sec": match["response_time_sec"],
    }

    if match["has_contact"] == 1:
        # ✅ OUTPUT_WITH_CONTACT_CSV now includes contact_type + respects email/phone-only logic
        with_contact = {
            "property address": address,
            "property city": city,
            "property postal code": postal,
            "property owners first name": owner_fn,
            "property owners last name": owner_ln,
            "property id": prop_id,
            "property owners people key": people_key,
            "owner contact type": match["contact_type"],
            "matched people emails": emails_str,
            "matched people phonenumber": phones_str,
            "matched people id": match["matched_people_id"],
        }
        return detail, with_contact

    return detail, None


# -----------------------------
# MAIN
# -----------------------------
def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    rows = rows[START_ROW:]
    if MAX_ROWS is not None:
        rows = rows[:MAX_ROWS]

    print(f"Input rows to process: {len(rows)}")
    print(f"Threads: {THREADS} | Cache enabled: {ENABLE_PEOPLEKEY_CACHE}")

    detail_rows = []
    with_contact_rows = []

    # ✅ Updated summary buckets
    summary = {
        "total_rows": 0,
        "rows_with_peopleKey": 0,
        "rows_missing_peopleKey": 0,
        "rows_with_contact": 0,
        "rows_without_contact": 0,
        "owners_email_only": 0,
        "owners_phone_only": 0,
        "owners_email_and_phone": 0,
    }

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = [ex.submit(process_property_row, r) for r in rows]

        for fut in as_completed(futures):
            detail, with_contact = fut.result()
            detail_rows.append(detail)

            summary["total_rows"] += 1
            if detail["status"] == "missing_peopleKey":
                summary["rows_missing_peopleKey"] += 1
            else:
                summary["rows_with_peopleKey"] += 1

            if detail["has_contact"] == 1:
                summary["rows_with_contact"] += 1

                # ✅ bucket counts
                ct = detail.get("contact_type", "")
                if ct == "owners email only":
                    summary["owners_email_only"] += 1
                elif ct == "owners phone only":
                    summary["owners_phone_only"] += 1
                elif ct == "owners email and phone":
                    summary["owners_email_and_phone"] += 1

            else:
                summary["rows_without_contact"] += 1

            if with_contact:
                with_contact_rows.append(with_contact)

            print(
                f'Property {detail["property_id"]} | peopleKey={detail["owners_peopleKey"]} | '
                f'contact={detail["has_contact"]} | type={detail.get("contact_type","")} | '
                f'status={detail["status"]} | {detail["response_time_sec"]}s'
            )

    # Write detail CSV (audit)
    with open(OUTPUT_DETAIL_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DETAIL_HEADERS)
        w.writeheader()
        w.writerows(detail_rows)

    # Write with-contact CSV (requested schema)
    with open(OUTPUT_WITH_CONTACT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=WITH_CONTACT_HEADERS)
        w.writeheader()
        w.writerows(with_contact_rows)

    # Write summary file
    with open(OUTPUT_SUMMARY_TXT, "w", encoding="utf-8") as f:
        f.write("PROPERTY -> PEOPLE CONTACT MATCH SUMMARY\n")
        f.write("======================================\n")
        f.write(f"Total property rows processed: {summary['total_rows']}\n")
        f.write(f"Rows with peopleKey: {summary['rows_with_peopleKey']}\n")
        f.write(f"Rows missing peopleKey: {summary['rows_missing_peopleKey']}\n\n")

        f.write(f"Rows WITH contact (email OR phone): {summary['rows_with_contact']}\n")
        f.write(f"  - owners email only: {summary['owners_email_only']}\n")
        f.write(f"  - owners phone only: {summary['owners_phone_only']}\n")
        f.write(f"  - owners email and phone: {summary['owners_email_and_phone']}\n")
        f.write(f"Rows WITHOUT contact: {summary['rows_without_contact']}\n")

    # ✅ Print summary to console
    print("\n📊 SUMMARY")
    print(f"Total property rows processed: {summary['total_rows']}")
    print(f"Rows with peopleKey: {summary['rows_with_peopleKey']}")
    print(f"Rows missing peopleKey: {summary['rows_missing_peopleKey']}")
    print(f"Rows WITH contact (email OR phone): {summary['rows_with_contact']}")
    print(f"  - owners email only: {summary['owners_email_only']}")
    print(f"  - owners phone only: {summary['owners_phone_only']}")
    print(f"  - owners email and phone: {summary['owners_email_and_phone']}")
    print(f"Rows WITHOUT contact: {summary['rows_without_contact']}")

    print("\n📁 Outputs")
    print(f"Detail (audit): {OUTPUT_DETAIL_CSV}")
    print(f"With-contact:   {OUTPUT_WITH_CONTACT_CSV}")
    print(f"Summary file:   {OUTPUT_SUMMARY_TXT}")


if __name__ == "__main__":
    main()