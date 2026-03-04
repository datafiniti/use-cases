import csv
import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# -----------------------------
# CONFIG
# -----------------------------
API_TOKEN = (
    "Insert_API_Key"
)

GOOGLE_ADDRVAL_API_KEY = "Insert_API_Key"

INPUT_CSV = r"parsed_addresses_FINAL_fixed_v5.csv"
SUMMARY_OUT_CSV = r"address_found_summary_Geoapify.csv"
PROPERTY_FOUND_OUT_CSV = r"property_records_Geoapify.csv"
BUSINESS_FOUND_OUT_CSV = r"business_records_Geoapify.csv"

# If you want only the first record per API, keep this as 1.
NUM_RECORDS_TO_FETCH = 1

# Optional: polite pacing (usually set this to 0 when using threads)
SLEEP_SECONDS_BETWEEN_CALLS = 0.0

# -----------------------------
# ROW LIMITER (for testing / cost control)
# Set to an integer (e.g. 100) to process only the first N rows.
# Set to None to process all rows.
# -----------------------------
MAX_ROWS_TO_PROCESS = None  # None = process all rows.

# -----------------------------
# SPEED SETTINGS (Option B)
# -----------------------------
MAX_WORKERS = 3  # Increase carefully; you may hit API rate limits.

# -----------------------------
# GOOGLE VERDICT STRICTNESS
# If True: accept more Google suggestions (more rescues, higher risk of bad suggestions)
# If False: keep conservative gating
# -----------------------------
AGGRESSIVE_GOOGLE_VALIDATOR = True

# -----------------------------
# ENDPOINTS
# -----------------------------
PROPERTY_SEARCH_URL = "https://api.datafiniti.co/v4/properties/search"
BUSINESS_SEARCH_URL = "https://api.datafiniti.co/v4/businesses/search"
GOOGLE_ADDRVAL_URL = "https://addressvalidation.googleapis.com/v1:validateAddress"

# -----------------------------
# DATAFINITI REQUEST HEADERS
# -----------------------------
REQUEST_HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}

# -----------------------------
# BAD NORMALIZED TOKEN MAP
# Reverse of Datafiniti normalized terms (Normalized → Full word)
# -----------------------------
BAD_NORMALIZED_TOKEN_MAP = {
    "ALY": "ALLEY",
    "ANX": "ANNEX",
    "ARC": "ARCADE",
    "AVE": "AVENUE",
    "BYU": "BAYOU",
    "BCH": "BEACH",
    "BND": "BEND",
    "BLF": "BLUFF",
    "BLVD": "BOULEVARD",
    "BR": "BRANCH",
    "BRG": "BRIDGE",
    "BRK": "BROOK",
    "BYP": "BYPASS",
    "CP": "CAMP",
    "CYN": "CANYON",
    "CPE": "CAPE",
    "CSWY": "CAUSEWAY",
    "CTR": "CENTER",
    "CIR": "CIRCLE",
    "CLF": "CLIFF",
    "CLB": "CLUB",
    "CMN": "COMMON",
    "COR": "CORNER",
    "CRSE": "COURSE",
    "CT": "COURT",
    "CV": "COVE",
    "CRK": "CREEK",
    "CRES": "CRESCENT",
    "DR": "DRIVE",
    "EXPY": "EXPRESSWAY",
    "EXT": "EXTENSION",
    "FLD": "FIELD",
    "FLDS": "FIELDS",
    "FLT": "FLAT",
    "FRD": "FORD",
    "FRG": "FORGE",
    "FRK": "FORK",
    "FWY": "FREEWAY",
    "GDN": "GARDEN",
    "GLN": "GLEN",
    "GRN": "GREEN",
    "GRV": "GROVE",
    "HL": "HILL",
    "HOLW": "HOLLOW",
    "HWY": "HIGHWAY",
    "IS": "ISLAND",
    "JCT": "JUNCTION",
    "KNL": "KNOLL",
    "KY": "KEY",
    "LNDG": "LANDING",
    "LN": "LANE",
    "LGT": "LIGHT",
    "LCK": "LOCK",
    "LDG": "LODGE",
    "LOOP": "LOOP",  # keep as is
    "MALL": "MALL",
    "MDW": "MEADOW",
    "MEWS": "MEWS",
    "MT": "MOUNT",
    "MTN": "MOUNTAIN",
    "NCK": "NECK",
    "ORCH": "ORCHARD",
    "OVAL": "OVAL",
    "PARK": "PARK",
    "PKWY": "PARKWAY",
    "PASS": "PASS",
    "PATH": "PATH",
    "PIKE": "PIKE",
    "PNE": "PINE",
    "PL": "PLACE",
    "PLZ": "PLAZA",
    "PT": "POINT",
    "PR": "PRAIRIE",
    "RADL": "RADIAL",
    "RAMP": "RAMP",
    "RD": "ROAD",
    "RDG": "RIDGE",
    "RIV": "RIVER",
    "RNCH": "RANCH",
    "ROW": "ROW",
    "RUN": "RUN",
    "SQ": "SQUARE",
    "STA": "STATION",
    "STRA": "STRAVENUE",
    "ST": "STREET",
    "TER": "TERRACE",
    "TRCE": "TRACE",
    "TRL": "TRAIL",
    "TUNL": "TUNNEL",
    "TPKE": "TURNPIKE",
    "UN": "UNION",
    "VLY": "VALLEY",
    "VW": "VIEW",
    "VLG": "VILLAGE",
    "VIS": "VISTA",
    "WALK": "WALK",
    "WAY": "WAY",
    "WL": "WELL",
    "XING": "CROSSING",
}

FULL_WORD_TO_NORMALIZED = {v.upper(): k.upper() for k, v in BAD_NORMALIZED_TOKEN_MAP.items()}
VALID_NORMALIZED_SET = set(BAD_NORMALIZED_TOKEN_MAP.keys())

DIRECTIONAL_MAP = {"NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W"}
MULTI_DIRECTIONAL_MAP = {
    "NORTH EAST": "NE",
    "NORTH WEST": "NW",
    "SOUTH EAST": "SE",
    "SOUTH WEST": "SW",
}

# -----------------------------
# REQUIRED OUTPUT HEADERS
# -----------------------------
PROPERTY_CSV_HEADERS = [
    "input_full_address",
    "id",
    "absenteeOwner",
    "address",
    "apiURLs",
    "appliances",
    "architecturalStyles",
    "assessedValues",
    "brokers",
    "buildingName",
    "categories",
    "cbsaName",
    "cbsaCode",
    "censusBlock",
    "censusBlockGroup",
    "censusTract",
    "city",
    "civilDivisionCode",
    "civilDivisionName",
    "companies",
    "congressionalDistrictHouse",
    "country",
    "county",
    "countyFIPS",
    "currentOwnerType",
    "dateAdded",
    "dateUpdated",
    "deposits",
    "descriptions",
    "estimatedPrices",
    "exteriorConstruction",
    "exteriorFeatures",
    "features",
    "fees",
    "floorPlans",
    "floorSizeValue",
    "floorSizeUnit",
    "geoLocation",
    "geoQuality",
    "hvacTypes",
    "instrumentNumber",
    "involuntaryLienJudgement",
    "isUnit",
    "languagesSpoken",
    "latitude",
    "leasingTerms",
    "legalDescription",
    "legalRange",
    "listingName",
    "longitude",
    "lotSizeValue",
    "lotSizeUnit",
    "managedBy",
    "mostRecentBrokerAgent",
    "mostRecentBrokerCompany",
    "mostRecentBrokerEmails",
    "mostRecentBrokerPhones",
    "mostRecentBrokerDateSeen",
    "mostRecentPriceAmount",
    "mostRecentPriceDomain",
    "mostRecentPriceSourceURL",
    "mostRecentPriceDate",
    "mostRecentPriceFirstDateSeen",
    "mostRecentRentalPriceAmount",
    "mostRecentRentalPricePeriod",
    "mostRecentRentalPriceDomain",
    "mostRecentRentalPriceSourceURL",
    "mostRecentRentalPriceDate",
    "mostRecentRentalPriceFirstDateSeen",
    "mostRecentEstimatedPriceAmount",
    "mostRecentEstimatedPriceDomain",
    "mostRecentEstimatedPriceSourceURL",
    "mostRecentEstimatedPriceDate",
    "mostRecentEstimatedPriceFirstDateSeen",
    "mostRecentStatus",
    "mostRecentStatusDate",
    "mostRecentStatusFirstDateSeen",
    "mostRecentVacancy",
    "mostRecentVacancyFirstDateSeen",
    "mostRecentAbsenteeOwner",
    "mostRecentAbsenteeOwnerFirstDateSeen",
    "mostRecentInvoluntaryJudgement",
    "mostRecentInvoluntaryLien",
    "mostRecentInvoluntaryLienJudgementFirstDateSeen",
    "mlsName",
    "mlsID",
    "mlsNumber",
    "msaName",
    "msaCode",
    "neighborhoods",
    "numBathroom",
    "numBedroom",
    "numFloor",
    "numParkingSpaces",
    "numPeople",
    "numRoom",
    "numUnit",
    "ownership",
    "owner",
    "ownerOccupied",
    "ownerOccupiedStatus",
    "parcelNumbers",
    "parking",
    "parkingTypes",
    "paymentTypes",
    "people",
    "petPolicy",
    "permits",
    "phones",
    "postalCode",
    "prices",
    "propertyTaxes",
    "propertyType",
    "province",
    "reviews",
    "roofing",
    "rules",
    "subdivision",
    "statuses",
    "taxExemptions",
    "taxID",
    "title",
    "topographyCode",
    "transactions",
    "trustDescription",
    "vacancy",
    "yearBuilt",
    "zoning",
]

BUSINESS_CSV_HEADERS = [
    "input_full_address",
    "id",
    "address",
    "categories",
    "city",
    "claimed",
    "country",
    "cuisines",
    "dateAdded",
    "dateUpdated",
    "descriptions",
    "domains",
    "emails",
    "facebookPageURL",
    "faxes",
    "features",
    "geoLocation",
    "hours",
    "imageURLs",
    "isClosed",
    "keys",
    "languagesSpoken",
    "latitude",
    "longitude",
    "menuPageURL",
    "menus",
    "name",
    "neighborhoods",
    "numEmployeesMin",
    "numEmployeesMax",
    "numRoom",
    "paymentTypes",
    "people",
    "phones",
    "postalCode",
    "priceRangeCurrency",
    "priceRangeMin",
    "priceRangeMax",
    "primaryCategories",
    "productsOrServices",
    "province",
    "revenueCurrency",
    "revenueMin",
    "revenueMax",
    "reviews",
    "rooms",
    "sic",
    "sourceURLs",
    "twitter",
    "websites",
    "yearIncorporated",
    "yearOpened",
]

# -----------------------------
# THREAD-LOCAL SESSIONS (requests.Session is not safely shared across threads)
# -----------------------------
_thread_local = threading.local()


def _get_df_session() -> requests.Session:
    sess = getattr(_thread_local, "df_sess", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update(REQUEST_HEADERS)
        _thread_local.df_sess = sess
    return sess


def _get_google_session() -> requests.Session:
    sess = getattr(_thread_local, "g_sess", None)
    if sess is None:
        sess = requests.Session()
        _thread_local.g_sess = sess
    return sess


# -----------------------------
# HELPERS
# -----------------------------
def to_csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def extract_zip5(postal: str) -> str:
    if not postal:
        return ""
    m = re.search(r"\b(\d{5})\b", str(postal))
    return m.group(1) if m else ""


def strip_unit_for_validation(address: str) -> str:
    return re.sub(r"\bUNIT\s+\S+\b", "", address, flags=re.IGNORECASE).strip()


def extract_unit_suffix(address: str) -> str:
    m = re.search(r"\b(UNIT\s+\S+)\b", address, flags=re.IGNORECASE)
    return m.group(1) if m else ""


def validate_address_normalization(address: str):
    if not address:
        return False, []
    issues = []
    check_addr = strip_unit_for_validation(address)
    tokens = check_addr.split()
    if not tokens:
        return False, []

    for t in tokens:
        if t.upper() in DIRECTIONAL_MAP:
            issues.append(f"{t} should be {DIRECTIONAL_MAP[t.upper()]}")

    last = tokens[-1].upper()
    if last in FULL_WORD_TO_NORMALIZED:
        issues.append(f"{last} should be {FULL_WORD_TO_NORMALIZED[last]}")
    elif last not in VALID_NORMALIZED_SET:
        issues.append(f"{last} not valid normalized street type")

    return len(issues) > 0, issues


def normalize_directionals_in_street(address: str) -> str:
    if not address:
        return address

    unit_suffix = extract_unit_suffix(address)
    base = strip_unit_for_validation(address)
    tokens = base.upper().split()
    if not tokens:
        return address

    out = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens):
            two_word = f"{tokens[i]} {tokens[i + 1]}"
            if two_word in MULTI_DIRECTIONAL_MAP:
                out.append(MULTI_DIRECTIONAL_MAP[two_word])
                i += 2
                continue

        if tokens[i] in DIRECTIONAL_MAP:
            out.append(DIRECTIONAL_MAP[tokens[i]])
        else:
            out.append(tokens[i])
        i += 1

    rebuilt = " ".join(out)
    if unit_suffix:
        rebuilt = f"{rebuilt} {unit_suffix}"
    return rebuilt


def normalize_last_token_to_map(address: str) -> str:
    if not address:
        return address

    unit_suffix = extract_unit_suffix(address)
    base = strip_unit_for_validation(address)
    tokens = base.split()
    if not tokens:
        return address

    last = tokens[-1].upper()
    if last in FULL_WORD_TO_NORMALIZED:
        tokens[-1] = FULL_WORD_TO_NORMALIZED[last]

    rebuilt = " ".join(tokens)
    if unit_suffix:
        rebuilt = f"{rebuilt} {unit_suffix}"
    return rebuilt


def _sleep_polite():
    if SLEEP_SECONDS_BETWEEN_CALLS and SLEEP_SECONDS_BETWEEN_CALLS > 0:
        time.sleep(SLEEP_SECONDS_BETWEEN_CALLS)


def _request_with_retries(method_fn, max_tries=4, base_sleep=0.5):
    """
    Retries transient HTTP errors (429, 5xx, 408) with exponential backoff.
    method_fn must return a requests.Response.
    """
    last_exc = None
    for attempt in range(1, max_tries + 1):
        try:
            resp = method_fn()
            if resp.status_code in (408, 429) or (500 <= resp.status_code <= 599):
                if attempt < max_tries:
                    time.sleep(base_sleep * (2 ** (attempt - 1)))
                    continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if attempt < max_tries:
                time.sleep(base_sleep * (2 ** (attempt - 1)))
                continue
            raise
    raise last_exc  # should never hit


def search_api(url: str, query: str, num_records: int):
    payload = {
        "query": query,
        "format": "JSON",
        "num_records": num_records,
        "download": False,
    }
    try:
        sess = _get_df_session()

        def do_post():
            return sess.post(url, json=payload, timeout=120)

        r = _request_with_retries(do_post, max_tries=4, base_sleep=0.5)
        data = r.json()
        num_found = int(data.get("num_found", 0))
        records = data.get("records", []) or []
        return num_found, records, ""
    except Exception as e:
        return 0, [], str(e)


def google_validate_address(full_address: str, region_code: str = "US", enable_usps_cass: bool = True):
    if not full_address:
        return "", "", "", ""

    region_code = (region_code or "US").strip().upper()
    payload = {"address": {"addressLines": [full_address], "regionCode": region_code}}
    if enable_usps_cass and region_code in ("US", "PR"):
        payload["enableUspsCass"] = True

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-Goog-Api-Key": GOOGLE_ADDRVAL_API_KEY,
    }

    try:
        sess = _get_google_session()

        def do_post():
            return sess.post(GOOGLE_ADDRVAL_URL, headers=headers, json=payload, timeout=30)

        r = _request_with_retries(do_post, max_tries=4, base_sleep=0.5)
        data = r.json()

        result = data.get("result", {}) or {}
        verdict = result.get("verdict", {}) or {}
        address_complete = bool(verdict.get("addressComplete"))
        has_unconfirmed = bool(verdict.get("hasUnconfirmedComponents"))

        postal = (((result.get("address") or {}).get("postalAddress")) or {})
        address_lines = postal.get("addressLines") or []
        street_line = address_lines[0].strip() if address_lines else ""
        city = (postal.get("locality") or postal.get("postalTown") or "").strip()
        state = (postal.get("administrativeArea") or "").strip()
        postcode = (postal.get("postalCode") or "").strip()

        if not street_line:
            return "", "", "", ""

        # Guardrail to avoid city-only suggestions: require a digit in the street line.
        # If you want to be *very* aggressive, you can remove this digit check too.
        if not re.search(r"\d", street_line):
            return "", "", "", ""

        # STRICT MODE (conservative): reject when Google says the address is incomplete AND has unconfirmed parts.
        # AGGRESSIVE MODE: accept anyway (more rescues, but higher chance of wrong/partial standardization).
        if (not AGGRESSIVE_GOOGLE_VALIDATOR) and (not address_complete) and has_unconfirmed:
            return "", "", "", ""

        return street_line, city, state, postcode
    except Exception:
        return "", "", "", ""


def build_query(address: str, zip5: str) -> str:
    if zip5:
        return f'address:"{address}" AND postalCode:"{zip5}"*'
    return f'address:"{address}"'


def record_to_row_with_headers(record: dict, headers: list[str], input_full_address: str) -> dict:
    row = {}
    for h in headers:
        if h == "input_full_address":
            row[h] = input_full_address
        else:
            row[h] = to_csv_cell(record.get(h))
    return row


def blank_row_with_headers(headers: list[str], input_full_address: str) -> dict:
    row = {}
    for h in headers:
        row[h] = input_full_address if h == "input_full_address" else ""
    return row


def build_input_full_address(row: dict) -> str:
    for col in ("full_address", "original_address_used", "asurian_address", "input_full_address"):
        v = (row.get(col) or "").strip()
        if v:
            return v

    parts = [
        (row.get("address") or "").strip(),
        (row.get("city") or "").strip(),
        (row.get("province") or row.get("state") or "").strip(),
        (row.get("postalCode") or row.get("zip") or "").strip(),
        (row.get("country") or "").strip(),
    ]
    parts = [p for p in parts if p]
    return ", ".join(parts)


def _process_one(index: int, total_rows: int, row: dict):
    """
    Worker function: does API calls + optional Google fallback.
    Returns (index, summary_row_dict, property_row_dict, business_row_dict, logs:list, counters:dict)
    """
    logs = []
    counters = {
        "valid_norm": 0,
        "invalid_norm": 0,
        "google_rescues": 0,
        "property_only": 0,
        "business_only": 0,
        "both_found": 0,
        "neither_found": 0,
    }

    address = (row.get("address") or "").strip()
    raw_postal = row.get("postalCode") or ""
    zip5 = extract_zip5(raw_postal)
    input_full_address = build_input_full_address(row)

    has_issues, issues = validate_address_normalization(address)
    if has_issues:
        counters["invalid_norm"] += 1
        logs.append(f"[{index}/{total_rows}] NORMALIZATION ISSUE: {issues}")
    else:
        counters["valid_norm"] += 1

    query_sent = build_query(address, zip5)
    logs.append(f"[{index}/{total_rows}] QUERY: {query_sent}")

    # ---- Timed Datafiniti PROPERTY call
    t0 = time.perf_counter()
    prop_num_found, prop_records, _ = search_api(PROPERTY_SEARCH_URL, query_sent, NUM_RECORDS_TO_FETCH)
    t1 = time.perf_counter()
    # logs.append(f"[{index}/{total_rows}] DF PROPERTY time: {(t1 - t0)*1000:.1f} ms")
    _sleep_polite()

    # ---- Timed Datafiniti BUSINESS call
    t0 = time.perf_counter()
    biz_num_found, biz_records, _ = search_api(BUSINESS_SEARCH_URL, query_sent, NUM_RECORDS_TO_FETCH)
    t1 = time.perf_counter()
    # logs.append(f"[{index}/{total_rows}] DF BUSINESS time: {(t1 - t0)*1000:.1f} ms")

    validator_address_used = ""

    if prop_num_found == 0 and biz_num_found == 0:
        country = (row.get("country") or "").strip().upper() or "US"
        region_code = country if len(country) == 2 else "US"
        full_addr = f"{address}, {row.get('city','')}, {row.get('province','')}, {raw_postal}, {country}"
        full_addr = re.sub(r"\s+", " ", full_addr).strip().strip(",")

        g_street, g_city, g_state, g_zip = google_validate_address(
            full_addr,
            region_code=region_code,
            enable_usps_cass=True,
        )
        if g_street:
            g_street_dir = normalize_directionals_in_street(g_street)
            g_street_norm = normalize_last_token_to_map(g_street_dir)
            validator_address_used = f"{g_street_norm}, {g_city}, {g_state}, {g_zip}"

            geo_zip5 = zip5 or extract_zip5(g_zip)
            query_sent = build_query(g_street_norm, geo_zip5)

            logs.append(f"[{index}/{total_rows}] GOOGLE VALIDATOR RERUN QUERY: {query_sent}")
            _sleep_polite()

            if prop_num_found > 0 or biz_num_found > 0:
                counters["google_rescues"] += 1

    property_found = prop_num_found > 0
    business_found = biz_num_found > 0

    if property_found and business_found:
        found_by = "both"
        counters["both_found"] += 1
    elif property_found:
        found_by = "property"
        counters["property_only"] += 1
    elif business_found:
        found_by = "business"
        counters["business_only"] += 1
    else:
        found_by = "neither"
        counters["neither_found"] += 1

    summary_row = {
        "input_address": address,
        "input_postalCode_raw": str(raw_postal),
        "query_sent": query_sent,
        "Geoapify rescues": validator_address_used,  # kept for compatibility
        "property_num_found": prop_num_found,
        "business_num_found": biz_num_found,
        "property_found": "true" if property_found else "false",
        "business_found": "true" if business_found else "false",
        "found_by": found_by,
    }

    # Always 1 output row per input row
    if property_found and prop_records:
        prop_row = record_to_row_with_headers(prop_records[0], PROPERTY_CSV_HEADERS, input_full_address)
    else:
        prop_row = blank_row_with_headers(PROPERTY_CSV_HEADERS, input_full_address)

    if business_found and biz_records:
        biz_row = record_to_row_with_headers(biz_records[0], BUSINESS_CSV_HEADERS, input_full_address)
    else:
        biz_row = blank_row_with_headers(BUSINESS_CSV_HEADERS, input_full_address)

    return index, summary_row, prop_row, biz_row, logs, counters


# -----------------------------
# MAIN
# -----------------------------
def main():
    # First pass: count rows for nicer progress. (fast)
    total_rows_in_file = 0
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for _ in reader:
            total_rows_in_file += 1

    total_rows = total_rows_in_file
    if MAX_ROWS_TO_PROCESS is not None:
        total_rows = min(total_rows_in_file, MAX_ROWS_TO_PROCESS)

    print(f"Processing {total_rows} rows with {MAX_WORKERS} workers")
    if MAX_ROWS_TO_PROCESS is not None:
        print(f"ROW LIMIT ACTIVE → First {MAX_ROWS_TO_PROCESS} rows only")
    else:
        print("ROW LIMIT OFF → Processing full file")

    # Summary counters aggregated
    valid_norm = 0
    invalid_norm = 0
    google_rescues = 0
    property_only = 0
    business_only = 0
    both_found = 0
    neither_found = 0

    summary_headers = [
        "input_address",
        "input_postalCode_raw",
        "query_sent",
        "Geoapify rescues",
        "property_num_found",
        "business_num_found",
        "property_found",
        "business_found",
        "found_by",
    ]

    # We keep results in-memory only as long as needed to write in-order
    pending = {}
    next_to_write = 1

    with (
        open(SUMMARY_OUT_CSV, "w", newline="", encoding="utf-8") as sum_f,
        open(PROPERTY_FOUND_OUT_CSV, "w", newline="", encoding="utf-8") as prop_f,
        open(BUSINESS_FOUND_OUT_CSV, "w", newline="", encoding="utf-8") as biz_f,
        ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool,
    ):
        sum_w = csv.DictWriter(sum_f, fieldnames=summary_headers)
        prop_w = csv.DictWriter(prop_f, fieldnames=PROPERTY_CSV_HEADERS)
        biz_w = csv.DictWriter(biz_f, fieldnames=BUSINESS_CSV_HEADERS)

        sum_w.writeheader()
        prop_w.writeheader()
        biz_w.writeheader()

        futures = []

        # Submit tasks
        with open(INPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, start=1):
                if MAX_ROWS_TO_PROCESS is not None and idx > MAX_ROWS_TO_PROCESS:
                    break
                futures.append(pool.submit(_process_one, idx, total_rows, row))

        # Collect and write in input order
        for fut in as_completed(futures):
            idx, summary_row, prop_row, biz_row, logs, counters = fut.result()

            # Print logs from worker (keeps output but may be out-of-order)
            for line in logs:
                print(line)

            # Aggregate counters
            valid_norm += counters["valid_norm"]
            invalid_norm += counters["invalid_norm"]
            google_rescues += counters["google_rescues"]
            property_only += counters["property_only"]
            business_only += counters["business_only"]
            both_found += counters["both_found"]
            neither_found += counters["neither_found"]

            pending[idx] = (summary_row, prop_row, biz_row)

            # Write any now-contiguous results in order
            while next_to_write in pending:
                srow, prow, brow = pending.pop(next_to_write)
                sum_w.writerow(srow)
                prop_w.writerow(prow)
                biz_w.writerow(brow)
                next_to_write += 1

    print("\n==== SUMMARY ====")
    print(f"Rows processed: {total_rows}")
    print(f"Valid normalization: {valid_norm}")
    print(f"Invalid normalization: {invalid_norm}")
    print(f"Google validator rescues: {google_rescues}")
    print(f"Property only: {property_only}")
    print(f"Business only: {business_only}")
    print(f"Both: {both_found}")
    print(f"Neither: {neither_found}")

    print("\nWrote:")
    print(" -", SUMMARY_OUT_CSV)
    print(" -", PROPERTY_FOUND_OUT_CSV)
    print(" -", BUSINESS_FOUND_OUT_CSV)
    print("\nDONE")


if __name__ == "__main__":
    main()