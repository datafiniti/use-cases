
# Datafiniti Bulk Address Matcher

### Property + Business Matching with Google Address Validation

This script performs **bulk address matching against the Datafiniti APIs**.

For each address in an input CSV file, the script:

1. Searches the **Datafiniti Property Data API**
2. Searches the **Datafiniti Business Data API**
3. If no match is found, attempts a **Google Address Validation rescue**
4. Exports results into three structured CSV files

---

# Features

✔ Property API matching  
✔ Business API matching  
✔ Multithreaded execution  
✔ Google Address Validation fallback  
✔ Address normalization validation  
✔ Directional normalization (N, S, E, W)  
✔ Detailed summary reporting  
✔ One output row per input row  
✔ Retry logic with exponential backoff  

---

# Requirements

Python 3.9+

Required package:

```
requests
```

Install with:

```
pip install requests
```

---

# API Keys Required

You must configure two API keys:

### Datafiniti API Key

Used for Property and Business searches.

<https://app.datafiniti.co>

### Google Address Validation API Key

<https://developers.google.com/maps/documentation/address-validation>

---

# Configuration

Edit the following variables in the script:

```python
API_TOKEN = "Insert_API_Key"
GOOGLE_ADDRVAL_API_KEY = "Insert_API_Key"
```

---

## File Paths

```python
INPUT_CSV = "parsed_addresses_FINAL_fixed_v5.csv"
SUMMARY_OUT_CSV = "address_found_summary.csv"
PROPERTY_FOUND_OUT_CSV = "property_records.csv"
BUSINESS_FOUND_OUT_CSV = "business_records.csv"
```

---

# Optional Settings

## Number of records returned

```
NUM_RECORDS_TO_FETCH = 1
```

## Row limiter

```
MAX_ROWS_TO_PROCESS = None
```

Example:

```
MAX_ROWS_TO_PROCESS = 100
```

---

## Thread speed

```
MAX_WORKERS = 3
```

Recommended safe range:

```
3-10 workers
```

---

## Google validation strictness

```
AGGRESSIVE_GOOGLE_VALIDATOR = True
```

---

# Input CSV Format

Required columns:

| Column | Description |
|------|------|
address | Street address |
postalCode | ZIP or postal code |

Optional:

| Column | Description |
|------|------|
city | City |
province | State/Province |
country | Country |

Example:

```
address,city,province,postalCode,country
5505 Bonneville BND,Park City,UT,84098,US
123 Main St,Austin,TX,78701,US
```

---

# Query Logic

Example query sent to Datafiniti:

```
address:"123 Main St" AND postalCode:"78701"*
```

---

# Google Address Validation Rescue

If both APIs return zero matches, the script sends the address to:

```
Google Address Validation API
```

If Google suggests a corrected address, the script reruns the Datafiniti query.

---

# Output Files

The script produces **three CSV files**.

## 1 Summary

```
address_found_summary.csv
```

Fields include:

- input_address
- input_postalCode_raw
- query_sent
- property_num_found
- business_num_found
- property_found
- business_found
- found_by

---

## 2 Property Records

```
property_records.csv
```

Contains Datafiniti property schema fields.

---

## 3 Business Records

```
business_records.csv
```

Contains Datafiniti business schema fields.

---

# Example Workflow

1. Prepare CSV

```
addresses.csv
```

1. Insert API keys

2. Run script

```
python Busi&PropBulkMatcherV1.py
```

1. Review results

```
address_found_summary.csv
property_records.csv
business_records.csv
```

---

# Performance Notes

The script uses:

```
ThreadPoolExecutor
```

Recommended worker settings:

| Dataset | Workers |
|-------|-------|
<5k rows | 3–5 |
5k–20k rows | 5–8 |
20k+ rows | 8–12 |

---

# Error Handling

Automatic retries for:

- HTTP 429
- HTTP 500+
- timeouts

Uses exponential backoff.

---

# Typical Use Cases

- Property coverage analysis
- Address enrichment
- Business presence detection
- Address normalization QA
- Lead generation

---

# Datafiniti Resources

Property API  
<https://docs.datafiniti.co/docs/property-data-api>

Business API  
<https://docs.datafiniti.co/docs/business-data-api>

Query Syntax  
<https://docs.datafiniti.co/docs/query-syntax>

---

# Support

<support@datafiniti.co>
