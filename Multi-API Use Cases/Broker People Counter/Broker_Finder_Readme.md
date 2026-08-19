# Broker → People → Property Count Workflow

A two-step pipeline for turning raw Datafiniti Property Data into a
deduplicated list of the people (brokers) tied to those properties, along with
a count of how many properties each one represents.

This use case answers a common question: **given a batch of property records,
who are the active brokers, and how many properties is each one associated
with?**

The workflow uses two Datafiniti APIs together:

1. **Property Data** — the source records you start with (as a JSON-Lines file).
2. **People Data** — enriches each qualifying broker with a full person record.

---

## Overview

| Step | Script | Input | Output |
| --- | --- | --- | --- |
| 1 | `FindBrokersInPeopleData.py` | A JSON-Lines property file | `property_output_<date>.csv` and `people_output_<date>.csv` |
| 2 | `BrokerCount.py` | One or more `people_output` CSVs | `houston_people_counts.csv` |

Step 1 filters property records by broker activity date, then looks up each
broker in the People Data API and writes matching people records to CSV. Step 2
scans those people CSVs and counts how often each unique person appears,
producing a deduplicated summary.

You run Step 1 first (often several times, across different property batches),
then run Step 2 once over all the resulting people CSVs.

---

## Requirements

- Python 3.7+
- The `requests` library (Step 1 only):

  ```bash
  pip install requests
  ```

- A Datafiniti API token with access to the People Data API. See the
  [Datafiniti API docs](https://docs.datafiniti.co/) for how to obtain one.

Both scripts are designed to run from the same directory as their input files.

---

## Step 1 — `FindBrokersInPeopleData.py`

### What it does

For every property record in your input file, the script:

1. Reads the `brokers` field and checks whether **any** broker has a `dateSeen`
   value inside the configured date window.
2. If a broker is in-window **and** carries a `people_key`, the property record
   is written to the property CSV using Datafiniti's Property "Default view"
   column order.
3. For each qualifying `people_key`, it queries the People Data search API with
   `keys:"<people_key>"`. If a match is found, the person record is written to
   the people CSV using the People Data header.

Duplicate `people_key` values are only queried once per run, so you don't spend
API calls (or rows) on the same person twice.

### Input format

A **JSON-Lines** file: one JSON property record per line. Each record is
expected to follow the Datafiniti Property Data schema, including a `brokers`
field. The `brokers` field may be a list of objects, or occasionally a
JSON-encoded string — the script parses both.

### Configuration

Open the script and set the values in the **Config** section near the top:

| Setting | Purpose | Default |
| --- | --- | --- |
| `DATAFINITI_TOKEN` | Your API token. **Replace `"API_KEY_HERE"`** — or leave it and use the `DATAFINITI_TOKEN` env var / `--token` flag instead. | `"API_KEY_HERE"` |
| `INPUT_FILE` | Name of the input JSON-Lines file in the script's folder. **Replace `"input.txt_file_here"`** with your real filename, or pass `--input`. | `"input.txt_file_here"` |
| `DATE_START` / `DATE_END` | The broker `dateSeen` window. A broker must have been seen within this range to qualify. | `2025-08-12` → `2026-08-12` |
| `API_CALL_DELAY` | Seconds to wait between People API calls, to respect rate limits. | `0.1` |
| `MAX_ROWS` | Cap on input rows processed. `0` = process everything. Useful for a small test run first. | `0` |
| `PEOPLE_SEARCH_URL` | People search endpoint. | `https://api.datafiniti.co/v4/people/search` |

> **Security note:** Never commit a real token. Prefer the environment variable:
>
> ```bash
> export DATAFINITI_TOKEN="your_api_token"
> ```
>
> The copy in this repository has been anonymized — the token and input
> filename are placeholders you must fill in.

### Running it

Simplest form (uses the in-file config / env var):

```bash
export DATAFINITI_TOKEN="your_api_token"
python FindBrokersInPeopleData.py --input /path/to/properties.jsonl
```

Common flags:

| Flag | Description |
| --- | --- |
| `--input` | Path to the JSON-Lines input file. |
| `--property-out` | Property CSV path (default: `property_output_<date>.csv`). |
| `--people-out` | People CSV path (default: `people_output_<date>.csv`). |
| `--token` | API token (overrides the in-file value and env var). |
| `--max-rows` | Limit input rows processed (overrides `MAX_ROWS`). |
| `--skip-people-api` | Build only the property CSV; make no People API calls. |
| `--log-out` | Append a run summary to this log file. |

**Test run first.** Before processing a large file, try a small slice to
confirm your token and input work and to eyeball the output:

```bash
python FindBrokersInPeopleData.py --input properties.jsonl --max-rows 25
```

Or validate the filtering without spending any API calls:

```bash
python FindBrokersInPeopleData.py --input properties.jsonl --skip-people-api
```

### Output

- **`property_output_<date>.csv`** — qualifying property records, in Property
  "Default view" column order.
- **`people_output_<date>.csv`** — matched people records, in People Data
  header order. **This is the file Step 2 consumes.**
- **`run_log_<date>.log`** — a summary (records read, properties exported,
  unique keys queried, people exported).

Output files are **appended** to, and the header is only written when the file
is new or empty. This means repeated runs on the same day accumulate into the
same dated files rather than overwriting them.

> **Naming for Step 2:** `BrokerCount.py` looks for files that start with a
> specific prefix (`Houston_people_output` by default). Either name your
> `--people-out` accordingly (e.g. `Houston_people_output_2026-08-12.csv`) or
> adjust the prefix in Step 2 — see below.

---

## Step 2 — `BrokerCount.py`

### What it does

Scans every CSV in its own directory whose name starts with a given prefix,
counts how many times each unique `id` appears **across all of those files
combined**, and writes one deduplicated row per `id` with the total as a new
`represented property count` column.

Because a person's `id` appears once per property they're tied to, that count
is effectively the number of properties each person represents in your data.

### How it reads the files

- It streams each CSV row by row (using Python's built-in `csv` module), so it
  never loads an entire file into memory and handles very large exports
  comfortably.
- It locates columns by **header name** (`id`, `businessName`, `firstName`,
  `lastName`) rather than by position, so it stays correct even if column order
  differs between files.
- For the descriptive fields, it keeps the first non-empty values seen for each
  `id`, so the output has no duplicate rows.

### Configuration

Set these near the top of the script:

| Setting | Purpose | Default |
| --- | --- | --- |
| `FILE_PREFIX` | Only files whose name starts with this are processed. | `"Houston_people_output"` |
| `WANTED` | The columns to pull. | `id, businessName, firstName, lastName` |
| `ROW_LIMIT` | Cap on **total** rows processed across all files. `0` (or `None`) = process everything. | `1000` |

> **Heads up:** the version in this repo ships with `ROW_LIMIT = 1000`, which
> is a small test cap. **Set `ROW_LIMIT = 0` to process your full dataset.**

### Running it

Place the script in the same folder as your `people_output` CSVs and run it
with no arguments:

```bash
python BrokerCount.py
```

It reads every `Houston_people_output*.csv` beside it and writes
`houston_people_counts.csv` to that same folder. Because it resolves paths
relative to its own location, it works no matter which directory you launch it
from.

### Output

`houston_people_counts.csv` with these columns:

| Column | Description |
| --- | --- |
| `id` | The unique person/entity id. |
| `businessName` | Business name (first non-empty value seen). |
| `firstName` | First name (first non-empty value seen). |
| `lastName` | Last name (first non-empty value seen). |
| `represented property count` | How many times this `id` appeared across all input files. |

Progress and a final summary (files read, unique ids, rows processed) are
printed to `stderr`, so they show in your terminal without polluting the CSV.

---

## End-to-end example

```bash
# 0. One-time setup
pip install requests
export DATAFINITI_TOKEN="your_api_token"

# 1. Turn property records into a people CSV (name it to match Step 2's prefix)
python FindBrokersInPeopleData.py \
    --input houston_properties.jsonl \
    --people-out Houston_people_output_2026-08-12.csv

#    ...repeat for as many property batches as you have, keeping the
#    Houston_people_output prefix on each people CSV.

# 2. Count how many properties each person represents, across all people CSVs
#    (remember to set ROW_LIMIT = 0 in BrokerCount.py first)
python BrokerCount.py

# Result: houston_people_counts.csv
```

---

## Notes & troubleshooting

- **"No files matching ... found."** `BrokerCount.py` didn't see any CSV with
  the expected prefix in its folder. Check `FILE_PREFIX` and that the people
  CSVs sit beside the script.
- **Only 1000 rows counted.** `ROW_LIMIT` is still at its default test value in
  `BrokerCount.py`. Set it to `0`.
- **`ERROR: no API token.`** Step 1 needs a token unless you pass
  `--skip-people-api`. Set `DATAFINITI_TOKEN`, pass `--token`, or edit the
  in-file value.
- **Rate limiting (HTTP 429).** Step 1 backs off and retries automatically;
  raise `API_CALL_DELAY` if you hit limits often.
- **Date window.** Only brokers whose `dateSeen` falls within
  `DATE_START`–`DATE_END` qualify. Adjust those constants to widen or shift the
  window.

For more Datafiniti examples, see the
[use-cases repository](https://github.com/datafiniti/use-cases).
