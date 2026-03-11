# Datafiniti Property Owner Contact Checker

This Python script analyzes **Datafiniti property owner `peopleKey` values** and checks the **Datafiniti People Data API** to determine whether the associated people records contain **emails**, **phone numbers**, or both.

The script assumes that the `peopleKey` values originate from **Datafiniti property owner records**, so a matching person record should normally exist in the People Data API.

The primary goal of this script is to **measure contact coverage** for property owner records.

---

# Overview

The script performs the following workflow:

1. Reads a CSV file containing property owner information and `peopleKey` values.
2. Queries the **Datafiniti People Data API** using the `keys` field.
3. Retrieves the matching People Data record.
4. Checks whether the record contains:
   - email addresses
   - phone numbers
5. Categorizes the results into:
   - email only
   - phone only
   - email and phone
   - no contact information
6. Writes results into multiple output files for auditing and analysis.

---

# Why This Script Exists

In many workflows using **Datafiniti Property Data**, property owners are linked to people records through a `peopleKey`.

However, not every people record contains usable contact information.

This script helps answer questions like:

- Which property owners have **emails available**?
- Which have **phone numbers available**?
- Which have **both email and phone**?
- Which people records exist but **lack contact information**?

This is useful for:

- enrichment audits
- skip tracing workflows
- marketing outreach preparation
- data coverage analysis

---

# Datafiniti APIs Used

This script uses the **People Data API**.

API endpoint used:

<https://api.datafiniti.co/v4/people/search>

Query format used:

keys:"<peopleKey>"

Example request body:

```json
{
  "query": "keys:\"abc123xyz\"",
  "num_records": 1
}

More information:

People Data API documentation
https://docs.datafiniti.co/docs/api-people-data

Requirements

Python 3.8+

Required Python packages:

requests

Install with:

pip install requests
Input File Format

The script expects a CSV file containing property owner information.

Required fields typically include:

Field Description
id Property record ID
address Property street address
city Property city
postalCode Property ZIP / postal code
owners.firstName Owner first name
owners.lastName Owner last name
owners.peopleKey Datafiniti people key

Example input:

id,address,city,postalCode,owners.firstName,owners.lastName,owners.peopleKey
12345,123 Main St,Austin,78701,John,Doe,abc123xyz
Configuration

At the top of the script you will find several configuration variables.

Example:

API_TOKEN = "YOUR_DATAFINITI_API_TOKEN"

INPUT_CSV = "input_properties.csv"

OUTPUT_DETAIL_CSV = "people_lookup_details.csv"
OUTPUT_WITH_CONTACT_CSV = "people_with_contact.csv"
OUTPUT_SUMMARY_TXT = "contact_summary.txt"

THREADS = 10

START_ROW = 0
MAX_ROWS = None
Explanation
Setting Purpose
API_TOKEN Your Datafiniti API token
INPUT_CSV Source CSV file
OUTPUT_DETAIL_CSV Full audit file
OUTPUT_WITH_CONTACT_CSV Rows with contact information
OUTPUT_SUMMARY_TXT Final summary report
THREADS Number of concurrent API requests
START_ROW Allows resuming partial runs
MAX_ROWS Limits rows for testing
Running the Script

Run the script using Python:

python contactInfo_Importer_v1.py

The script will:

Load the CSV file

Process rows using multiple threads

Query the People Data API

Evaluate contact coverage

Write output files

Script Workflow
Step 1 — Read Property Records

The script reads the CSV using:

csv.DictReader

Each row represents a property record with owner information.

Step 2 — Extract the peopleKey

The script attempts to locate the people key using:

owners.peopleKey

or

peopleKey

If no key exists, the row is marked as:

missing_peopleKey
Step 3 — Query the People Data API

The script sends a request using:

keys:"<peopleKey>"

Example:

keys:"abc123xyz"
Step 4 — Parse Contact Information

The script checks two People Data fields:

Field Description
emails List of email addresses
phones / phoneNumbers Phone number fields

The script normalizes these values into lists and removes duplicates.

Step 5 — Classify Contact Type

Each record is categorized into one of the following:

Category Meaning
owners email only email found but no phone
owners phone only phone found but no email
owners email and phone both email and phone found
no contact neither field populated
Step 6 — Write Results

Results are written to multiple files.

Output Files
1. Detail Audit File
OUTPUT_DETAIL_CSV

Contains a record for every processed row.

Columns include:

property information

owner information

peopleKey

People API query

matched person ID

emails found

phones found

response status

response time

2. Contact File
OUTPUT_WITH_CONTACT_CSV

Contains only rows where contact information exists.

Useful for outreach lists.

3. Summary File
OUTPUT_SUMMARY_TXT

Contains totals such as:

Total rows processed
Rows with peopleKey
Rows missing peopleKey
Rows with contact
Rows without contact
Owners email only
Owners phone only
Owners email and phone
Performance Features
Multithreading

The script uses:

ThreadPoolExecutor

This allows multiple People API lookups to run simultaneously.

PeopleKey Caching

If multiple rows contain the same peopleKey, the script stores the first API response and reuses it.

Benefits:

fewer API calls

faster processing

reduced API credit usage

Example Console Output
Row 102 | Property ID: 55421 | peopleKey: abc123xyz | Contact: email and phone | Status: success | Response Time: 0.39s

At completion the script prints summary totals.

Troubleshooting
No Matches Found

If matches are not found:

verify the peopleKey value exists

confirm the API token is valid

ensure the key exists in People Data

API Rate Limits

If you encounter API throttling:

Lower the thread count:

THREADS = 5
Security Note

Do not commit your API token to public repositories.

Instead consider using environment variables:

export DATAFINITI_API_TOKEN=your_token_here
Additional Resources

Datafiniti Documentation

Property Data Schema
https://docs.datafiniti.co/docs/property-data-schema

People Data API
https://docs.datafiniti.co/docs/api-people-data

API Reference
https://docs.datafiniti.co/reference

License

Example scripts provided by Datafiniti are intended for educational and integration purposes when using the Datafiniti API.


---

If you'd like, I can also generate:

- a **cleaner README specifically styled for Datafiniti's GitHub repos**
- a **README with diagrams explaining the data flow**
- or a **README optimized for LLM training / API examples** (which fits your goal of feeding models how to
