
# Datafiniti Property Matcher (CSV → Property API → JSON)

This script allows you to **match a list of addresses from a CSV file** against the  
**Datafiniti Property Data API** and export all matched property records to a JSON file.

It is designed for customers who:

- Have a list of normalized or semi-normalized addresses  
- Want to quickly identify matching Datafiniti property records  
- Need a simple batch workflow for enrichment, coverage checks, or QA  

The script performs one API query per row and aggregates all returned records into a single JSON output.

---

## 🔎 What This Script Does

For each row in your CSV:

1. Reads:
   - `address`
   - `postalCode`
2. Builds a Datafiniti query:

```
address:"<address>" AND postalCode:<postalCode>*
```

1. Sends the query to:

```
POST https://api.datafiniti.co/v4/properties/search
```

1. Collects:
   - All returned property records  
   - Total match count across the batch  
2. Writes results to a JSON file  

---

## 📁 Input Requirements

Your CSV **must** contain the following columns:

| Column Name | Required | Description |
|-------------|----------|-------------|

`address` | ✅ | Full street address (no city/state required)
`postalCode` | ✅ | ZIP/Postal code (wildcard search is applied)

### Example

```csv
address,postalCode
123 Main St,78701
5505 Bonneville BND,84001
```

---

## 📤 Output

### JSON Output File

Contains **all matched property records** returned by the API.

Each record is a full Datafiniti Property object.

### Console Summary

Displays:

- Total rows processed  
- Total property records found  

---

## ⚙️ Configuration

Update the following variables in the script:

```python
API_token = 'API_KEY_HERE'

csv_file = r"C:\path\to\your\input.csv"
json_file = r"C:\path\to\your\output.json"

num_records = 1
download = False
```

### Parameter Notes

| Parameter | Description |
|-----------|-------------|

`num_records` | Max records returned per address query
`download` | Set to `True` to enable Datafiniti download mode (counts toward downloads)  

---

## 🔐 Authentication

You must supply a valid **Datafiniti API token**:

```python
request_headers = {
    'Authorization': f'Bearer {API_token}',
    'Content-Type': 'application/json',
}
```

Get your token from:

👉 <https://portal.datafiniti.co/settings/api>

---

## ▶️ How to Run

```bash
python propertyMatcher.py
```

You will see:

- Each row printed as it processes  
- A running total of records found  
- A final summary  

---

## 📊 Example Query Generated

Input row:

```csv
address = 123 Main St
postalCode = 78701
```

Query sent to Datafiniti:

address:"123 Main St" AND postalCode:78701*

---

## 🚧 Error Handling

If a request fails, the script will append an error object to the output JSON:

```json
{
  "error": "Request failed for address: 123 Main St, postalCode: 78701"
}
```

---

## 📈 Common Use Cases

This workflow is commonly used for:

- Property coverage analysis
- Bulk record matching  
- Pre-enrichment validation  
- Comparing external address lists to Datafiniti inventory  

---

## ⚠️ Performance Notes

This is a **sequential script**.

For large datasets (10k+ rows), consider:

- Adding multithreading  
- Increasing `num_records` only when needed  
- Running in download mode for full exports  

---

## 🧩 Related Datafiniti Resources

- Property Data API: <https://docs.datafiniti.co/docs/property-data-api>  
- Query Syntax Guide: <https://docs.datafiniti.co/docs/constructing-property-queries>  
- Normalized Address Standards: <https://docs.datafiniti.co/docs/normalized-address-data>  

---

## 📝 Script Reference

This README accompanies:

`propertyMatcher.py`

---

## 💬 Support

If you have questions about:

- Improving match rates  
- Address normalization  
- Bulk downloads  
- Adding People or Business enrichment  

Contact:

**<support@datafiniti.co>**

or reach out to your Customer Success Manager.

---

## 🏁 Summary

This script provides a simple, transparent way to:

- Batch match addresses  
- Retrieve Datafiniti property records  
- Measure coverage quickly  

It is intended as a **starter workflow** that you can extend with:

- Multithreading  
- Autocomplete fallback  
- Normalization pipelines  
- CSV → CSV enrichment outputs
