# Datafiniti People Key → Contact Enrichment Script

This script scans a **Datafiniti Property Data export** and uses the  
`mostRecentOwnerPeopleKey` to look up the corresponding **People record**.

If the People record contains **emails or phone numbers**, the script:

✅ Writes a summary row to a summary CSV  
✅ Exports the **full original property row** with appended contact fields  
✅ Counts total email and phone matches  

This enables **skip trace style contact enrichment** using Datafiniti’s
Property → People linkage.

---

## Input File

Example input provided:

📄 `Example_property_data_wPeopleKeys.csv` :contentReference[oaicite:0]{index=0}

This must be a **Property Data export** that includes:

| Required Column |
|-----------------|
| `mostRecentOwnerPeopleKey` |

Optional but recommended:

- `property_firstName` or `owners.firstName`
- `property_lastName` or `owners.lastName`
- `address`, `city`, `province/state`, `postalCode`
- `id` or `property_id`

---

## What the Script Does

For each row:

1. Reads `mostRecentOwnerPeopleKey`
2. Builds a People API query:
