# Datafiniti Product Matching Config (`configProductMatching.js`)

This repository includes a reusable configuration file (`configProductMatching.js`) that helps customers match/enrich product records using the **Datafiniti Product Data API**.

The config is designed to turn common product identifiers (GTIN, manufacturer number/MPN, brand, manufacturer) into a strong Datafiniti search query, returning the top candidate matches.

---

## What this config does

Given an input product record (from your catalog, POS, supplier feed, etc.), the config builds a Datafiniti query using:

1. **Primary match:** `(brand OR manufacturer) AND manufacturerNumber`
2. **Fallback match:** `gtins`

The query is built in `advanceQueryManipulation()` and returns up to **5** candidate records.

---

## Requirements

- A **Datafiniti account** with access to **Product Data**
- A valid **Datafiniti API token**
- Node.js 16+ recommended (works on modern Node versions)

---

## Security note (API token)

The sample file currently includes a token inline for convenience. For production usage, you should store your token in an environment variable and reference it in your runtime.

Recommended pattern:

```js
token: process.env.DATAFINITI_TOKEN
