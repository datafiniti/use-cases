# 📍 Address Abbreviation & Normalization Toolkit

This repository provides two implementations for **standardizing and abbreviating address data** using a shared mapping of street suffixes, directional terms, and geographic regions.

These tools are designed to improve **data quality, normalization consistency, and query performance**—especially when working with APIs such as Datafiniti.

## 🚀 Overview

This project includes two implementations of the same core concept:

| File | Language | Purpose |
|------|----------|--------|
| AddressAbbreviator.java | Java | General-purpose address abbreviation utility |
| DF_Address_Abbreviator_integration.py | Python | Datafiniti-focused normalization with smarter logic |

Both scripts:

- Normalize street suffixes (e.g., `Street → ST`, `Avenue → AVE`)
- Convert directional terms (e.g., `North → N`)
- Standardize address formatting
- Improve downstream search/query matching

## 🧠 Key Differences

### Java Implementation (`AddressAbbreviator.java`)

- Straightforward **token-based replacement**
- Applies abbreviations to **all matching words**
- Preserves punctuation (commas, spacing)

Ideal for:

- Bulk transformations
- Pre-processing pipelines
- General-purpose applications

### Python Implementation (`DF_Address_Abbreviator_integration.py`)

- **Context-aware normalization**
- Only abbreviates the **most relevant street suffix**
- Handles:

  - `UNIT` logic (avoids breaking unit numbers)
  - Direction normalization
  - Title casing for readability

Ideal for:

- Datafiniti query preparation
- Address validation pipelines
- Higher precision normalization

## 📦 Features

- ✅ Comprehensive abbreviation dictionary (US + Canada + directions)
- ✅ Smart parsing of address components
- ✅ UNIT-aware processing (Python version)
- ✅ Regex-based cleaning and formatting
- ✅ Easily extendable mappings

## 🛠️ Installation

### Java

```bash
javac AddressAbbreviator.java
java AddressAbbreviator
```

### Python

```bash
python DF_Address_Abbreviator_integration.py
```

## 📌 Usage Examples

### Java Example

```java
String address = "123 Main Street, Alberta, Northwest";
String result = AddressAbbreviator.abbreviateAddress(address);
System.out.println(result);
```

**Output:**

```
123 Main ST, AB, NW
```

### Python Example

```python
normalize_address("123 Main Street Unit 5 North")
```

**Output:**

```
123 Main St UNIT 5 N
```

## 🔍 How It Works

### Java Logic

1. Split address into tokens (preserving commas)  
2. Normalize each token (strip punctuation for lookup)  
3. Replace using abbreviation map  
4. Rebuild address string  

### Python Logic

1. Normalize whitespace  
2. Identify street suffix and UNIT positions  
3. Apply rules:
   - Abbreviate only the most relevant suffix  
   - Preserve UNIT structure  
   - Normalize directions  
4. Rebuild formatted address  

## 📊 When to Use Each

| Use Case | Recommended Version |
|----------|-------------------|
| Bulk data processing | Java |
| API query optimization | Python |
| Datafiniti integrations | Python |
| Simple normalization | Java |
| High-accuracy address matching | Python |

## 🔗 Datafiniti Use Case

These tools are especially useful when constructing queries like:

```json
address:"123 Main ST" AND postalCode:"12345*"
```

Proper normalization:

- Improves match rates  
- Reduces duplicate records  
- Ensures consistency across datasets  

## ⚙️ Customization

You can extend the abbreviation dictionaries in both files.

### Java

```java
abbreviations.put("EXPRESSWAY", "EXPY");
```

### Python

```python
abbreviations["EXPRESSWAY"] = "Expy"
```

## ⚠️ Notes & Limitations

- Does not validate addresses against external APIs  
- Assumes English-language address formatting  
- Java version may over-abbreviate in edge cases  
- Python version prioritizes correctness over completeness  

## 🧩 Future Improvements

- Google Address Validation API fallback  
- Geoapify integration  
- ZIP/postal code normalization  
- Multi-country support expansion  
- Batch CSV processing pipeline  

## 👤 Author

Leonard Trahan<br>
Built for **Datafiniti workflows and address normalization pipelines**  
Designed for scalability, accuracy, and API-ready formatting  
