# PMDC Scraper (Registration + Qualification Pipeline)

## Preview

Example output (`pmdc_licenses.csv`):

| RegistrationNo | Name | Status |
|----------------|------|--------|
| 12345-P        | Dr X | Active |

## Overview

This project extracts healthcare practitioner data from the Pakistan Medical and Dental Council (PMDC) public API.

It builds a controlled scraping pipeline to ensure full record coverage, retry handling, and resumable execution.

---

## What this scraper does

- Calls PMDC endpoints:
  - /GetData → search practitioners
  - /GetQualifications → fetch detailed data
- Enumerates records using prefix expansion (A → AA → AAA)
- Stores unique Registration Numbers in SQLite
- Fetches doctor + qualification data in a second phase
- Tracks progress using a JSON state file
- Supports retrying failed qualification records

---

## How it works

### 1. Prefix enumeration

The API limits results per query, so this scraper:

- Starts with prefixes: A → Z
- If results exceed threshold (~20,000), it splits:
  - A → AA → AB → ...
- Continues recursively until all records are covered

All Registration Numbers are stored in: `seen.sqlite`

---

### 2. Data extraction

For each Registration Number:

- Calls /GetQualifications
- Extracts:
  - Doctor profile
  - Qualifications list
- Outputs:
  - `pmdc_licenses.csv`
  - `pmdc_qualifications.csv`

---

### 3. Resume support

Progress is saved in: `state.json`

Phases:
- `enumerate_prefixes`
- `fetch_qualifications`
- `done`

The script resumes automatically if interrupted.

---

### 4. Retry missing data

Some records may fail or return empty data.

Run:

```bash
python src/retry_quals.py retry_missing_quals.csv
```

---

## Visual Pipeline

```text
PMDC /GetData
   ↓
Prefix enumeration (A → Z → AA → AAA)
   ↓
Collect Registration Numbers
   ↓
Store in SQLite (seen.sqlite)
   ↓
PMDC /GetQualifications
   ↓
Extract doctor + qualifications
   ├──> pmdc_licenses.csv
   └──> pmdc_qualifications.csv

State:
state.json → enumerate → fetch → done

Recovery:
retry_missing_quals.csv → retry_quals.py
```

---

## Project structure

```text
pmdc-scraper/
├── src/
│   ├── pmdc_accuracy_scraper.py
│   └── retry_quals.py
├── sample_data/
├── outputs/
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Run

```bash
python src/pmdc_accuracy_scraper.py
```

---

## Output

- `pmdc_licenses.csv`
- `pmdc_qualifications.csv`
- `prefix_audit.csv`

---

## Metrics

### Example run (local)

- Total prefixes processed: 1,898
- Total successful prefixes: 1,826
- Total prefixes split: 72
- Total new registration numbers collected: 385,611
- Output files generated:
  - `pmdc_licenses.csv`
  - `pmdc_qualifications.csv`
- Pipeline supports resume via `state.json`

### Pipeline characteristics

- Split threshold: ~20,000 records per prefix
- Prefix expansion depth: up to 3 levels (`A → AA → AAA`)
- Deduplication: SQLite (`seen.sqlite`)
- Recovery: `retry_quals.py` for missing records

---

## Why this approach

PMDC API limitations:
- Large result sets per query
- No full dataset endpoint
- Inconsistent responses
- Missing qualification data

Solutions:
- Prefix splitting for full coverage
- SQLite tracking to avoid duplicates
- Resume logic for long runs
- Retry flow for failed records

---

## Author

**Dulick**  
Data Operations Coordinator — Veeva  
Healthcare data (Vietnam, Pakistan, Singapore)
