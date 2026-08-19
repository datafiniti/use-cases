#!/usr/bin/env python3
"""
Scan every CSV starting with "Houston_people_output" in a directory,
count how many times each unique `id` appears across all files, and write
a deduplicated CSV with id, businessName, firstName, lastName, and
represented property count.

Usage:
    python count_properties.py

Input files are read from the same directory as this script (any CSV whose
name starts with "Houston_people_output"). The result is written to
houston_people_counts.csv in that same directory.

Set the row limit via the ROW_LIMIT global near the top of this file.
ROW_LIMIT of 0 (or None) processes every row found; a positive number caps
the TOTAL data rows processed across all files combined.
"""

import csv
import glob
import os
import sys
from collections import defaultdict

# Allow large fields (datafiniti rows can be very wide)
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

FILE_PREFIX = "Houston_people_output"
WANTED = ("id", "businessName", "firstName", "lastName")

# ---------------------------------------------------------------------------
# CONFIG: set the row limit here.
#   0 (or None) = process every row found.
#   Any positive number caps the TOTAL data rows processed across all files.
# ---------------------------------------------------------------------------
ROW_LIMIT = 1000


def main():
    # Input files are assumed to live in the same directory as this script.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = script_dir
    output_file = os.path.join(script_dir, "houston_people_counts.csv")

    # row_limit comes from the ROW_LIMIT global at the top of this file.
    # 0 or None = no limit (process everything).
    row_limit = ROW_LIMIT if ROW_LIMIT else 0
    if row_limit < 0:
        row_limit = 0
    unlimited = row_limit == 0

    pattern = os.path.join(input_dir, FILE_PREFIX + "*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No files matching {pattern!r} found.", file=sys.stderr)
        sys.exit(1)

    counts = defaultdict(int)     # id -> occurrence count
    info = {}                     # id -> (businessName, firstName, lastName)
    rows_processed = 0

    if unlimited:
        print("Row limit: none (processing all rows).", file=sys.stderr)
    else:
        print(f"Row limit: {row_limit:,} total rows.", file=sys.stderr)

    for path in files:
        if not unlimited and rows_processed >= row_limit:
            break
        print(f"Reading {path} ...", file=sys.stderr)
        # newline="" + utf-8-sig handles quoted fields and a BOM if present
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                continue  # empty file

            # Map wanted column names to their index in this file's header
            idx = {name: header.index(name) for name in WANTED if name in header}
            if "id" not in idx:
                print(f"  WARNING: no 'id' column in {path}, skipping.", file=sys.stderr)
                continue

            id_i = idx["id"]
            biz_i = idx.get("businessName")
            fn_i = idx.get("firstName")
            ln_i = idx.get("lastName")
            maxcol = max(i for i in idx.values())

            for row in reader:
                if not unlimited and rows_processed >= row_limit:
                    break
                if len(row) <= maxcol:
                    continue
                rows_processed += 1
                rid = row[id_i]
                if not rid:
                    continue
                counts[rid] += 1
                # Keep first non-empty descriptive info we see for this id
                if rid not in info:
                    info[rid] = (
                        row[biz_i] if biz_i is not None else "",
                        row[fn_i] if fn_i is not None else "",
                        row[ln_i] if ln_i is not None else "",
                    )

    with open(output_file, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["id", "businessName", "firstName", "lastName",
                        "represented property count"])
        for rid, cnt in counts.items():
            biz, fn, ln = info.get(rid, ("", "", ""))
            writer.writerow([rid, biz, fn, ln, cnt])

    print(f"\nDone. {len(counts):,} unique ids across {len(files)} file(s).",
        file=sys.stderr)
    print(f"Rows processed: {rows_processed:,}"
        + ("" if unlimited else f" (limit {row_limit:,})"), file=sys.stderr)
    print(f"Output written to {output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()