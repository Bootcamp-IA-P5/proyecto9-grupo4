#!/usr/bin/env python3
"""
classify_and_split.py

1) Inspect nested 'data' payloads inside collection `probando_messages`.
   - Prints frequency of keys found in the inner `data` dict (top N keys)
   - Prints up to 5 example `data` payloads per detected "shape"

2) Optionally split documents into collections:
   - formatA  (expects 'fullname' and 'address' inside data)
   - formatB  (expects 'fullname' + company fields inside data)
   - formatC  (address-only fragments inside data)
   - formatD  (name +/- passport inside data)
   - formatE  (passport + IBAN or salary inside data)
   The splitting is safe: it inserts copies into new collections; it does NOT delete or modify original docs.

USAGE:
  # from project root, with venv active and MONGO_URI set:
  cd scripts
  python classify_and_split.py      # inspect only (default)
  # To actually write split collections (careful, process SAMPLE_LIMIT docs only by default):
  python classify_and_split.py --save

Notes:
  - This script uses SAMPLE_LIMIT to avoid OOM on dev machines. Set SAMPLE_LIMIT=None to process all docs (risky).
  - Adjust heuristics in `detect_format()` if your field names differ from expectations.
"""

import os
import argparse
import json
from collections import Counter, defaultdict
from pymongo import MongoClient

# ---------- CONFIG ----------
DB_NAME = "kafka_data"
RAW_COLL = "probando_messages"

# Change to None to scan entire collection (be careful with memory)
SAMPLE_LIMIT = 2000

# When --save is passed, docs will be inserted into these collections. Otherwise nothing is written.
OUT_A = "formatA"
OUT_B = "formatB"
OUT_C = "formatC"
OUT_D = "formatD"
OUT_E = "formatE"
OUT_UNKNOWN = "format_unknown"

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set MONGO_URI first")

client = MongoClient(MONGO_URI)
db = client['kafka_data']

source_coll = db['probando_messages']
target_collections = {
    'formatA': db['formatA'],
    'formatB': db['formatB'],
    'formatC': db['formatC'],
    'formatD': db['formatD'],
    'formatE': db['formatE'],
    'format_unknown': db['format_unknown']
}

def classify(doc):
    data = doc.get('data', {})
    keys = set(k.lower() for k in data.keys())
    if 'fullname' in keys and 'address' in keys and 'city' in keys:
        return 'formatA'
    elif 'fullname' in keys and 'company' in keys:
        return 'formatB'
    elif 'address' in keys and 'ipv4' in keys:
        return 'formatC'
    elif 'name' in keys and 'passport' in keys:
        return 'formatD'
    elif 'passport' in keys and 'iban' in keys:
        return 'formatE'
    else:
        return 'format_unknown'

batch_size = 1000
cursor = source_coll.find({})
for i, doc in enumerate(cursor, start=1):
    fmt = classify(doc)
    target_collections[fmt].insert_one(doc)
    if i % batch_size == 0:
        print(f"Processed {i} documents...")

print("Classification complete. Counts:")
for name, coll in target_collections.items():
    print(f"  {name}: {coll.count_documents({})}")
# ---------- Helper: format detection heuristics ----------
def detect_format_from_data(data: dict) -> str:
    """
    Return 'A'..'E' or 'unknown' based on keys present in the nested `data` dict.
    Adjust heuristics here if your actual key names differ.
    """
    if not isinstance(data, dict):
        return 'unknown'
    keys = set(k.lower() for k in data.keys())

    # Format A: has fullname and address (home-person fragment)
    if 'fullname' in keys and 'address' in keys:
        return 'A'

    # Format B: fullname + company info (company fragment)
    # We check for presence of 'company' or 'company address' or 'company_telfnumber' or 'company email'
    company_tokens = {'company', 'company address', 'company_address', 'company_telfnumber', 'company email', 'company_email'}
    if 'fullname' in keys and (keys & company_tokens):
        return 'B'

    # Format E: passport + IBAN or salary (sensitive financial fragment) - prefer E before D if passport + IBAN present
    if 'passport' in keys and ('iban' in keys or 'salary' in keys or 'iban_number' in keys):
        return 'E'

    # Format D: name + passport OR name fields (identity fragment)
    if ('passport' in keys and ('name' in keys or 'last_name' in keys)) or ('name' in keys and 'last_name' in keys):
        return 'D'

    # Format C: address-only fragments (no fullname or name)
    if 'address' in keys and 'fullname' not in keys and 'name' not in keys:
        return 'C'

    # Fallback: if it has 'name' but not passport, assume D-ish
    if 'name' in keys:
        return 'D'

    return 'unknown'

# ---------- Utility: pretty preview for nested data (safe) ----------
def data_preview(data, max_chars=150):
    if data is None:
        return None
    preview = {}
    for k, v in data.items():
        if isinstance(v, str):
            preview[k] = v[:max_chars] + ('...' if len(v) > max_chars else '')
        else:
            preview[k] = type(v).__name__
    return preview

# ---------- Main ----------
def main(save=False, sample_limit=SAMPLE_LIMIT):
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise SystemExit("ERROR: set MONGO_URI environment variable in this shell before running.")

    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    db = client[DB_NAME]
    if RAW_COLL not in db.list_collection_names():
        raise SystemExit(f"ERROR: collection '{RAW_COLL}' not found in DB '{DB_NAME}'. Available: {db.list_collection_names()}")

    coll = db[RAW_COLL]
    cursor = coll.find({}, {}).limit(sample_limit) if sample_limit else coll.find({}, {})

    # Counters & samples
    key_counter = Counter()
    format_counter = Counter()
    samples_by_format = defaultdict(list)

    processed = 0
    for doc in cursor:
        processed += 1

        # the actual payload appears to be nested in `data` according to your preview
        nested = doc.get('data', {})
        # gather observed keys
        if isinstance(nested, dict):
            key_counter.update(k.lower() for k in nested.keys())

        fmt = detect_format_from_data(nested)
        format_counter[fmt] += 1

        # save sample preview (keep small)
        if len(samples_by_format[fmt]) < 5:
            samples_by_format[fmt].append(data_preview(nested))

        # Optionally save/copy to new collection(s)
        if save:
            # decide destination collection
            dest = None
            if fmt == 'A':
                dest = OUT_A
            elif fmt == 'B':
                dest = OUT_B
            elif fmt == 'C':
                dest = OUT_C
            elif fmt == 'D':
                dest = OUT_D
            elif fmt == 'E':
                dest = OUT_E
            else:
                dest = OUT_UNKNOWN

            # Prepare doc copy to insert: keep top-level metadata, and data, and original _id as source_id
            # We store original _id in field 'source_id' to keep provenance.
            copy_doc = {
                'source_id': str(doc.get('_id')),
                'metadata': doc.get('metadata'),
                'data': nested,
                'ingested_at': doc.get('inserted_at')
            }
            try:
                db[dest].insert_one(copy_doc)
            except Exception as e:
                print(f"Warning: failed to insert into {dest}: {e}")

    # Print inspection results
    print(f"\nScanned {processed} documents from '{RAW_COLL}' (sample_limit={sample_limit})")
    print("\nTop keys found inside `data` (top 40):")
    for key, cnt in key_counter.most_common(40):
        print(f"  {key:30s} {cnt}")

    print("\nFormat classification counts (sample):")
    for k, v in format_counter.most_common():
        print(f"  {k:>7s}  {v}")

    print("\n--- Example data previews by detected format (up to 5 each) ---")
    for fmt in ['A','B','C','D','E','unknown']:
        print(f"\nFormat {fmt} (count {format_counter.get(fmt,0)}) -- examples:")
        for ex in samples_by_format.get(fmt, []):
            print(json.dumps(ex, ensure_ascii=False, indent=2))

    if save:
        print("\nSaved classified copies to collections:")
        print(" ", OUT_A, OUT_B, OUT_C, OUT_D, OUT_E, OUT_UNKNOWN)
        print("You can inspect counts now (be patient for large SAMPLE_LIMIT):")
        for name in [OUT_A, OUT_B, OUT_C, OUT_D, OUT_E, OUT_UNKNOWN]:
            try:
                print(f"  {name}: {db[name].count_documents({})}")
            except Exception as e:
                print(f"  {name}: count error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect and optionally split probando_messages into formatA..E")
    parser.add_argument("--save", action="store_true", help="Save documents into formatA..formatE collections (default: don't save).")
    parser.add_argument("--limit", type=int, default=SAMPLE_LIMIT, help="How many docs to scan (default %s)." % (SAMPLE_LIMIT))
    args = parser.parse_args()
    main(save=args.save, sample_limit=args.limit)
