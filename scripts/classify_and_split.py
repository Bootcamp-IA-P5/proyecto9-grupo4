#!/usr/bin/env python3
"""
classify_and_split.py

Inspect nested 'data' payloads inside `probando_messages` and optionally copy (split)
documents into separate collections: formatA..formatE (safe copies, original _id kept
in 'source_id').

Usage examples (from scripts/ with venv active and MONGO_URI set):
  # dry-run inspect 200 docs (default SAMPLE_LIMIT)
  python classify_and_split.py --limit 200

  # save copies of up to 2000 docs into format* collections (no drop)
  python classify_and_split.py --limit 2000 --save

  # drop existing format* collections first, then save (fresh start)
  python classify_and_split.py --save --fresh

  # process entire source collection (risky on dev machine)
  python classify_and_split.py --limit None --save --fresh
"""
import os
import argparse
import json
from collections import Counter, defaultdict
from pymongo import MongoClient

# ---------- CONFIG ----------
DB_NAME = "kafka_data"
RAW_COLL = "probando_messages"

# default sample limit for quick tests; set to None to scan the whole source (use with care)
DEFAULT_SAMPLE_LIMIT = 2000

# output collections (target)
OUT_A = "formatA"
OUT_B = "formatB"
OUT_C = "formatC"
OUT_D = "formatD"
OUT_E = "formatE"
OUT_UNKNOWN = "format_unknown"

# ---------- Format detection heuristics ----------
def detect_format_from_data(data: dict) -> str:
    """
    Return 'A'..'E' or 'unknown' based on keys present in the nested `data` dict.
    Adjust heuristics here if your actual key names differ.
    """
    if not isinstance(data, dict):
        return 'unknown'
    keys = set(k.lower() for k in data.keys())

    # Format A: has fullname and address (+city)
    if 'fullname' in keys and 'address' in keys:
        return 'A'

    # Format B: fullname + company info
    company_tokens = {'company', 'company address', 'company_address', 'company_telfnumber', 'company email', 'company_email'}
    if 'fullname' in keys and (keys & company_tokens):
        return 'B'

    # Format E: passport + IBAN or salary (financial)
    if 'passport' in keys and ('iban' in keys or 'salary' in keys or 'iban_number' in keys):
        return 'E'

    # Format D: passport + name OR name + last_name
    if ('passport' in keys and ('name' in keys or 'last_name' in keys)) or ('name' in keys and 'last_name' in keys):
        return 'D'

    # Format C: address-only fragments (no fullname or name)
    if 'address' in keys and 'fullname' not in keys and 'name' not in keys:
        return 'C'

    # Fallback: if it has 'name' but not passport, treat as D-ish
    if 'name' in keys:
        return 'D'

    return 'unknown'

# ---------- Utility ----------
def data_preview(data, max_chars=150):
    """Make a small preview of nested data for printing examples."""
    if data is None:
        return None
    preview = {}
    for k, v in data.items():
        if isinstance(v, str):
            preview[k] = v[:max_chars] + ('...' if len(v) > max_chars else '')
        else:
            preview[k] = type(v).__name__
    return preview

# ---------- Main logic ----------
def main(save=False, sample_limit=DEFAULT_SAMPLE_LIMIT, fresh=False):
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise SystemExit("ERROR: set MONGO_URI environment variable in this shell before running.")

    client = MongoClient(uri, serverSelectionTimeoutMS=20000)
    db = client[DB_NAME]

    if RAW_COLL not in db.list_collection_names():
        raise SystemExit(f"ERROR: collection '{RAW_COLL}' not found in DB '{DB_NAME}'. Available: {db.list_collection_names()}")

    # prepare target collections
    targets = {
        'A': db[OUT_A],
        'B': db[OUT_B],
        'C': db[OUT_C],
        'D': db[OUT_D],
        'E': db[OUT_E],
        'unknown': db[OUT_UNKNOWN]
    }

    # If fresh requested, drop any existing target collections (clean start)
    if fresh and save:
        print("Dropping existing target collections (fresh start)...")
        for name in targets.values():
            try:
                name.drop()
            except Exception as e:
                print("  drop error:", e)

    # If saving, ensure an index on source_id to speed up skip-checks and make idempotent
    if save:
        for coll in targets.values():
            try:
                coll.create_index('source_id')
            except Exception:
                pass

    # Build the cursor
    src_coll = db[RAW_COLL]
    cursor = src_coll.find({}, {})
    if sample_limit is not None:
        cursor = cursor.limit(sample_limit)

    # counters & samples
    key_counter = Counter()
    format_counter = Counter()
    samples_by_format = defaultdict(list)

    processed = 0
    batch_print = 500

    print("Starting scan. save=%s, sample_limit=%s, fresh=%s" % (save, sample_limit, fresh))
    for doc in cursor:
        processed += 1
        nested = doc.get('data', {}) or {}

        # collect key frequencies
        if isinstance(nested, dict):
            key_counter.update(k.lower() for k in nested.keys())

        # detect format
        fmt = detect_format_from_data(nested)
        format_counter[fmt] += 1

        # save sample preview
        if len(samples_by_format[fmt]) < 5:
            samples_by_format[fmt].append(data_preview(nested))

        # if asked to save, copy into target collection safely
        if save:
            dest_coll = targets.get(fmt, targets['unknown'])

            # prepare copy without original _id; store original _id in 'source_id'
            source_id = str(doc.get('_id'))
            # resume-safety: skip if copy already exists (idempotent)
            if dest_coll.find_one({'source_id': source_id}) is None:
                copy_doc = {
                    'source_id': source_id,
                    'metadata': doc.get('metadata'),
                    'data': nested,
                    'ingested_at': doc.get('inserted_at')
                }
                try:
                    dest_coll.insert_one(copy_doc)
                except Exception as e:
                    print(f"Warning: failed to insert into {dest_coll.name}: {e}")
            # else: already present => skip

        # progress printing
        if processed % batch_print == 0:
            print(f"  Processed {processed} documents...")

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
        print("Counts now in DB (these may be larger if you re-ran without --fresh):")
        for coll_name in [OUT_A, OUT_B, OUT_C, OUT_D, OUT_E, OUT_UNKNOWN]:
            try:
                print(f"  {coll_name}: {db[coll_name].count_documents({})}")
            except Exception as e:
                print(f"  {coll_name}: count error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect and optionally split probando_messages into formatA..E")
    parser.add_argument("--save", action="store_true", help="Save documents into formatA..formatE collections (default: don't save).")
    parser.add_argument("--limit", type=lambda s: None if s.lower() == 'none' else int(s), default=DEFAULT_SAMPLE_LIMIT,
                        help="How many docs to scan (default %s). Use 'None' to scan all." % (DEFAULT_SAMPLE_LIMIT))
    parser.add_argument("--fresh", action="store_true", help="If set with --save, drop existing target collections first.")
    args = parser.parse_args()
    main(save=args.save, sample_limit=args.limit, fresh=args.fresh)
