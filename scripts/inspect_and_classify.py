# inspect_and_classify.py
# Run this from the same folder where stitch_pipeline.py is (with MONGO_URI set)
# It will:
#  - open probando_messages
#  - sample up to SAMPLE_LIMIT documents
#  - classify each doc into format A/B/C/D/E using simple heuristics
#  - print counts and 3 example docs per bucket (with keys only to avoid huge dumps)

import os
from pymongo import MongoClient
from pprint import pprint

# config
DB_NAME = "kafka_data"
RAW_COLL = "probando_messages"
SAMPLE_LIMIT = 500  # increase if you want to scan more

# helper: heuristics to detect format
def detect_format(doc):
    # doc is a dict from Mongo
    keys = set(k.lower() for k in doc.keys())

    # Heuristics based on your earlier descriptions:
    # Format A -> has 'fullname' and 'address' and maybe 'city'
    if 'fullname' in keys and 'address' in keys:
        return 'A'
    # Format B -> has 'fullname' and company fields
    if 'fullname' in keys and ('company' in keys or 'company address' in keys or 'company_telfnumber' in keys or 'company email' in keys):
        return 'B'
    # Format C -> primarily address-only fragments (no fullname)
    if 'address' in keys and 'fullname' not in keys and 'name' not in keys:
        return 'C'
    # Format D -> has 'name' and 'passport' (or name + last_name)
    if 'passport' in keys and ('name' in keys or 'last_name' in keys):
        return 'D'
    # Format E -> passport + IBAN or salary fields (final fragment)
    if 'iban' in keys or 'salary' in keys:
        return 'E'
    # fallback: look for 'name' without passport => maybe D-ish
    if 'name' in keys and 'passport' not in keys:
        return 'D'
    return 'unknown'

def short_keys(doc, n=10):
    """return a small dict with keys and first n chars of string fields to preview safely"""
    preview = {}
    for k,v in doc.items():
        if k == '_id':
            preview[k] = str(v)
            continue
        if isinstance(v, str):
            preview[k] = v[:n] + ('...' if len(v) > n else '')
        else:
            preview[k] = type(v).__name__
    return preview

def main():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise SystemExit("set MONGO_URI environment variable before running this script")

    client = MongoClient(uri)
    db = client[DB_NAME]
    if RAW_COLL not in db.list_collection_names():
        raise SystemExit(f"Collection '{RAW_COLL}' not found in database '{DB_NAME}'. Available: {db.list_collection_names()}")

    coll = db[RAW_COLL]
    cursor = coll.find({}, {}).limit(SAMPLE_LIMIT)

    buckets = {'A':[], 'B':[], 'C':[], 'D':[], 'E':[], 'unknown':[]}
    counts = {'A':0,'B':0,'C':0,'D':0,'E':0,'unknown':0}

    total = 0
    for doc in cursor:
        total += 1
        fmt = detect_format(doc)
        counts[fmt] += 1
        if len(buckets[fmt]) < 3:
            buckets[fmt].append(short_keys(doc))
    print(f"Scanned {total} documents from '{RAW_COLL}' (limit {SAMPLE_LIMIT}).")
    print("Classification counts:")
    pprint(counts)
    print("\nExample previews (up to 3 per class):")
    for k in ['A','B','C','D','E','unknown']:
        print(f"\n--- {k} ({counts[k]} examples shown {len(buckets[k])}) ---")
        pprint(buckets[k])

if __name__ == '__main__':
    main()
