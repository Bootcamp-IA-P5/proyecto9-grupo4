# stitch_pipeline.py
# Python 3.8+
#
# Single-file pipeline to:
# - load five different "format" collections from MongoDB
# - compute fuzzy/deterministic pairwise similarities for the matching rules you described:
#     A <-> B  by fullname-fullname
#     A <-> C  by address-address
#     A <-> D  by fullname-name
#     D <-> E  by passport-passport (exact)
# - greedily assign 1:1 matches and produce merged groups and diagnostics
#
# Edit DB_NAME and COL_A..COL_E below to match your Mongo database/collection names.
# Use the MONGO_URI environment variable to pass the connection string (don't hardcode credentials).

from typing import List, Dict, Tuple, Any
import re
import heapq
import os
from pymongo import MongoClient

# ---------------------------
#  Configuration - change these
# ---------------------------
DB_NAME = "kafka_data"        # Mongo database name (change to your DB)
COL_A = "formatA"             # collection for format A
COL_B = "formatB"             # collection for format B
COL_C = "formatC"             # collection for format C
COL_D = "formatD"             # collection for format D
COL_E = "formatE"             # collection for format E

# Optional quick test: limit how many docs to load for each collection (None = load all)
SAMPLE_LIMIT = 1000  # set to None to load full collection (be careful with memory)

# ---------------------------
#  Fuzzy matching backend
# ---------------------------
# Prefer rapidfuzz (fast & good). If it's not installed, fallback to difflib.
try:
    from rapidfuzz import fuzz
    def token_score(a: str, b: str) -> float:
        """Token-sort ratio from rapidfuzz (0..100)."""
        return fuzz.token_sort_ratio(a or "", b or "")
    def partial_score(a: str, b: str) -> float:
        """Partial ratio from rapidfuzz (0..100)."""
        return fuzz.partial_ratio(a or "", b or "")
    _HAS_RAPIDFUZZ = True
except Exception:
    import difflib
    _HAS_RAPIDFUZZ = False
    def token_score(a: str, b: str) -> float:
        """Fallback approximate ratio scaled to 0..100."""
        if not a and not b: return 100.0
        return difflib.SequenceMatcher(None, a or "", b or "").ratio() * 100.0
    def partial_score(a: str, b: str) -> float:
        if not a and not b: return 100.0
        return difflib.SequenceMatcher(None, a or "", b or "").ratio() * 100.0

# ---------------------------
#  Normalizers
# ---------------------------
def normalize_name(s: str) -> str:
    """Lowercase, remove honorifics and punctuation, collapse spaces."""
    if not s:
        return ""
    s = s.lower().strip()
    # remove honorifics (mr/mrs/ms/dr etc)
    s = re.sub(r'\b(mr|mrs|ms|dr|sr|sra|srta)\.?\b', ' ', s)
    # remove non alnum (but keep spaces)
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_address(s: str) -> str:
    """Lowercase, normalize common street tokens, remove punctuation."""
    if not s:
        return ""
    s = s.lower().strip()
    # replace common street tokens with a space (so "av." = removed, but number remains)
    s = re.sub(r'\b(av|ave|avenida|boulevard|boulv|blvd|st|street|calle|circunvalaci[oó]n)\b', ' ', s)
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_passport(s: str) -> str:
    """Uppercase passport and remove whitespace for deterministic exact compare."""
    if s is None:
        return ""
    return re.sub(r'\s+', '', str(s)).upper()

# ---------------------------
#  Similarity functions
# ---------------------------
def name_similarity(a: str, b: str) -> float:
    return token_score(normalize_name(a), normalize_name(b))

def address_similarity(a: str, b: str) -> float:
    na, nb = normalize_address(a), normalize_address(b)
    if not na or not nb:
        return 0.0
    # combine token-based similarity and partial match to handle numbers and abbreviations
    return 0.6 * token_score(na, nb) + 0.4 * partial_score(na, nb)

def passport_similarity(a: str, b: str) -> float:
    na, nb = normalize_passport(a), normalize_passport(b)
    if not na or not nb:
        return 0.0
    return 100.0 if na == nb else 0.0

# ---------------------------
#  Greedy one-to-one matcher
# ---------------------------
def greedy_one_to_one_match(left_items: List[Dict], right_items: List[Dict],
                            score_fn, left_id_field='id', right_id_field='id',
                            min_score=50.0) -> List[Tuple[Any, Any, float]]:
    """
    Build all candidate pairs with score >= min_score,
    then greedily pick highest-scoring pairs ensuring one-to-one assignment.
    Returns list of tuples (left_id, right_id, score).
    """
    pairs = []
    # compute scores for all pairs (consider blocking here for large lists)
    for L in left_items:
        for R in right_items:
            sc = score_fn(L, R)
            if sc >= min_score:
                # push negative score so heapq gives us max-first
                pairs.append((-sc, L[left_id_field], R[right_id_field], sc))
    heapq.heapify(pairs)

    matched_left = set()
    matched_right = set()
    matches = []
    # pop highest score, keep it if both ids not already used
    while pairs:
        neg, lid, rid, sc = heapq.heappop(pairs)
        if lid in matched_left or rid in matched_right:
            continue
        matched_left.add(lid)
        matched_right.add(rid)
        matches.append((lid, rid, sc))
    return matches

# Helper functions to produce score functions that operate on dicts
def make_name_score_fn(field_left: str, field_right: str):
    def fn(L, R):
        return name_similarity(L.get(field_left, ''), R.get(field_right, ''))
    return fn

def make_address_score_fn(field_left: str, field_right: str):
    def fn(L, R):
        return address_similarity(L.get(field_left, ''), R.get(field_right, ''))
    return fn

def make_passport_score_fn(field_left: str, field_right: str):
    def fn(L, R):
        return passport_similarity(L.get(field_left, ''), R.get(field_right, ''))
    return fn

# ---------------------------
#  Core stitching function
# ---------------------------
def stitch_five_formats(A: List[Dict], B: List[Dict], C: List[Dict], D: List[Dict], E: List[Dict],
                       thresholds=None) -> Tuple[List[Dict], Dict]:
    """
    Given 5 lists of records (A,B,C,D,E) returns:
        - merged: list of person groups anchored on A (each group contains A,B,C,D,E or None)
        - diagnostics: dictionary with counts and unmatched ids
    thresholds: dict with similarity thresholds for each step
    """
    if thresholds is None:
        thresholds = {
            'AB_name': 70.0,   # A.fullname <-> B.fullname
            'AC_addr': 65.0,   # A.address  <-> C.address
            'AD_name': 65.0,   # A.fullname <-> D.name
            'DE_passport': 100.0  # D.passport <-> E.passport (exact)
        }

    # compute matches for each pair of formats using the appropriate score function
    name_ab_fn = make_name_score_fn('fullname', 'fullname')
    matches_ab = greedy_one_to_one_match(A, B, name_ab_fn, min_score=thresholds['AB_name'])

    addr_ac_fn = make_address_score_fn('address', 'address')
    matches_ac = greedy_one_to_one_match(A, C, addr_ac_fn, min_score=thresholds['AC_addr'])

    name_ad_fn = make_name_score_fn('fullname', 'name')
    matches_ad = greedy_one_to_one_match(A, D, name_ad_fn, min_score=thresholds['AD_name'])

    pass_de_fn = make_passport_score_fn('passport', 'passport')
    matches_de = greedy_one_to_one_match(D, E, pass_de_fn, min_score=thresholds['DE_passport'])

    # Create quick lookup maps and score maps
    map_ab = {a: b for (a, b, s) in matches_ab}
    score_ab = {(a, b): s for (a, b, s) in matches_ab}

    map_ac = {a: c for (a, c, s) in matches_ac}
    score_ac = {(a, c): s for (a, c, s) in matches_ac}

    map_ad = {a: d for (a, d, s) in matches_ad}
    score_ad = {(a, d): s for (a, d, s) in matches_ad}

    map_de = {d: e for (d, e, s) in matches_de}
    score_de = {(d, e): s for (d, e, s) in matches_de}

    # Diagnostics
    diagnostics = {
        'counts': {
            'A': len(A), 'B': len(B), 'C': len(C), 'D': len(D), 'E': len(E),
            'matched_AB': len(matches_ab), 'matched_AC': len(matches_ac),
            'matched_AD': len(matches_ad), 'matched_DE': len(matches_de)
        },
        'unmatched': {'A': [], 'B': [], 'C': [], 'D': [], 'E': []}
    }

    # Index records by id for fast lookup
    idxA = {r['id']: r for r in A}
    idxB = {r['id']: r for r in B}
    idxC = {r['id']: r for r in C}
    idxD = {r['id']: r for r in D}
    idxE = {r['id']: r for r in E}

    usedB = set()
    usedC = set()
    usedD = set()
    usedE = set()

    merged = []
    # for each A anchor, assemble its group
    for a_rec in A:
        aid = a_rec['id']
        person = {
            'person_anchor_A': aid,
            'A': a_rec,
            'B': None,
            'C': None,
            'D': None,
            'E': None,
            'scores': {},
            'needs_review': False,
            'notes': []
        }

        # attach matching B (by A-B name)
        if aid in map_ab:
            bid = map_ab[aid]
            person['B'] = idxB.get(bid)
            person['scores']['A-B_name'] = score_ab[(aid, bid)]
            usedB.add(bid)
            # mark for review if score close to threshold (tunable)
            if person['scores']['A-B_name'] < thresholds['AB_name'] + 5:
                person['needs_review'] = True
        else:
            person['notes'].append('no_B_match')
            person['needs_review'] = True

        # attach matching C (by A-C address)
        if aid in map_ac:
            cid = map_ac[aid]
            person['C'] = idxC.get(cid)
            person['scores']['A-C_address'] = score_ac[(aid, cid)]
            usedC.add(cid)
            if person['scores']['A-C_address'] < thresholds['AC_addr'] + 5:
                person['needs_review'] = True
        else:
            person['notes'].append('no_C_match')
            person['needs_review'] = True

        # attach matching D (by A-D name)
        if aid in map_ad:
            did = map_ad[aid]
            person['D'] = idxD.get(did)
            person['scores']['A-D_name'] = score_ad[(aid, did)]
            usedD.add(did)
            if person['scores']['A-D_name'] < thresholds['AD_name'] + 5:
                person['needs_review'] = True

            # via D try to attach E (passport)
            if did in map_de:
                eid = map_de[did]
                person['E'] = idxE.get(eid)
                person['scores']['D-E_passport'] = score_de[(did, eid)]
                usedE.add(eid)
                if person['scores']['D-E_passport'] < thresholds['DE_passport']:
                    person['needs_review'] = True
            else:
                person['notes'].append('no_E_for_D')
                person['needs_review'] = True

        else:
            person['notes'].append('no_D_match')
            person['needs_review'] = True

        merged.append(person)

    # report unmatched ids for B/C/D/E
    diagnostics['unmatched']['B'] = [bid for bid in idxB.keys() if bid not in usedB]
    diagnostics['unmatched']['C'] = [cid for cid in idxC.keys() if cid not in usedC]
    diagnostics['unmatched']['D'] = [did for did in idxD.keys() if did not in usedD]
    diagnostics['unmatched']['E'] = [eid for eid in idxE.keys() if eid not in usedE]

    return merged, diagnostics

# ---------------------------
#  Helper: load collection from Mongo safely
# ---------------------------
def load_collection(db, coll_name: str, sample_limit=None) -> List[Dict]:
    """
    Load documents from `db[coll_name]` and ensure each document has an 'id' field
    (if it doesn't, we create one from the Mongo _id as a string).
    Use sample_limit (int) to limit the number of documents for quick testing.
    """
    coll = db[coll_name]
    cursor = coll.find({}, projection=None)
    if sample_limit:
        cursor = cursor.limit(sample_limit)
    docs = []
    for doc in cursor:
        if 'id' not in doc:
            doc['id'] = str(doc.get('_id'))
        docs.append(doc)
    print(f"Loaded {len(docs)} documents from collection '{coll_name}'")
    return docs

# ---------------------------
#  Main entrypoint
# ---------------------------
if __name__ == "__main__":
    # Ensure MONGO_URI is in environment variables (don't hardcode credentials)
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise SystemExit("ERROR: set MONGO_URI environment variable to your connection string and re-run.")

    # connect to Mongo
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print("Connected to MongoDB. Collections available:", db.list_collection_names())

    # load collections (use SAMPLE_LIMIT for quick debugging)
    A = load_collection(db, COL_A, sample_limit=SAMPLE_LIMIT)
    B = load_collection(db, COL_B, sample_limit=SAMPLE_LIMIT)
    C = load_collection(db, COL_C, sample_limit=SAMPLE_LIMIT)
    D = load_collection(db, COL_D, sample_limit=SAMPLE_LIMIT)
    E = load_collection(db, COL_E, sample_limit=SAMPLE_LIMIT)

    # Run the stitcher
    merged, diagnostics = stitch_five_formats(A, B, C, D, E)

    # Output results summary
    print("\n--- RUN SUMMARY ---")
    print("Using rapidfuzz:", _HAS_RAPIDFUZZ)
    print("Total anchors (A):", len(A))
    print("Total merged groups:", len(merged))
    print("Diagnostics (counts):", diagnostics['counts'])
    print("Sample merged group (first 3):")
    from pprint import pprint
    pprint(merged[:3])
    print("\nUnmatched IDs preview (up to 10 each):")
    for k in ['B', 'C', 'D', 'E']:
        print(f"{k} unmatched (count {len(diagnostics['unmatched'][k])}):",
              diagnostics['unmatched'][k][:10])
