"""
This script performs a multi-stage data consolidation process on a MongoDB database.
It reads raw data, identifies duplicates based on different criteria (passport, address, name),
and merges them into a final 'golden' collection. The process follows a
Bronze-Silver-Golden architecture:
- BRONZE: Initial aggregation and merging based on less reliable keys like address and fullname.
- SILVER: Further consolidation of BRONZE data and passport-based aggregation from raw data.
- GOLDEN: The final, cleanest dataset, created by normalizing and merging records from SILVER.
"""
from src.database.write_to_mongodb import connect_to_mongodb
from scripts.read_from_kafka import MONGO_DATABASE, MONGO_COLLECTION

from src.core.logger import Logger

BRONZE="bronze"
SILVER="silver"
GOLDEN="golden"

log = Logger()

def aggregate_by_passport(input_collection,output):
    """
    Aggregates documents by 'passport', merges duplicates, and outputs to a new collection.

    This pipeline first unnests the 'data' field, then groups documents by the
    'passport' number. It only processes groups with more than one document (i.e., duplicates),
    merges them into a single record, and writes the result to the specified
    output collection.

    Args:
        input_collection (pymongo.collection.Collection): The source collection.
        output (str): The name of the target collection for the merged documents.
    """
    pipeline = [
        {'$replaceRoot': {'newRoot': {'$ifNull': ['$data', {}]}}},
        {'$group': {'_id': '$passport', 'documents': {'$push': '$$ROOT'}, 'record_count': {'$sum': 1}}},
        {'$match': {'record_count': {'$gt': 1}}},
        {'$project': {'_id': '$_id', 'merged': {'$mergeObjects': '$documents'}}},
        {'$replaceRoot': {'newRoot': '$merged'}},
        {'$merge': {'into': output, 'on': '_id', 'whenMatched': 'replace', 'whenNotMatched': 'insert'}}
    ]
    input_collection.aggregate(pipeline)

def aggregate_by_address(input_collection, output):
    """
    Aggregates documents by 'address', merges them, and outputs to a new collection.

    This pipeline first unnests the 'data' field, then groups documents by the
    'address' field. All documents sharing the same address are merged into a
    single record.

    Args:
        input_collection (pymongo.collection.Collection): The source collection.
        output (str): The name of the target collection for the merged documents.
    """
    pipeline = [
        {'$replaceRoot': {'newRoot': {'$ifNull': ['$data', {}]}}},
        {'$group': {'_id': '$address', 'documents': {'$push': '$$ROOT'}}},
        {'$project': {'_id': 0, 'merged': {'$mergeObjects': '$documents'}}},
        {'$replaceRoot': {'newRoot': '$merged'}},
        {'$merge': {'into': output, 'on': '_id', 'whenMatched': 'replace', 'whenNotMatched': 'insert'}}
    ]
    input_collection.aggregate(pipeline)

def aggregate_by_address_no_replacing_root(input_collection, output):
    """
    Aggregates documents by 'address' without an initial $replaceRoot stage.

    This is designed to run on collections (like BRONZE) where the data is
    already at the top level. It groups by 'address', merges the documents,
    and writes them to the output collection.

    Args:
        input_collection (pymongo.collection.Collection): The source collection.
        output (str): The name of the target collection for the merged documents.
    """
    pipeline = [
        {'$group': {'_id': '$address', 'documents': {'$push': '$$ROOT'}}},
        {'$project': {'_id': 0, 'merged': {'$mergeObjects': '$documents'}}},
        {'$replaceRoot': {'newRoot': '$merged'}},
        {'$merge': {'into': output, 'on': '_id', 'whenMatched': 'replace', 'whenNotMatched': 'insert'}}
    ]
    input_collection.aggregate(pipeline)

def aggregate_by_fullname(input_collection, output):
    """
    Aggregates documents by 'fullname', merges them, and outputs to a new collection.

    This pipeline first unnests the 'data' field, then groups documents by the
    'fullname' field. All documents sharing the same fullname are merged into a
    single record.

    Args:
        input_collection (pymongo.collection.Collection): The source collection.
        output (str): The name of the target collection for the merged documents.
    """
    pipeline = [
        {'$replaceRoot': {'newRoot': {'$ifNull': ['$data', {}]}}},
        {'$group': {'_id': '$fullname', 'documents': {'$push': '$$ROOT'}}},
        {'$project': {'_id': 0, 'merged': {'$mergeObjects': '$documents'}}},
        {'$replaceRoot': {'newRoot': '$merged'}},
        {'$merge': {'into': output, 'on': '_id', 'whenMatched': 'replace', 'whenNotMatched': 'insert'}}
    ]
    input_collection.aggregate(pipeline)

def aggregate_by_name_last_name_fullname(input_collection, output):
    """
    Aggregates documents by a normalized full name and outputs to a new collection.

    This pipeline creates a 'normalized_name' field by concatenating 'name' and
    'last_name' if they exist, otherwise falling back to 'fullname'. It then
    groups documents by this normalized name, merges them into a single record,
    and writes the result to the final output collection. This is the last step
    in creating the GOLDEN dataset.

    Args:
        input_collection (pymongo.collection.Collection): The source collection (typically SILVER).
        output (str): The name of the target collection for the final merged documents (GOLDEN).
    """
    pipeline = [
        {'$addFields': {'normalized_name': {'$trim': {'input': {'$cond': [{'$and': ['$name', '$last_name']}, {'$concat': ['$name', ' ', '$last_name']}, '$fullname']}}}}},
        {'$group': {'_id': '$normalized_name', 'documents': {'$push': '$$ROOT'}}},
        {'$project': {'_id': 0, 'merged': {'$mergeObjects': '$documents'}}},
        {'$replaceRoot': {'newRoot': '$merged'}},
        {'$merge': {'into': output, 'on': '_id', 'whenMatched': 'replace', 'whenNotMatched': 'insert'}}
    ]
    input_collection.aggregate(pipeline)

def mongo_consolidate():
    """
    Orchestrates the entire multi-stage data consolidation process.

    Connects to MongoDB and executes a series of aggregation pipelines to
    clean, deduplicate, and merge data. It follows a Bronze -> Silver -> Golden
    flow, where each stage produces a more refined dataset. Finally, it logs
    the total number of records in the final GOLDEN collection and cleans up
    the intermediate BRONZE and SILVER collections.
    """
    client, raw_data_collection = connect_to_mongodb(database=MONGO_DATABASE, collection_name=MONGO_COLLECTION)
    client[MONGO_DATABASE][GOLDEN].drop()
    aggregate_by_passport(raw_data_collection, SILVER)
    aggregate_by_address(raw_data_collection, BRONZE)
    aggregate_by_fullname(raw_data_collection, BRONZE)
    aggregate_by_address_no_replacing_root(client[MONGO_DATABASE][BRONZE], SILVER)
    aggregate_by_name_last_name_fullname(client[MONGO_DATABASE][SILVER], GOLDEN)

    golden_collection = client[MONGO_DATABASE][GOLDEN]
    record_count = golden_collection.count_documents({})
    log.info(f"✅ Total records in GOLDEN collection: {record_count}")

    client[MONGO_DATABASE][BRONZE].drop()
    client[MONGO_DATABASE][SILVER].drop()
    

if __name__ == "__main__":
    mongo_consolidate()
