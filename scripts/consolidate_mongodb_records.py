"""
MongoDB Record Consolidation Script

This script reads raw Kafka messages from MongoDB (5 different schemas),
consolidates records belonging to the same individual, and writes them
to a 'golden_records' collection.

The 5 schemas are:
1. Personal data: name, lastname, sex, telfnumber, passport, email
2. Location: fullname, city, address
3. Professional: fullname, company, company_address, company_telfnumber, company_email, job
4. Bank: passport, IBAN, salary
5. Network: address, IPv4

Matching strategy:
- Primary key: passport (schemas 1 & 4)
- Secondary key: fullname (schemas 2 & 3) matched to name + lastname
- Tertiary key: address (schemas 2 & 5)

Golden record structure:
- Uses MongoDB _id field as the unique passport identifier (no separate 'passport' field)
- This eliminates redundancy and leverages MongoDB's built-in indexing on _id
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict
from dotenv import load_dotenv

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.database.write_to_mongodb import connect_to_mongodb, close_connection
from src.core.logger import Logger

load_dotenv()

log = Logger()

# Configuration
RAW_COLLECTION = 'probando_messages'
GOLDEN_COLLECTION = 'golden_records'
DATABASE = 'kafka_data'


class SchemaType:
    """Enumeration of the 5 schema types"""
    PERSONAL = 'personal'
    LOCATION = 'location'
    PROFESSIONAL = 'professional'
    BANK = 'bank'
    NETWORK = 'network'
    UNKNOWN = 'unknown'


def detect_schema_type(data: dict) -> str:
    """
    Detect which of the 5 schemas this record belongs to based on its fields.
    
    Args:
        data: The data dictionary from Kafka message
        
    Returns:
        Schema type as string
    """
    fields = set(data.keys())
    
    # Schema 1: Personal data (has passport, name, lastname, sex, telfnumber, email)
    if 'passport' in fields and 'name' in fields and 'last_name' in fields and 'sex' in fields:
        return SchemaType.PERSONAL
    
    # Schema 4: Bank data (has passport, IBAN, salary)
    if 'passport' in fields and 'IBAN' in fields and 'salary' in fields:
        return SchemaType.BANK
    
    # Schema 2: Location (has fullname, city, address but no company)
    if 'fullname' in fields and 'city' in fields and 'address' in fields and 'company' not in fields:
        return SchemaType.LOCATION
    
    # Schema 3: Professional (has fullname, company, job)
    if 'fullname' in fields and 'company' in fields and 'job' in fields:
        return SchemaType.PROFESSIONAL
    
    # Schema 5: Network (has address and IPv4, but no fullname)
    if 'IPv4' in fields and 'address' in fields and 'fullname' not in fields:
        return SchemaType.NETWORK
    
    log.warning(f"Unknown schema type for data with fields: {fields}")
    return SchemaType.UNKNOWN


def normalize_fullname(fullname: str) -> str:
    """Normalize fullname for matching (lowercase, strip whitespace)"""
    return fullname.strip().lower() if fullname else ""


def consolidate_records(raw_collection, golden_collection, dry_run: bool = False) -> dict:
    """
    Main consolidation logic: groups records by individual and merges schemas.
    
    Args:
        raw_collection: MongoDB collection with raw Kafka messages
        golden_collection: MongoDB collection for consolidated golden records
        dry_run: If True, don't write to golden collection (for testing)
        
    Returns:
        Statistics dictionary with processing info
    """
    log.info("=" * 70)
    log.info("Starting record consolidation process")
    log.info("=" * 70)
    
    # Statistics tracking
    stats = {
        'total_raw_messages': 0,
        'personal_records': 0,
        'location_records': 0,
        'professional_records': 0,
        'bank_records': 0,
        'network_records': 0,
        'unknown_records': 0,
        'golden_records_created': 0,
        'golden_records_updated': 0,
        'unmatched_records': 0,
    }
    
    # Data structures to hold categorized records
    personal_by_passport: Dict[str, dict] = {}
    bank_by_passport: Dict[str, dict] = {}
    location_by_fullname: Dict[str, dict] = {}
    professional_by_fullname: Dict[str, dict] = {}
    network_by_address: Dict[str, dict] = {}
    
    # Also track fullname -> passport mapping for linking
    fullname_to_passport: Dict[str, str] = {}
    
    log.info("\n📥 PHASE 1: Reading and categorizing raw messages...")
    
    # First pass: Read all raw messages and categorize by schema type
    for doc in raw_collection.find():
        stats['total_raw_messages'] += 1
        
        # Extract the actual data from the Kafka message wrapper
        data = doc.get('data', {})
        
        if not data:
            stats['empty_data_count'] = stats.get('empty_data_count', 0) + 1
            continue
        
        schema_type = detect_schema_type(data)
        
        if schema_type == SchemaType.PERSONAL:
            passport = data.get('passport')
            if passport:
                personal_by_passport[passport] = data
                # Create fullname for future matching
                fullname = f"{data.get('name', '')} {data.get('last_name', '')}"
                fullname_to_passport[normalize_fullname(fullname)] = passport
                stats['personal_records'] += 1
            else:
                log.warning(f"Personal record missing passport: {data}")
                
        elif schema_type == SchemaType.BANK:
            passport = data.get('passport')
            if passport:
                bank_by_passport[passport] = data
                stats['bank_records'] += 1
            else:
                log.warning(f"Bank record missing passport: {data}")
                
        elif schema_type == SchemaType.LOCATION:
            fullname = data.get('fullname')
            if fullname:
                location_by_fullname[normalize_fullname(fullname)] = data
                stats['location_records'] += 1
                
        elif schema_type == SchemaType.PROFESSIONAL:
            fullname = data.get('fullname')
            if fullname:
                professional_by_fullname[normalize_fullname(fullname)] = data
                stats['professional_records'] += 1
                
        elif schema_type == SchemaType.NETWORK:
            address = data.get('address')
            if address:
                network_by_address[normalize_fullname(address)] = data
                stats['network_records'] += 1
        else:
            stats['unknown_records'] += 1
    
    log.info(f"✓ Processed {stats['total_raw_messages']} raw messages")
    log.info(f"  - Personal records: {stats['personal_records']}")
    log.info(f"  - Bank records: {stats['bank_records']}")
    log.info(f"  - Location records: {stats['location_records']}")
    log.info(f"  - Professional records: {stats['professional_records']}")
    log.info(f"  - Network records: {stats['network_records']}")
    log.info(f"  - Unknown schema: {stats['unknown_records']}")
    
    log.info("\n🔗 PHASE 2: Consolidating records by individual...")
    
    # Prepare bulk operations for efficient database writes
    from pymongo import UpdateOne
    bulk_operations = []
    batch_size = 1000  # Write in batches of 1000
    batch_count = 0
    
    # Second pass: For each person (by passport), merge all related data
    all_passports = set(personal_by_passport.keys()) | set(bank_by_passport.keys())
    
    for passport in all_passports:
        # Initialize golden record with ALL fields set to None for consistent schema
        # Note: _id field serves as the passport identifier (no separate 'passport' field needed)
        golden_record = {
            '_id': passport,  # Use passport as unique identifier (primary key)
            'consolidated_at': datetime.now().isoformat(),
            'data_sources': [],  # Track which schemas contributed
            # Personal data fields
            'name': None,
            'last_name': None,
            'sex': None,
            'email': None,
            'telfnumber': None,
            # Location data fields
            'fullname': None,
            'city': None,
            'address': None,
            # Network data fields
            'IPv4': None,
            # Bank data fields
            'IBAN': None,
            'salary': None,
            # Professional data fields
            'company': None,
            'company address': None,
            'company_telfnumber': None,
            'company_email': None,
            'job': None,
        }
        
        # Start with personal data (if available)
        if passport in personal_by_passport:
            personal = personal_by_passport[passport]
            golden_record['name'] = personal.get('name')
            golden_record['last_name'] = personal.get('last_name')
            golden_record['sex'] = personal.get('sex')
            golden_record['telfnumber'] = personal.get('telfnumber')
            golden_record['email'] = personal.get('email')
            golden_record['data_sources'].append('personal')
            
            # Create fullname for matching other records
            fullname_normalized = normalize_fullname(
                f"{personal.get('name', '')} {personal.get('last_name', '')}"
            )
        else:
            log.warning(f"No personal data for passport {passport}")
            fullname_normalized = None
        
        # Add bank data
        if passport in bank_by_passport:
            bank = bank_by_passport[passport]
            golden_record['IBAN'] = bank.get('IBAN')
            golden_record['salary'] = bank.get('salary')
            golden_record['data_sources'].append('bank')
        
        # Try to match location data by fullname
        if fullname_normalized and fullname_normalized in location_by_fullname:
            location = location_by_fullname[fullname_normalized]
            golden_record['address'] = location.get('address')
            golden_record['city'] = location.get('city')
            golden_record['fullname'] = location.get('fullname')  # Keep original casing
            golden_record['data_sources'].append('location')
            
            # Try to match network data by address
            address_normalized = normalize_fullname(location.get('address', ''))
            if address_normalized in network_by_address:
                network = network_by_address[address_normalized]
                golden_record['IPv4'] = network.get('IPv4')
                golden_record['data_sources'].append('network')
        
        # Try to match professional data by fullname
        if fullname_normalized and fullname_normalized in professional_by_fullname:
            professional = professional_by_fullname[fullname_normalized]
            golden_record['company'] = professional.get('company')
            golden_record['company address'] = professional.get('company address')
            golden_record['company_telfnumber'] = professional.get('company_telfnumber')
            golden_record['company_email'] = professional.get('company_email')
            golden_record['job'] = professional.get('job')
            golden_record['data_sources'].append('professional')
        
        # Check data completeness
        # Note: _id field already contains the passport, so we only check name and last_name
        required_fields = ['name', 'last_name']
        if all(golden_record.get(field) for field in required_fields):
            
            if not dry_run:
                # Add to bulk operations instead of individual writes
                bulk_operations.append(
                    UpdateOne(
                        {'_id': passport},
                        {'$set': golden_record},
                        upsert=True
                    )
                )
                
                # Execute batch when it reaches batch_size
                if len(bulk_operations) >= batch_size:
                    try:
                        result = golden_collection.bulk_write(bulk_operations, ordered=False)
                        stats['golden_records_created'] += result.upserted_count
                        stats['golden_records_updated'] += result.modified_count
                        batch_count += 1
                        log.info(f"✓ Batch {batch_count}: {result.upserted_count} created, {result.modified_count} updated ({len(bulk_operations)} operations)")
                    except Exception as e:
                        log.error(f"Batch write failed: {e}")
                    finally:
                        bulk_operations = []  # Reset for next batch
            else:
                log.debug(f"[DRY RUN] Would create/update golden record for passport {passport}")
                log.debug(f"  Data sources: {golden_record['data_sources']}")
                stats['golden_records_created'] += 1
        else:
            log.warning(f"Incomplete golden record for passport {passport}: missing required fields")
            stats['unmatched_records'] += 1
    
    # Execute remaining bulk operations (final batch)
    if not dry_run and bulk_operations:
        try:
            result = golden_collection.bulk_write(bulk_operations, ordered=False)
            stats['golden_records_created'] += result.upserted_count
            stats['golden_records_updated'] += result.modified_count
            batch_count += 1
            log.info(f"✓ Final batch {batch_count}: {result.upserted_count} created, {result.modified_count} updated ({len(bulk_operations)} operations)")
        except Exception as e:
            log.error(f"Final batch write failed: {e}")
    
    log.info("\n✅ CONSOLIDATION COMPLETE")
    log.info(f"  - Total raw messages: {stats['total_raw_messages']}")
    log.info(f"  - Empty data records (skipped): {stats.get('empty_data_count', 0)}")
    log.info(f"  - Golden records created: {stats['golden_records_created']}")
    log.info(f"  - Golden records updated: {stats['golden_records_updated']}")
    log.info(f"  - Incomplete/unmatched records: {stats['unmatched_records']}")
    
    return stats


def main():
    """Main entry point for the consolidation script"""
    log.info("🚀 MongoDB Record Consolidation Script")
    log.info("=" * 70)
    
    # Check for dry-run mode
    dry_run = '--dry-run' in sys.argv
    if dry_run:
        log.info("🧪 Running in DRY RUN mode (no writes to golden collection)")
    
    try:
        # Connect to MongoDB
        log.info(f"\n📡 Connecting to MongoDB...")
        raw_client, raw_collection = connect_to_mongodb(
            database=DATABASE,
            collection_name=RAW_COLLECTION
        )
        
        golden_client, golden_collection = connect_to_mongodb(
            database=DATABASE,
            collection_name=GOLDEN_COLLECTION
        )
        
        # Run consolidation
        stats = consolidate_records(raw_collection, golden_collection, dry_run=dry_run)
        
        # Print summary
        log.info("\n" + "=" * 70)
        log.info("📊 FINAL STATISTICS")
        log.info("=" * 70)
        for key, value in stats.items():
            log.info(f"  {key}: {value}")
        
        # Close connections
        close_connection(raw_client)
        close_connection(golden_client)
        
        log.info("\n✅ Script completed successfully!")
        return 0
        
    except Exception as e:
        log.error(f"\n❌ Error during consolidation: {e}")
        import traceback
        log.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
