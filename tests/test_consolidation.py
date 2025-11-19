"""
Test script for consolidation logic using test data

This script tests the consolidation process using the sample data from tests/data/test_data.json
"""

import os
import sys
import json
from datetime import datetime

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.logger import Logger

log = Logger()


def simulate_raw_kafka_messages():
    """
    Simulate raw Kafka messages by splitting the test_data.json golden records
    back into the 5 separate schemas
    """
    log.info("📋 Loading test data...")
    
    test_data_path = os.path.join(PROJECT_ROOT, 'tests', 'data', 'test_data.json')
    
    with open(test_data_path, 'r') as f:
        data = json.load(f)
    
    raw_messages = []
    
    for record in data['Golden']:
        # Schema 1: Personal data
        personal = {
            'schema_type': 'personal',
            'data': {
                'name': record['name'],
                'last_name': record['last_name'],
                'sex': record['sex'],
                'telfnumber': record['telfnumber'],
                'passport': record['_id'],
                'email': record['email']
            }
        }
        raw_messages.append(personal)
        
        # Schema 2: Location
        location = {
            'schema_type': 'location',
            'data': {
                'fullname': record['fullname'],
                'city': record['city'],
                'address': record['address']
            }
        }
        raw_messages.append(location)
        
        # Schema 3: Professional
        professional = {
            'schema_type': 'professional',
            'data': {
                'fullname': record['fullname'],
                'company': record['company'],
                'company address': record['company address'],
                'company_telfnumber': record['company_telfnumber'],
                'company_email': record['company_email'],
                'job': record['job']
            }
        }
        raw_messages.append(professional)
        
        # Schema 4: Bank
        bank = {
            'schema_type': 'bank',
            'data': {
                'passport': record['_id'],
                'IBAN': record['IBAN'],
                'salary': record['salary']
            }
        }
        raw_messages.append(bank)
        
        # Schema 5: Network
        network = {
            'schema_type': 'network',
            'data': {
                'address': record['address'],
                'IPv4': record['IPv4']
            }
        }
        raw_messages.append(network)
    
    log.info(f"✓ Generated {len(raw_messages)} simulated raw messages from {len(data['Golden'])} golden records")
    return raw_messages


def test_schema_detection():
    """Test that schema detection works correctly"""
    from scripts.consolidate_mongodb_records import detect_schema_type, SchemaType
    
    log.info("\n" + "="*70)
    log.info("🧪 TEST 1: Schema Detection")
    log.info("="*70)
    
    test_cases = [
        {
            'data': {'name': 'John', 'last_name': 'Doe', 'sex': ['M'], 'passport': '123', 'email': 'john@test.com', 'telfnumber': '555-0000'},
            'expected': SchemaType.PERSONAL
        },
        {
            'data': {'passport': '123', 'IBAN': 'GB123456', 'salary': '50000$'},
            'expected': SchemaType.BANK
        },
        {
            'data': {'fullname': 'John Doe', 'city': 'London', 'address': '123 Main St'},
            'expected': SchemaType.LOCATION
        },
        {
            'data': {'fullname': 'John Doe', 'company': 'Acme Corp', 'job': 'Engineer', 'company_email': 'info@acme.com', 'company_telfnumber': '555-0001', 'company address': '456 Corporate Blvd'},
            'expected': SchemaType.PROFESSIONAL
        },
        {
            'data': {'address': '123 Main St', 'IPv4': '192.168.1.1'},
            'expected': SchemaType.NETWORK
        }
    ]
    
    passed = 0
    failed = 0
    
    for i, test in enumerate(test_cases, 1):
        detected = detect_schema_type(test['data'])
        if detected == test['expected']:
            log.info(f"✓ Test {i}: {test['expected']} - PASSED")
            passed += 1
        else:
            log.error(f"✗ Test {i}: Expected {test['expected']}, got {detected} - FAILED")
            failed += 1
    
    log.info(f"\n📊 Results: {passed} passed, {failed} failed")
    return failed == 0


def test_consolidation_logic():
    """Test the complete consolidation logic"""
    log.info("\n" + "="*70)
    log.info("🧪 TEST 2: Consolidation Logic")
    log.info("="*70)
    
    raw_messages = simulate_raw_kafka_messages()
    
    # Simulate MongoDB collections using dicts
    class MockCollection:
        def __init__(self, data):
            self.data = data
        
        def find(self):
            # Wrap raw messages to simulate MongoDB structure
            for msg in self.data:
                yield {
                    '_id': f"kafka_{hash(str(msg))}",
                    'data': msg['data'],
                    'kafka_metadata': {'offset': 0},
                    'inserted_at': datetime.now().isoformat()
                }
        
        def count_documents(self, query):
            return len(self.data)
        
        def update_one(self, filter_query, update, upsert=False):
            log.debug(f"Mock update: {filter_query}")
            class Result:
                upserted_id = True
                modified_count = 0
            return Result()
    
    raw_collection = MockCollection(raw_messages)
    golden_collection = MockCollection([])
    
    # Run consolidation
    from scripts.consolidate_mongodb_records import consolidate_records
    
    stats = consolidate_records(raw_collection, golden_collection, dry_run=True)
    
    # Verify statistics
    log.info("\n📊 Consolidation Statistics:")
    for key, value in stats.items():
        log.info(f"  {key}: {value}")
    
    # Check if we got the expected number of golden records
    expected_golden = 10  # From test_data.json
    if stats['golden_records_created'] == expected_golden:
        log.info(f"\n✓ TEST PASSED: Created {expected_golden} golden records as expected")
        return True
    else:
        log.error(f"\n✗ TEST FAILED: Expected {expected_golden} golden records, got {stats['golden_records_created']}")
        return False


def main():
    """Run all tests"""
    log.info("="*70)
    log.info("🚀 Consolidation Logic Test Suite")
    log.info("="*70)
    
    results = []
    
    # Test 1: Schema detection
    results.append(("Schema Detection", test_schema_detection()))
    
    # Test 2: Consolidation logic
    results.append(("Consolidation Logic", test_consolidation_logic()))
    
    # Summary
    log.info("\n" + "="*70)
    log.info("📊 TEST SUMMARY")
    log.info("="*70)
    
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        log.info(f"  {test_name}: {status}")
    
    all_passed = all(result for _, result in results)
    
    if all_passed:
        log.info("\n✅ All tests passed!")
        return 0
    else:
        log.error("\n❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
