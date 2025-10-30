"""
Test configuration and fixtures
"""
import pytest
import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def test_config():
    """Test configuration"""
    return {
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA': 'PUBLIC',
        'SNOWFLAKE_WAREHOUSE': 'TEST_WH',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
        'KAFKA_TOPIC': 'test-topic',
        'KAFKA_GROUP_ID': 'test-group',
        'BATCH_SIZE': 100,
        'BATCH_TIMEOUT': 5
    }


@pytest.fixture
def sample_transaction():
    """Sample transaction record"""
    return {
        'transaction_id': 1,
        'user_id': 101,
        'amount': 150.50,
        'status': 'completed',
        'created_at': '2025-10-30 14:00:00',
        'updated_at': '2025-10-30 14:00:00',
        'cdc_operation': 'CREATE'
    }


@pytest.fixture
def sample_debezium_message():
    """Sample Debezium CDC message"""
    return {
        "payload": {
            "before": None,
            "after": {
                "transaction_id": 1,
                "user_id": 101,
                "amount": "MTUwLjUw",  # 150.50 in base64
                "status": "completed",
                "created_at": 1698500000000000,
                "updated_at": 1698500000000000
            },
            "source": {
                "version": "2.3.4.Final",
                "connector": "postgresql",
                "name": "postgres-transactions",
                "ts_ms": 1698500000000,
                "snapshot": "false",
                "db": "postgres",
                "sequence": "[\"1234567890\",\"1234567891\"]",
                "schema": "public",
                "table": "transactions",
                "txId": 12345,
                "lsn": 123456789,
                "xmin": None
            },
            "op": "c",
            "ts_ms": 1698500000123,
            "transaction": None
        }
    }
