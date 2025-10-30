"""
Tests unitaires pour kafka_consumer.py
"""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kafka_consumer import CDCKafkaConsumer


class TestCDCKafkaConsumer:
    """Tests for CDCKafkaConsumer class"""
    
    @pytest.fixture
    def consumer(self):
        """Create a consumer instance for testing"""
        with patch('kafka_consumer.Config'):
            consumer = CDCKafkaConsumer()
            consumer.config.KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
            consumer.config.KAFKA_TOPIC = 'test-topic'
            consumer.config.KAFKA_GROUP_ID = 'test-group'
            consumer.config.BATCH_SIZE = 100
            return consumer
    
    def test_parse_debezium_create_message(self, consumer):
        """Test parsing a CREATE (INSERT) operation"""
        message_value = json.dumps({
            "payload": {
                "op": "c",
                "after": {
                    "transaction_id": 1,
                    "user_id": 101,
                    "amount": "MTUwLjUw",  # base64 encoded 150.50
                    "status": "completed",
                    "created_at": 1698500000000000,
                    "updated_at": 1698500000000000
                }
            }
        }).encode('utf-8')
        
        record = consumer.parse_debezium_message(message_value)
        
        assert record is not None
        assert record['transaction_id'] == 1
        assert record['user_id'] == 101
        assert record['status'] == 'completed'
        assert record['cdc_operation'] == 'CREATE'
    
    def test_parse_debezium_update_message(self, consumer):
        """Test parsing an UPDATE operation"""
        message_value = json.dumps({
            "payload": {
                "op": "u",
                "after": {
                    "transaction_id": 2,
                    "user_id": 102,
                    "amount": "MjAwLjAw",
                    "status": "pending",
                    "created_at": 1698500000000000,
                    "updated_at": 1698510000000000
                }
            }
        }).encode('utf-8')
        
        record = consumer.parse_debezium_message(message_value)
        
        assert record is not None
        assert record['cdc_operation'] == 'UPDATE'
        assert record['status'] == 'pending'
    
    def test_parse_debezium_delete_message(self, consumer):
        """Test parsing a DELETE operation"""
        message_value = json.dumps({
            "payload": {
                "op": "d",
                "before": {
                    "transaction_id": 3,
                    "user_id": 103,
                    "amount": "NzUuMjU=",
                    "status": "completed",
                    "created_at": 1698500000000000,
                    "updated_at": 1698500000000000
                }
            }
        }).encode('utf-8')
        
        record = consumer.parse_debezium_message(message_value)
        
        assert record is not None
        assert record['cdc_operation'] == 'DELETE'
        assert record['transaction_id'] == 3
    
    def test_validate_record_success(self, consumer):
        """Test validation of a valid record"""
        valid_record = {
            'transaction_id': 1,
            'user_id': 101,
            'amount': 150.50,
            'status': 'completed',
            'created_at': '2025-10-30 14:00:00',
            'updated_at': '2025-10-30 14:00:00',
            'cdc_operation': 'CREATE'
        }
        
        assert consumer.validate_record(valid_record) is True
    
    def test_validate_record_missing_transaction_id(self, consumer):
        """Test validation fails when transaction_id is missing"""
        invalid_record = {
            'user_id': 101,
            'amount': 150.50,
            'status': 'completed'
        }
        
        assert consumer.validate_record(invalid_record) is False
    
    def test_validate_record_invalid_user_id(self, consumer):
        """Test validation fails for invalid user_id"""
        invalid_record = {
            'transaction_id': 1,
            'user_id': -1,  # Negative user_id
            'amount': 150.50,
            'status': 'completed'
        }
        
        assert consumer.validate_record(invalid_record) is False
    
    def test_validate_record_negative_amount(self, consumer):
        """Test validation fails for negative amount"""
        invalid_record = {
            'transaction_id': 1,
            'user_id': 101,
            'amount': -100.00,
            'status': 'completed'
        }
        
        assert consumer.validate_record(invalid_record) is False
    
    def test_validate_record_invalid_status(self, consumer):
        """Test validation fails for invalid status"""
        invalid_record = {
            'transaction_id': 1,
            'user_id': 101,
            'amount': 150.50,
            'status': 'invalid_status'
        }
        
        assert consumer.validate_record(invalid_record) is False
    
    def test_parse_empty_message(self, consumer):
        """Test parsing an empty message returns None"""
        assert consumer.parse_debezium_message(None) is None
        assert consumer.parse_debezium_message(b'') is None
    
    def test_parse_malformed_json(self, consumer):
        """Test parsing malformed JSON returns None"""
        malformed_message = b'{invalid json}'
        
        record = consumer.parse_debezium_message(malformed_message)
        assert record is None
    
    @patch('kafka_consumer.Consumer')
    def test_create_consumer(self, mock_consumer_class, consumer):
        """Test Kafka consumer creation"""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        result = consumer.create_consumer()
        
        assert result is not None
        mock_consumer_class.assert_called_once()
        mock_consumer.subscribe.assert_called_once()
    
    def test_convert_timestamp_microseconds(self, consumer):
        """Test timestamp conversion from microseconds"""
        # 1698500000000000 microseconds = 2023-10-28 14:13:20
        timestamp_micro = 1698500000000000
        result = consumer._convert_timestamp(timestamp_micro)
        
        assert result is not None
        assert isinstance(result, str)
    
    def test_convert_timestamp_string(self, consumer):
        """Test timestamp conversion from string"""
        timestamp_str = "2025-10-30 14:00:00"
        result = consumer._convert_timestamp(timestamp_str)
        
        assert result == timestamp_str
    
    def test_convert_timestamp_none(self, consumer):
        """Test timestamp conversion with None"""
        assert consumer._convert_timestamp(None) is None
