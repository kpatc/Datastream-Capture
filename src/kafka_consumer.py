"""
Kafka consumer for CDC events from Debezium
"""
import json
import logging
import base64
import struct
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal
from confluent_kafka import Consumer, KafkaError, KafkaException
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CDCKafkaConsumer:
    """Consume CDC events from Kafka topics"""
    
    def __init__(self):
        self.config = Config
        self.consumer = None
        
    def create_consumer(self):
        """Create and configure Kafka consumer"""
        try:
            conf = {
                'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS,
                'group.id': self.config.KAFKA_GROUP_ID,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'session.timeout.ms': 30000,
                'max.poll.interval.ms': 300000
            }
            
            self.consumer = Consumer(conf)
            self.consumer.subscribe([self.config.KAFKA_TOPIC])
            logger.info(f"Kafka consumer created for topic: {self.config.KAFKA_TOPIC}")
            return self.consumer
        except KafkaException as e:
            logger.error(f"Error creating Kafka consumer: {e}")
            raise
    
    def poll_messages(self, timeout: float = 1.0, max_messages: int = 100) -> List[Any]:
        """Poll messages from Kafka"""
        messages = []
        try:
            for _ in range(max_messages):
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    break
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                messages.append(msg)
                
            return messages
        except Exception as e:
            logger.error(f"Error polling messages: {e}")
            return messages
    
    def commit(self):
        """Commit current offsets"""
        try:
            self.consumer.commit(asynchronous=False)
            logger.debug("Offsets committed")
        except KafkaException as e:
            logger.error(f"Error committing offsets: {e}")
    
    def parse_debezium_message(self, message_value: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse Debezium CDC message and extract transaction data
        
        Debezium message structure:
        {
            "before": {...},  # State before change (null for INSERT)
            "after": {...},   # State after change (null for DELETE)
            "source": {...},  # Metadata about the change
            "op": "c|u|d|r",  # Operation: create, update, delete, read
            "ts_ms": 1234567890
        }
        """
        try:
            if not message_value:
                return None
            
            # Decode JSON from bytes
            message = json.loads(message_value.decode('utf-8'))
            payload = message.get('payload', message)
            
            # Get operation type
            operation = payload.get('op', 'u')
            op_map = {
                'c': 'CREATE',
                'u': 'UPDATE',
                'd': 'DELETE',
                'r': 'READ'
            }
            cdc_operation = op_map.get(operation, 'UNKNOWN')
            
            # Get the data (use 'after' for INSERT/UPDATE, 'before' for DELETE)
            if cdc_operation == 'DELETE':
                data = payload.get('before', {})
            else:
                data = payload.get('after', {})
            
            if not data:
                logger.warning(f"No data found in message for operation: {cdc_operation}")
                return None
            
            # Convert timestamps from microseconds to datetime
            created_at = self._convert_timestamp(data.get('created_at'))
            updated_at = self._convert_timestamp(data.get('updated_at'))
            
            # Parse amount (can be bytes from Debezium Decimal encoding)
            amount = self._parse_decimal(data.get('amount'))
            
            # Build the record
            record = {
                'transaction_id': data.get('transaction_id'),
                'user_id': data.get('user_id'),
                'amount': amount,
                'status': data.get('status'),
                'created_at': created_at,
                'updated_at': updated_at,
                'cdc_operation': cdc_operation
            }
            
            # Validate required fields
            if not record['transaction_id']:
                logger.warning("Record missing transaction_id, skipping")
                return None
            
            return record
            
        except Exception as e:
            logger.error(f"Error parsing Debezium message: {e}")
            logger.debug(f"Message content: {message}")
            return None
    
    def _parse_decimal(self, amount_value) -> Optional[float]:
        """
        Parse Debezium Decimal field (can be bytes, int, float, or string)
        Debezium encodes DECIMAL as base64 bytes with scale parameter
        """
        if not amount_value:
            return None
        
        try:
            # If it's already a number
            if isinstance(amount_value, (int, float)):
                return float(amount_value)
            
            # If it's a string representation
            if isinstance(amount_value, str):
                return float(amount_value)
            
            # If it's bytes (Debezium Decimal encoding)
            if isinstance(amount_value, bytes):
                # Decode bytes to integer then apply scale (2 decimal places)
                import struct
                # Unpack big-endian bytes
                value_int = int.from_bytes(amount_value, byteorder='big', signed=True)
                # Apply scale (defined in schema as 2 for DECIMAL(10,2))
                return value_int / 100.0
            
            logger.warning(f"Unknown amount format: {type(amount_value)}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing amount {amount_value}: {e}")
            return None
    
    def _convert_timestamp(self, timestamp) -> Optional[str]:
        """Convert various timestamp formats to ISO format string"""
        if not timestamp:
            return None
        
        try:
            # If it's already a string, return it
            if isinstance(timestamp, str):
                return timestamp
            
            # If it's microseconds since epoch
            if isinstance(timestamp, int):
                # Debezium sends timestamps in microseconds
                dt = datetime.fromtimestamp(timestamp / 1000000.0)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            
            return str(timestamp)
        except Exception as e:
            logger.warning(f"Error converting timestamp {timestamp}: {e}")
            return None
    
    def _parse_amount(self, amount_value) -> Optional[float]:
        """
        Parse amount field which can be:
        - Direct numeric value
        - Base64 encoded decimal (Debezium format for DECIMAL types)
        - Dictionary with 'scale' and 'value' (base64)
        """
        if not amount_value:
            return None
        
        try:
            # If it's already a number, return it
            if isinstance(amount_value, (int, float)):
                return float(amount_value)
            
            # If it's a string that looks like a number
            if isinstance(amount_value, str):
                try:
                    return float(amount_value)
                except ValueError:
                    # It's base64 encoded, decode it
                    pass
            
            # Debezium encodes DECIMAL as base64
            # Format: base64 string representing the unscaled value
            if isinstance(amount_value, str):
                try:
                    # Decode base64
                    decoded = base64.b64decode(amount_value)
                    
                    # Convert bytes to integer (big-endian)
                    unscaled_value = int.from_bytes(decoded, byteorder='big', signed=True)
                    
                    # PostgreSQL DECIMAL(10,2) has scale=2
                    # So divide by 10^2 = 100
                    scale = 2
                    amount = unscaled_value / (10 ** scale)
                    
                    return float(amount)
                except Exception as e:
                    logger.warning(f"Error decoding base64 amount {amount_value}: {e}")
                    return None
            
            # If it's a dict with scale and value (alternative Debezium format)
            if isinstance(amount_value, dict):
                scale = amount_value.get('scale', 2)
                value = amount_value.get('value')
                
                if value:
                    if isinstance(value, str):
                        decoded = base64.b64decode(value)
                        unscaled_value = int.from_bytes(decoded, byteorder='big', signed=True)
                    else:
                        unscaled_value = int(value)
                    
                    amount = unscaled_value / (10 ** scale)
                    return float(amount)
            
            logger.warning(f"Unknown amount format: {amount_value} (type: {type(amount_value)})")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing amount {amount_value}: {e}")
            return None
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate transaction record
        
        Business rules:
        - transaction_id must exist
        - user_id must exist and be positive
        - amount should be present (can be None for some statuses)
        - status should be valid
        """
        try:
            # Check required fields
            if not record.get('transaction_id'):
                logger.warning("Validation failed: missing transaction_id")
                return False
            
            if not record.get('user_id') or record['user_id'] <= 0:
                logger.warning(f"Validation failed: invalid user_id {record.get('user_id')}")
                return False
            
            # Validate status if present
            valid_statuses = ['pending', 'completed', 'failed', 'cancelled', None]
            if record.get('status') and record['status'].lower() not in [s for s in valid_statuses if s]:
                logger.warning(f"Validation failed: invalid status {record.get('status')}")
                return False
            
            # Validate amount if present
            if record.get('amount') is not None:
                try:
                    amount = float(record['amount'])
                    if amount < 0:
                        logger.warning(f"Validation failed: negative amount {amount}")
                        return False
                except (ValueError, TypeError):
                    logger.warning(f"Validation failed: invalid amount {record.get('amount')}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating record: {e}")
            return False
    
    def close(self):
        """Close the Kafka consumer"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
    
    def get_consumer(self):
        """Get the consumer instance"""
        return self.consumer
