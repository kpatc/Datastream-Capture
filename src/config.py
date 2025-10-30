"""
Configuration module for CDC to Snowflake pipeline
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration class for the CDC pipeline"""
    
    # Snowflake Configuration (adapted to your .env variables)
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_URL', '').replace('.snowflakecomputing.com', '')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASS')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DB', 'CDC_DB')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WH', 'COMPUTE_WH')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    SNOWFLAKE_TABLE = 'TRANSACTIONS'
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'postgres-transactions.public.transactions')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'snowflake-consumer-group')
    
    # Schema Registry
    SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
    
    # Pipeline Mode: 'realtime' or 'batch'
    PIPELINE_MODE = os.getenv('PIPELINE_MODE', 'realtime')
    
    # Real-time processing configuration
    # Micro-batch size for efficient Snowflake loading (process as soon as we have data)
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))  # Small batches for low latency
    # Batch timeout in seconds (process even if batch not full)
    BATCH_TIMEOUT = int(os.getenv('BATCH_TIMEOUT', '2'))  # 2 seconds max wait
    # Kafka poll timeout in seconds
    KAFKA_POLL_TIMEOUT = float(os.getenv('KAFKA_POLL_TIMEOUT', '0.5'))  # 500ms
    
    # For batch mode: max duration in seconds (default 5 minutes)
    BATCH_MAX_DURATION = int(os.getenv('BATCH_MAX_DURATION', '300'))
    # For batch mode: max messages to process (default 10000)
    BATCH_MAX_MESSAGES = int(os.getenv('BATCH_MAX_MESSAGES', '10000'))
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required = {
            'SNOWFLAKE_ACCOUNT': cls.SNOWFLAKE_ACCOUNT,
            'SNOWFLAKE_USER': cls.SNOWFLAKE_USER,
            'SNOWFLAKE_PASSWORD': cls.SNOWFLAKE_PASSWORD,
            'SNOWFLAKE_DATABASE': cls.SNOWFLAKE_DATABASE
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        
        print(f"✓ Configuration validated:")
        print(f"  - Account: {cls.SNOWFLAKE_ACCOUNT}")
        print(f"  - User: {cls.SNOWFLAKE_USER}")
        print(f"  - Database: {cls.SNOWFLAKE_DATABASE}")
        print(f"  - Schema: {cls.SNOWFLAKE_SCHEMA}")
        print(f"  - Warehouse: {cls.SNOWFLAKE_WAREHOUSE}")
        return True
