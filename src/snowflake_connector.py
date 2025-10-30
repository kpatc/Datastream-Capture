"""
Snowflake connector for loading CDC data
"""
import logging
from typing import List, Dict, Any
import snowflake.connector
from snowflake.connector import DictCursor
from contextlib import contextmanager
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """Handle Snowflake connections and data loading"""
    
    def __init__(self):
        self.config = Config
        self.connection = None
        
    @contextmanager
    def get_connection(self):
        """Context manager for Snowflake connection"""
        try:
            conn = snowflake.connector.connect(
                account=self.config.SNOWFLAKE_ACCOUNT,
                user=self.config.SNOWFLAKE_USER,
                password=self.config.SNOWFLAKE_PASSWORD,
                database=self.config.SNOWFLAKE_DATABASE,
                schema=self.config.SNOWFLAKE_SCHEMA,
                warehouse=self.config.SNOWFLAKE_WAREHOUSE,
                role=self.config.SNOWFLAKE_ROLE
            )
            logger.info("Connected to Snowflake successfully")
            yield conn
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Snowflake connection closed")
    
    def create_table_if_not_exists(self):
        """Create the transactions table in Snowflake if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
            transaction_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount DECIMAL(10, 2),
            status VARCHAR(20),
            created_at TIMESTAMP_NTZ,
            updated_at TIMESTAMP_NTZ,
            cdc_operation VARCHAR(10),
            cdc_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            source_db VARCHAR(50) DEFAULT 'postgres'
        )
        """.format(
            database=self.config.SNOWFLAKE_DATABASE,
            schema=self.config.SNOWFLAKE_SCHEMA,
            table=self.config.SNOWFLAKE_TABLE
        )
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(create_table_sql)
                logger.info(f"Table {self.config.SNOWFLAKE_TABLE} created or already exists")
            except Exception as e:
                logger.error(f"Error creating table: {e}")
                raise
            finally:
                cursor.close()
    
    def insert_batch(self, records: List[Dict[str, Any]]):
        """Insert a batch of records into Snowflake"""
        if not records:
            logger.warning("No records to insert")
            return
        
        merge_sql = """
        MERGE INTO {database}.{schema}.{table} AS target
        USING (
            SELECT 
                %(transaction_id)s AS transaction_id,
                %(user_id)s AS user_id,
                %(amount)s AS amount,
                %(status)s AS status,
                %(created_at)s AS created_at,
                %(updated_at)s AS updated_at,
                %(cdc_operation)s AS cdc_operation
        ) AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED AND source.cdc_operation = 'DELETE' THEN
            DELETE
        WHEN MATCHED AND source.cdc_operation IN ('UPDATE', 'READ') THEN
            UPDATE SET
                user_id = source.user_id,
                amount = source.amount,
                status = source.status,
                created_at = source.created_at,
                updated_at = source.updated_at,
                cdc_operation = source.cdc_operation,
                cdc_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED AND source.cdc_operation != 'DELETE' THEN
            INSERT (transaction_id, user_id, amount, status, created_at, updated_at, cdc_operation)
            VALUES (source.transaction_id, source.user_id, source.amount, source.status, 
                    source.created_at, source.updated_at, source.cdc_operation)
        """.format(
            database=self.config.SNOWFLAKE_DATABASE,
            schema=self.config.SNOWFLAKE_SCHEMA,
            table=self.config.SNOWFLAKE_TABLE
        )
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for record in records:
                    cursor.execute(merge_sql, record)
                conn.commit()
                logger.info(f"Successfully inserted/updated {len(records)} records")
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
                conn.rollback()
                raise
            finally:
                cursor.close()
    
    def get_record_count(self) -> int:
        """Get the total count of records in the table"""
        count_sql = f"""
        SELECT COUNT(*) as count 
        FROM {self.config.SNOWFLAKE_DATABASE}.{self.config.SNOWFLAKE_SCHEMA}.{self.config.SNOWFLAKE_TABLE}
        """
        
        with self.get_connection() as conn:
            cursor = conn.cursor(DictCursor)
            try:
                cursor.execute(count_sql)
                result = cursor.fetchone()
                return result['COUNT'] if result else 0
            except Exception as e:
                logger.error(f"Error getting record count: {e}")
                return 0
            finally:
                cursor.close()
