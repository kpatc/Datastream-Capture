"""
Main CDC to Snowflake pipeline
"""
import logging
import time
import signal
import sys
from typing import List, Dict, Any
from kafka_consumer import CDCKafkaConsumer
from snowflake_connector import SnowflakeConnector
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCPipeline:
    """Main pipeline orchestrating CDC data flow from Kafka to Snowflake"""
    
    def __init__(self):
        self.config = Config
        self.kafka_consumer = CDCKafkaConsumer()
        self.snowflake_connector = SnowflakeConnector()
        self.running = False
        self.stats = {
            'messages_consumed': 0,
            'records_validated': 0,
            'records_loaded': 0,
            'errors': 0
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def setup(self):
        """Initialize the pipeline"""
        try:
            # Validate configuration
            self.config.validate()
            logger.info("Configuration validated successfully")
            
            # Create Snowflake table
            self.snowflake_connector.create_table_if_not_exists()
            logger.info("Snowflake table setup completed")
            
            # Create Kafka consumer
            self.kafka_consumer.create_consumer()
            logger.info("Kafka consumer setup completed")
            
            logger.info("Pipeline setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during pipeline setup: {e}")
            raise
    
    def process_batch(self, messages: List) -> List[Dict[str, Any]]:
        """Process a batch of Kafka messages"""
        validated_records = []
        
        for message in messages:
            try:
                self.stats['messages_consumed'] += 1
                
                # Parse Debezium message from message value (bytes)
                record = self.kafka_consumer.parse_debezium_message(message.value())
                
                if not record:
                    continue
                
                # Validate record
                if self.kafka_consumer.validate_record(record):
                    validated_records.append(record)
                    self.stats['records_validated'] += 1
                    logger.debug(f"Validated record: {record['transaction_id']}")
                else:
                    logger.warning(f"Record validation failed: {record}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                self.stats['errors'] += 1
        
        return validated_records
    
    def run(self):
        """Run the main pipeline loop"""
        logger.info("Starting CDC pipeline...")
        self.running = True
        
        consumer = self.kafka_consumer.get_consumer()
        batch = []
        last_commit_time = time.time()
        
        try:
            while self.running:
                # Poll for messages
                messages = self.kafka_consumer.poll_messages(
                    timeout=1.0,
                    max_messages=self.config.BATCH_SIZE
                )
                
                # Add messages to batch
                if messages:
                    batch.extend(messages)
                
                # Check if we should process the batch
                current_time = time.time()
                should_process = (
                    len(batch) >= self.config.BATCH_SIZE or
                    (batch and (current_time - last_commit_time) >= self.config.BATCH_TIMEOUT)
                )
                
                if should_process:
                    logger.info(f"Processing batch of {len(batch)} messages")
                    
                    # Process and validate records
                    validated_records = self.process_batch(batch)
                    
                    # Load to Snowflake
                    if validated_records:
                        try:
                            self.snowflake_connector.insert_batch(validated_records)
                            self.stats['records_loaded'] += len(validated_records)
                            logger.info(f"Loaded {len(validated_records)} records to Snowflake")
                            
                            # Commit offsets after successful load
                            self.kafka_consumer.commit()
                            logger.debug("Kafka offsets committed")
                            
                        except Exception as e:
                            logger.error(f"Error loading to Snowflake: {e}")
                            self.stats['errors'] += 1
                            # Don't commit offsets on error, will retry
                    
                    # Clear batch and update time
                    batch.clear()
                    last_commit_time = current_time
                    
                    # Log statistics
                    self._log_statistics()
                
                # Small sleep to prevent tight loop when no messages
                if not messages:
                    time.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Error in pipeline main loop: {e}")
            raise
        finally:
            self._shutdown()
    
    def _log_statistics(self):
        """Log pipeline statistics"""
        logger.info(
            f"Pipeline Stats - "
            f"Consumed: {self.stats['messages_consumed']}, "
            f"Validated: {self.stats['records_validated']}, "
            f"Loaded: {self.stats['records_loaded']}, "
            f"Errors: {self.stats['errors']}"
        )
        
        # Get Snowflake record count
        try:
            count = self.snowflake_connector.get_record_count()
            logger.info(f"Total records in Snowflake: {count}")
        except Exception as e:
            logger.warning(f"Could not get Snowflake record count: {e}")
    
    def _shutdown(self):
        """Cleanup and shutdown"""
        logger.info("Shutting down pipeline...")
        
        try:
            self.kafka_consumer.close()
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")
        
        logger.info("Final statistics:")
        self._log_statistics()
        logger.info("Pipeline shutdown complete")


def main():
    """Main entry point"""
    logger.info("Initializing CDC to Snowflake pipeline...")
    
    pipeline = CDCPipeline()
    
    try:
        # Setup pipeline
        pipeline.setup()
        
        # Run pipeline
        pipeline.run()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Pipeline terminated")


if __name__ == "__main__":
    main()
