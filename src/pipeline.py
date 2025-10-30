"""
Main CDC to Snowflake pipeline - Production Ready
"""
import logging
import time
import signal
import sys
import os
from typing import List, Dict, Any
from kafka_consumer import CDCKafkaConsumer
from snowflake_connector import SnowflakeConnector
from config import Config
from error_handler import (
    CircuitBreaker, 
    ErrorHandler, 
    DeadLetterQueue,
    kafka_retry_decorator,
    snowflake_retry_decorator
)
from metrics import PipelineMetrics

# Setup professional logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'pipeline.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CDCPipeline:
    """
    Real-time CDC Pipeline - Production Ready
    
    Features:
    - Retry logic with exponential backoff
    - Circuit breaker for Snowflake
    - Detailed metrics
    - Robust error handling
    - Graceful shutdown
    """
    
    def __init__(self):
        self.config = Config
        self.kafka_consumer = CDCKafkaConsumer()
        self.snowflake_connector = SnowflakeConnector()
        self.running = False
        
        # Professional metrics
        self.metrics = PipelineMetrics()
        
        # Error handling with Dead Letter Queue
        self.dlq = DeadLetterQueue()
        self.error_handler = ErrorHandler(dlq=self.dlq, metrics_collector=self.metrics)
        
        # Circuit breakers
        self.kafka_circuit = self.error_handler.kafka_circuit_breaker
        self.snowflake_circuit = self.error_handler.snowflake_circuit_breaker
        
        # Statistics
        self.stats = {
            'messages_consumed': 0,
            'records_validated': 0,
            'records_loaded': 0,
            'errors': 0
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("CDC Pipeline initialized with robust error handling")
    
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
        """Process a batch of Kafka messages with robust error handling"""
        validated_records = []
        start_time = time.time()
        
        for message in messages:
            try:
                self.metrics.increment_consumed()
                self.stats['messages_consumed'] += 1
                
                # Parse Debezium message from message value (bytes)
                record = self.kafka_consumer.parse_debezium_message(message.value())
                
                if not record:
                    self.metrics.increment('errors_parsing')
                    continue
                
                self.metrics.increment_processed()
                
                # Track CDC operation type
                self.metrics.record_operation(record.get('cdc_operation', 'UNKNOWN'))
                
                # Validate record
                if self.kafka_consumer.validate_record(record):
                    validated_records.append(record)
                    self.metrics.increment_validated()
                    self.stats['records_validated'] += 1
                    logger.debug(f"✓ Validated record: {record.get('transaction_id')}")
                else:
                    # Send to DLQ
                    validation_error = ValueError(f"Invalid record: {record}")
                    self.error_handler.handle_validation_error(
                        validation_error, 
                        record, 
                        {'transaction_id': record.get('transaction_id')}
                    )
                    self.metrics.increment_rejected()
                    logger.warning(f"✗ Record validation failed: {record.get('transaction_id')}")
                    
            except Exception as e:
                self.metrics.increment_failed()
                self.stats['errors'] += 1
                self.error_handler.handle_kafka_error(e, message, {'stage': 'process_batch'})
                logger.error(f"Error processing message: {e}")
        
        # Record processing time and batch size
        processing_time = time.time() - start_time
        self.metrics.record_processing_time(processing_time)
        self.metrics.record_batch_size(len(messages))
        
        return validated_records
    
    def run(self):
        """Run the main pipeline loop"""
        mode = self.config.PIPELINE_MODE
        logger.info(f"Starting CDC pipeline in {mode.upper()} mode...")
        
        if mode == 'batch':
            self._run_batch_mode()
        else:
            self._run_realtime_mode()
    
    def _run_realtime_mode(self):
        """Run pipeline in continuous real-time mode"""
        logger.info("Real-time mode: Processing messages continuously...")
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
    
    def _run_batch_mode(self):
        """Run pipeline in batch mode (process available messages then exit)"""
        logger.info(f"Batch mode: Processing messages for max {self.config.BATCH_MAX_DURATION}s or {self.config.BATCH_MAX_MESSAGES} messages...")
        self.running = True
        
        consumer = self.kafka_consumer.get_consumer()
        batch = []
        start_time = time.time()
        total_processed = 0
        
        try:
            while self.running:
                # Check stop conditions for batch mode
                elapsed_time = time.time() - start_time
                if elapsed_time >= self.config.BATCH_MAX_DURATION:
                    logger.info(f"Batch mode: Max duration reached ({self.config.BATCH_MAX_DURATION}s)")
                    break
                
                if total_processed >= self.config.BATCH_MAX_MESSAGES:
                    logger.info(f"Batch mode: Max messages reached ({self.config.BATCH_MAX_MESSAGES})")
                    break
                
                # Poll for messages
                messages = self.kafka_consumer.poll_messages(
                    timeout=2.0,
                    max_messages=self.config.BATCH_SIZE
                )
                
                # If no messages for 10 seconds, assume we're done
                if not messages:
                    if len(batch) == 0 and elapsed_time > 10:
                        logger.info("Batch mode: No messages found after 10s, exiting")
                        break
                    time.sleep(0.5)
                    continue
                
                # Add messages to batch
                batch.extend(messages)
                total_processed += len(messages)
                
                # Check if we should process the batch
                current_time = time.time()
                should_process = (
                    len(batch) >= self.config.BATCH_SIZE or
                    (batch and (current_time - start_time) >= self.config.BATCH_TIMEOUT)
                )
                
                if should_process:
                    logger.info(f"Processing batch of {len(batch)} messages")
                    
                    # Process and validate records
                    validated_records = self.process_batch(batch)
                    
                    # Load to Snowflake with circuit breaker protection
                    if validated_records:
                        try:
                            # Use circuit breaker for Snowflake calls
                            self.snowflake_circuit.call(
                                self._load_to_snowflake_with_retry,
                                validated_records
                            )
                            
                            self.stats['records_loaded'] += len(validated_records)
                            self.metrics.increment_loaded(len(validated_records))
                            logger.info(f"✓ Loaded {len(validated_records)} records to Snowflake")
                            
                            # Commit offsets after successful load
                            self.kafka_consumer.commit()
                            logger.debug("Kafka offsets committed")
                            
                        except Exception as e:
                            logger.error(f"✗ Error loading to Snowflake: {e}")
                            self.stats['errors'] += 1
                            self.error_handler.handle_snowflake_error(
                                e, 
                                validated_records, 
                                {'batch_size': len(validated_records)}
                            )
                            # Don't commit offsets on error, will retry next run
                    
                    # Clear batch
                    batch.clear()
                    
                    # Log statistics
                    self._log_statistics()
            
            logger.info("Batch mode: Processing completed")
            
        except Exception as e:
            logger.error(f"Error in batch mode: {e}")
            raise
        finally:
            self._shutdown()
    
    @snowflake_retry_decorator()
    def _load_to_snowflake_with_retry(self, records: List[Dict[str, Any]]):
        """Load to Snowflake with automatic retry (exponential backoff)"""
        self.snowflake_connector.insert_batch(records)
    
    def _log_statistics(self):
        """Log pipeline statistics - production version"""
        self.metrics.increment_batch()
        
        # Quick stats every batch
        if self.metrics.batch_count % 5 == 0:
            self.metrics.log_quick_stats()
        
        # Detailed summary every 20 batches
        if self.metrics.batch_count % 20 == 0:
            self.metrics.log_summary()
            
            # Check Snowflake count
            try:
                count = self.snowflake_connector.get_record_count()
                logger.info(f"📊 Total records in Snowflake: {count:,}")
            except Exception as e:
                logger.warning(f"Could not get Snowflake record count: {e}")
            
            # Log DLQ status
            dlq_count = self.dlq.count()
            if dlq_count > 0:
                logger.warning(f"⚠️  Dead Letter Queue has {dlq_count} failed messages")
    
    def _shutdown(self):
        """Cleanup and shutdown - Production version"""
        logger.info("=" * 80)
        logger.info("SHUTTING DOWN CDC PIPELINE")
        logger.info("=" * 80)
        
        try:
            self.kafka_consumer.close()
            logger.info("✓ Kafka consumer closed")
        except Exception as e:
            logger.error(f"✗ Error closing Kafka consumer: {e}")
        
        # Log final metrics
        logger.info("\n" + "=" * 80)
        logger.info("FINAL PIPELINE STATISTICS")
        logger.info("=" * 80)
        self.metrics.log_summary()
        
        # Export metrics to JSON
        try:
            metrics_file = os.path.join(
                os.path.dirname(__file__), 
                '..', 
                'logs', 
                f'metrics_{int(time.time())}.json'
            )
            self.metrics.export_to_json(metrics_file)
        except Exception as e:
            logger.warning(f"Could not export metrics: {e}")
        
        # Log Dead Letter Queue status
        dlq_count = self.dlq.count()
        if dlq_count > 0:
            logger.warning(f"\n⚠️  DEAD LETTER QUEUE: {dlq_count} failed messages")
            logger.warning(f"Review failed messages in: {self.dlq.filepath}")
        else:
            logger.info(f"\n✓ Dead Letter Queue is empty")
        
        # Log circuit breaker states
        logger.info(f"\nCircuit Breaker States:")
        logger.info(f"  - Kafka:     {self.kafka_circuit.state}")
        logger.info(f"  - Snowflake: {self.snowflake_circuit.state}")
        
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE SHUTDOWN COMPLETE")
        logger.info("=" * 80)


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
