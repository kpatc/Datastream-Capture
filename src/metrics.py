"""
Metrics collection and monitoring for CDC pipeline
"""
import logging
import time
from typing import Dict, Any, List
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """
    Comprehensive metrics collector for CDC pipeline
    Tracks performance, throughput, errors, and latency
    """
    
    def __init__(self):
        # Message metrics
        self.messages_consumed = 0
        self.messages_processed = 0
        self.messages_validated = 0
        self.messages_rejected = 0
        self.records_loaded = 0
        
        # Batch metrics
        self.batch_count = 0
        self.total_batches_processed = 0
        
        # Error metrics
        self.errors_total = 0
        self.errors_kafka = 0
        self.errors_snowflake = 0
        self.errors_validation = 0
        self.errors_parsing = 0
        
        # Performance metrics
        self.processing_times: List[float] = []
        self.batch_sizes: List[int] = []
        
        # Timestamps
        self.start_time = time.time()
        self.last_metric_log = time.time()
        
        # Operation counts by CDC operation type
        self.operations = defaultdict(int)  # CREATE, UPDATE, DELETE, READ
        
        # Error tracking
        self.error_details: List[Dict[str, Any]] = []
        
        logger.info("Pipeline metrics initialized")
    
    def increment_consumed(self):
        """Increment messages consumed counter"""
        self.messages_consumed += 1
    
    def increment_processed(self):
        """Increment messages processed counter"""
        self.messages_processed += 1
    
    def increment_validated(self):
        """Increment validated messages counter"""
        self.messages_validated += 1
    
    def increment_rejected(self):
        """Increment rejected messages counter"""
        self.messages_rejected += 1
    
    def increment_loaded(self, count: int = 1):
        """Increment loaded records counter"""
        self.records_loaded += count
    
    def increment_failed(self):
        """Increment failed messages counter"""
        self.errors_total += 1
    
    def increment_batch(self):
        """Increment batch counter"""
        self.batch_count += 1
        self.total_batches_processed += 1
    
    def increment(self, metric_name: str):
        """Generic increment for any metric"""
        if hasattr(self, metric_name):
            current = getattr(self, metric_name)
            setattr(self, metric_name, current + 1)
        else:
            logger.warning(f"Unknown metric: {metric_name}")
    
    def record_operation(self, operation: str):
        """Record CDC operation type"""
        self.operations[operation] += 1
    
    def record_processing_time(self, duration: float):
        """Record batch processing time"""
        self.processing_times.append(duration)
        # Keep only last 100 measurements for memory efficiency
        if len(self.processing_times) > 100:
            self.processing_times = self.processing_times[-100:]
    
    def record_batch_size(self, size: int):
        """Record batch size"""
        self.batch_sizes.append(size)
        if len(self.batch_sizes) > 100:
            self.batch_sizes = self.batch_sizes[-100:]
    
    def record_validation_error(self, error_detail: str):
        """Record validation error details"""
        self.errors_validation += 1
        self.error_details.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'type': 'validation',
            'detail': error_detail
        })
        # Keep only last 50 errors
        if len(self.error_details) > 50:
            self.error_details = self.error_details[-50:]
    
    def record_error(self, error_type: str, error_detail: str):
        """Record general error"""
        self.errors_total += 1
        self.error_details.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'type': error_type,
            'detail': error_detail
        })
        if len(self.error_details) > 50:
            self.error_details = self.error_details[-50:]
    
    def get_throughput(self) -> float:
        """Calculate messages per second throughput"""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.messages_consumed / elapsed
        return 0.0
    
    def get_average_processing_time(self) -> float:
        """Get average batch processing time"""
        if self.processing_times:
            return sum(self.processing_times) / len(self.processing_times)
        return 0.0
    
    def get_average_batch_size(self) -> float:
        """Get average batch size"""
        if self.batch_sizes:
            return sum(self.batch_sizes) / len(self.batch_sizes)
        return 0.0
    
    def get_success_rate(self) -> float:
        """Calculate success rate (validated / consumed)"""
        if self.messages_consumed > 0:
            return (self.messages_validated / self.messages_consumed) * 100
        return 0.0
    
    def get_error_rate(self) -> float:
        """Calculate error rate"""
        if self.messages_consumed > 0:
            return (self.errors_total / self.messages_consumed) * 100
        return 0.0
    
    def get_uptime(self) -> float:
        """Get pipeline uptime in seconds"""
        return time.time() - self.start_time
    
    def log_summary(self):
        """Log comprehensive metrics summary"""
        elapsed = self.get_uptime()
        throughput = self.get_throughput()
        avg_processing_time = self.get_average_processing_time()
        avg_batch_size = self.get_average_batch_size()
        success_rate = self.get_success_rate()
        error_rate = self.get_error_rate()
        
        logger.info("=" * 80)
        logger.info("PIPELINE METRICS SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Uptime: {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
        logger.info("")
        
        logger.info("MESSAGE STATISTICS:")
        logger.info(f"  • Consumed:    {self.messages_consumed:,}")
        logger.info(f"  • Processed:   {self.messages_processed:,}")
        logger.info(f"  • Validated:   {self.messages_validated:,}")
        logger.info(f"  • Rejected:    {self.messages_rejected:,}")
        logger.info(f"  • Loaded:      {self.records_loaded:,}")
        logger.info("")
        
        logger.info("BATCH STATISTICS:")
        logger.info(f"  • Total batches:     {self.total_batches_processed:,}")
        logger.info(f"  • Avg batch size:    {avg_batch_size:.1f} messages")
        logger.info(f"  • Avg process time:  {avg_processing_time:.3f}s")
        logger.info("")
        
        logger.info("PERFORMANCE:")
        logger.info(f"  • Throughput:    {throughput:.2f} msg/sec")
        logger.info(f"  • Success rate:  {success_rate:.2f}%")
        logger.info(f"  • Error rate:    {error_rate:.2f}%")
        logger.info("")
        
        if self.operations:
            logger.info("CDC OPERATIONS:")
            for op, count in sorted(self.operations.items()):
                logger.info(f"  • {op:8s}: {count:,}")
            logger.info("")
        
        if self.errors_total > 0:
            logger.info("ERROR BREAKDOWN:")
            logger.info(f"  • Total errors:      {self.errors_total:,}")
            logger.info(f"  • Kafka errors:      {self.errors_kafka:,}")
            logger.info(f"  • Snowflake errors:  {self.errors_snowflake:,}")
            logger.info(f"  • Validation errors: {self.errors_validation:,}")
            logger.info(f"  • Parsing errors:    {self.errors_parsing:,}")
            logger.info("")
        
        logger.info("=" * 80)
    
    def log_quick_stats(self):
        """Log quick one-line stats"""
        logger.info(
            f"📊 Pipeline Stats - "
            f"Consumed: {self.messages_consumed:,}, "
            f"Validated: {self.messages_validated:,}, "
            f"Loaded: {self.records_loaded:,}, "
            f"Errors: {self.errors_total:,}, "
            f"Throughput: {self.get_throughput():.2f} msg/s"
        )
    
    def reset(self):
        """Reset all metrics (useful for testing)"""
        self.__init__()
        logger.info("Metrics reset")
    
    def to_dict(self) -> Dict[str, Any]:
        """Export metrics as dictionary"""
        return {
            'messages': {
                'consumed': self.messages_consumed,
                'processed': self.messages_processed,
                'validated': self.messages_validated,
                'rejected': self.messages_rejected,
                'loaded': self.records_loaded
            },
            'batches': {
                'total': self.total_batches_processed,
                'average_size': self.get_average_batch_size(),
                'average_processing_time': self.get_average_processing_time()
            },
            'errors': {
                'total': self.errors_total,
                'kafka': self.errors_kafka,
                'snowflake': self.errors_snowflake,
                'validation': self.errors_validation,
                'parsing': self.errors_parsing
            },
            'performance': {
                'uptime_seconds': self.get_uptime(),
                'throughput_msg_per_sec': self.get_throughput(),
                'success_rate_percent': self.get_success_rate(),
                'error_rate_percent': self.get_error_rate()
            },
            'operations': dict(self.operations),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def export_to_json(self, filepath: str):
        """Export metrics to JSON file"""
        import json
        try:
            with open(filepath, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            logger.info(f"Metrics exported to {filepath}")
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
