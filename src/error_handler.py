"""
Gestion d'erreurs robuste avec retry logic et circuit breaker
Pour une meilleure résilience en production
"""
import logging
from typing import Callable, Any, Optional
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)
from confluent_kafka import KafkaException
import snowflake.connector.errors as sf_errors

logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration des stratégies de retry"""
    
    # Kafka retries
    KAFKA_MAX_ATTEMPTS = 5
    KAFKA_WAIT_MIN = 1  # secondes
    KAFKA_WAIT_MAX = 30  # secondes
    
    # Snowflake retries
    SNOWFLAKE_MAX_ATTEMPTS = 3
    SNOWFLAKE_WAIT_MIN = 2
    SNOWFLAKE_WAIT_MAX = 60
    
    # Validation retries
    VALIDATION_MAX_ATTEMPTS = 2


def kafka_retry_decorator():
    """
    Décorateur pour retry sur erreurs Kafka
    Exponentiel backoff: 1s, 2s, 4s, 8s, 16s
    """
    return retry(
        stop=stop_after_attempt(RetryConfig.KAFKA_MAX_ATTEMPTS),
        wait=wait_exponential(
            min=RetryConfig.KAFKA_WAIT_MIN,
            max=RetryConfig.KAFKA_WAIT_MAX
        ),
        retry=retry_if_exception_type((KafkaException, ConnectionError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )


def snowflake_retry_decorator():
    """
    Décorateur pour retry sur erreurs Snowflake récupérables
    """
    return retry(
        stop=stop_after_attempt(RetryConfig.SNOWFLAKE_MAX_ATTEMPTS),
        wait=wait_exponential(
            min=RetryConfig.SNOWFLAKE_WAIT_MIN,
            max=RetryConfig.SNOWFLAKE_WAIT_MAX
        ),
        retry=retry_if_exception_type((
            sf_errors.OperationalError,
            sf_errors.InternalServerError,
            ConnectionError,
            TimeoutError
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )


class CircuitBreaker:
    """
    Circuit breaker pattern pour éviter les cascades d'erreurs
    États: CLOSED (normal) -> OPEN (erreurs) -> HALF_OPEN (test) -> CLOSED
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        
        self.logger = logging.getLogger(f"{__name__}.CircuitBreaker")
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Exécuter une fonction avec circuit breaker protection
        """
        import time
        
        if self.state == 'OPEN':
            if self.last_failure_time and \
               (time.time() - self.last_failure_time) > self.recovery_timeout:
                self.logger.info("Circuit breaker entering HALF_OPEN state")
                self.state = 'HALF_OPEN'
            else:
                raise Exception(f"Circuit breaker is OPEN - too many failures")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        """Réinitialiser après succès"""
        if self.state == 'HALF_OPEN':
            self.logger.info("Circuit breaker recovered - entering CLOSED state")
            self.state = 'CLOSED'
        
        self.failure_count = 0
        self.last_failure_time = None
    
    def _on_failure(self):
        """Incrémenter compteur d'échecs"""
        import time
        
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.logger.error(
                f"Circuit breaker threshold reached ({self.failure_count} failures) "
                f"- entering OPEN state"
            )
            self.state = 'OPEN'
        else:
            self.logger.warning(
                f"Circuit breaker failure count: {self.failure_count}/{self.failure_threshold}"
            )
    
    def reset(self):
        """Réinitialiser manuellement le circuit breaker"""
        self.logger.info("Circuit breaker manually reset")
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'


class DeadLetterQueue:
    """
    Dead Letter Queue pour messages qui ne peuvent pas être traités
    Sauvegarde pour analyse ultérieure
    """
    
    def __init__(self, filepath: str = "logs/dead_letter_queue.jsonl"):
        self.filepath = filepath
        self.logger = logging.getLogger(f"{__name__}.DeadLetterQueue")
        
        # Créer le répertoire si nécessaire
        import os
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    def add(self, message: Any, error: Exception, context: dict = None):
        """Ajouter un message problématique à la DLQ"""
        import json
        from datetime import datetime
        
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context or {},
            'message': self._serialize_message(message)
        }
        
        try:
            with open(self.filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry) + '\n')
            
            self.logger.warning(
                f"Message added to DLQ - Error: {type(error).__name__}: {str(error)}"
            )
        except Exception as e:
            self.logger.error(f"Failed to write to DLQ: {e}")
    
    def _serialize_message(self, message: Any) -> dict:
        """Sérialiser un message pour sauvegarde"""
        import json
        
        if hasattr(message, 'value'):
            # Kafka message
            try:
                return {
                    'type': 'kafka_message',
                    'value': message.value().decode('utf-8') if message.value() else None,
                    'key': message.key().decode('utf-8') if message.key() else None,
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset()
                }
            except:
                return {'type': 'kafka_message', 'error': 'Failed to serialize'}
        elif isinstance(message, dict):
            return {'type': 'dict', 'data': message}
        else:
            return {'type': str(type(message)), 'data': str(message)}
    
    def count(self) -> int:
        """Compter le nombre de messages dans la DLQ"""
        try:
            with open(self.filepath, 'r') as f:
                return sum(1 for _ in f)
        except FileNotFoundError:
            return 0
    
    def read_all(self) -> list:
        """Lire tous les messages de la DLQ"""
        import json
        
        messages = []
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    messages.append(json.loads(line))
        except FileNotFoundError:
            pass
        
        return messages
    
    def clear(self):
        """Vider la DLQ"""
        try:
            with open(self.filepath, 'w') as f:
                pass
            self.logger.info("Dead Letter Queue cleared")
        except Exception as e:
            self.logger.error(f"Failed to clear DLQ: {e}")


class ErrorHandler:
    """Gestionnaire centralisé d'erreurs pour le pipeline"""
    
    def __init__(self, dlq: DeadLetterQueue, metrics_collector=None):
        self.dlq = dlq
        self.metrics = metrics_collector
        self.logger = logging.getLogger(__name__)
        
        # Circuit breakers pour différents composants
        self.kafka_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
            expected_exception=KafkaException
        )
        
        self.snowflake_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=120,
            expected_exception=sf_errors.Error
        )
    
    def handle_kafka_error(self, error: Exception, message=None, context: dict = None):
        """Gérer une erreur Kafka"""
        self.logger.error(f"Kafka error: {type(error).__name__}: {str(error)}")
        
        if self.metrics:
            self.metrics.increment('errors_kafka')
            self.metrics.increment('errors_total')
        
        if message:
            self.dlq.add(message, error, context or {'source': 'kafka'})
    
    def handle_snowflake_error(self, error: Exception, records=None, context: dict = None):
        """Gérer une erreur Snowflake"""
        self.logger.error(f"Snowflake error: {type(error).__name__}: {str(error)}")
        
        if self.metrics:
            self.metrics.increment('errors_snowflake')
            self.metrics.increment('errors_total')
        
        if records:
            for record in records:
                self.dlq.add(record, error, context or {'source': 'snowflake'})
    
    def handle_validation_error(self, error: Exception, record: dict, context: dict = None):
        """Gérer une erreur de validation"""
        self.logger.warning(f"Validation error: {type(error).__name__}: {str(error)}")
        
        if self.metrics:
            self.metrics.increment('errors_validation')
            self.metrics.increment('messages_rejected')
        
        self.dlq.add(record, error, context or {'source': 'validation'})
    
    def is_transient_error(self, error: Exception) -> bool:
        """Déterminer si une erreur est transitoire (devrait être retry)"""
        transient_errors = (
            ConnectionError,
            TimeoutError,
            sf_errors.OperationalError,
            sf_errors.InternalServerError,
        )
        
        return isinstance(error, transient_errors)
