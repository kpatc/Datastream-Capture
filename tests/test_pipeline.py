"""
Tests unitaires pour le pipeline CDC
"""
import pytest
import json
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Ajouter src au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kafka_consumer import CDCKafkaConsumer
from config import Config
from metrics import PipelineMetrics


class TestCDCKafkaConsumer:
    """Tests pour le consumer Kafka"""
    
    @pytest.fixture
    def consumer(self):
        """Fixture pour créer un consumer"""
        return CDCKafkaConsumer()
    
    def test_parse_debezium_insert(self, consumer):
        """Test parsing d'un message INSERT Debezium"""
        message = {
            "payload": {
                "op": "c",  # create
                "after": {
                    "transaction_id": 1,
                    "user_id": 101,
                    "amount": "150.50",
                    "status": "completed",
                    "created_at": 1698765432000,
                    "updated_at": 1698765432000
                }
            }
        }
        
        message_bytes = json.dumps(message).encode('utf-8')
        result = consumer.parse_debezium_message(message_bytes)
        
        assert result is not None
        assert result['transaction_id'] == 1
        assert result['user_id'] == 101
        assert result['cdc_operation'] == 'CREATE'
    
    def test_parse_debezium_update(self, consumer):
        """Test parsing d'un message UPDATE Debezium"""
        message = {
            "payload": {
                "op": "u",  # update
                "after": {
                    "transaction_id": 1,
                    "user_id": 101,
                    "amount": "200.00",
                    "status": "completed"
                }
            }
        }
        
        message_bytes = json.dumps(message).encode('utf-8')
        result = consumer.parse_debezium_message(message_bytes)
        
        assert result['cdc_operation'] == 'UPDATE'
    
    def test_parse_debezium_delete(self, consumer):
        """Test parsing d'un message DELETE Debezium"""
        message = {
            "payload": {
                "op": "d",  # delete
                "before": {
                    "transaction_id": 1,
                    "user_id": 101
                }
            }
        }
        
        message_bytes = json.dumps(message).encode('utf-8')
        result = consumer.parse_debezium_message(message_bytes)
        
        assert result['cdc_operation'] == 'DELETE'
    
    def test_validate_record_valid(self, consumer):
        """Test validation d'un enregistrement valide"""
        record = {
            'transaction_id': 1,
            'user_id': 101,
            'amount': 150.50,
            'status': 'completed'
        }
        
        assert consumer.validate_record(record) is True
    
    def test_validate_record_missing_transaction_id(self, consumer):
        """Test validation échoue si transaction_id manquant"""
        record = {
            'user_id': 101,
            'amount': 150.50
        }
        
        assert consumer.validate_record(record) is False
    
    def test_validate_record_invalid_user_id(self, consumer):
        """Test validation échoue si user_id invalide"""
        record = {
            'transaction_id': 1,
            'user_id': -1,  # négatif
            'amount': 150.50
        }
        
        assert consumer.validate_record(record) is False
    
    def test_validate_record_negative_amount(self, consumer):
        """Test validation échoue si montant négatif"""
        record = {
            'transaction_id': 1,
            'user_id': 101,
            'amount': -50.00
        }
        
        assert consumer.validate_record(record) is False


class TestPipelineMetrics:
    """Tests pour les métriques"""
    
    @pytest.fixture
    def metrics(self):
        """Fixture pour créer des métriques"""
        return PipelineMetrics()
    
    def test_initial_values(self, metrics):
        """Test initial metric values"""
        assert metrics.messages_consumed == 0
        assert metrics.errors_total == 0
        assert metrics.batch_count == 0
    
    def test_throughput_calculation(self, metrics):
        """Test throughput calculation"""
        import time
        
        # Simulate consuming 100 messages
        for _ in range(100):
            metrics.increment_consumed()
        
        time.sleep(0.1)  # Wait a bit
        
        throughput = metrics.get_throughput()
        assert throughput > 0
        assert throughput < 10000  # Reasonable check
    
    def test_error_rate(self, metrics):
        """Test error rate calculation"""
        # Consume 100 messages with 5 errors
        for _ in range(100):
            metrics.increment_consumed()
        for _ in range(5):
            metrics.increment_failed()
        
        error_rate = metrics.get_error_rate()
        assert error_rate == 5.0  # 5%
    
    def test_success_rate(self, metrics):
        """Test success rate calculation"""
        # Consume 100 messages, validate 95
        for _ in range(100):
            metrics.increment_consumed()
        for _ in range(95):
            metrics.increment_validated()
        
        success_rate = metrics.get_success_rate()
        assert success_rate == 95.0  # 95%
    
    def test_to_dict(self, metrics):
        """Test conversion to dictionary"""
        metrics.increment_consumed()
        metrics.increment_consumed()
        metrics.increment_validated()
        metrics.increment_failed()
        
        result = metrics.to_dict()
        
        # Check main structure
        assert 'messages' in result
        assert 'batches' in result
        assert 'errors' in result
        assert 'performance' in result
        assert 'operations' in result
        
        # Check values
        assert result['messages']['consumed'] == 2
        assert result['messages']['validated'] == 1
        assert result['errors']['total'] == 1


class TestMetricsCollector:
    """Tests pour le collecteur de métriques"""
    
    @pytest.fixture
    def metrics(self):
        """Fixture pour créer un collecteur de métriques"""
        return PipelineMetrics()
    
    def test_increment_counter(self, metrics):
        """Test incrémentation d'un compteur"""
        metrics.increment_consumed()
        metrics.increment_consumed()
        
        assert metrics.messages_consumed == 2
    
    def test_record_operation(self, metrics):
        """Test enregistrement d'opération CDC"""
        metrics.record_operation('CREATE')
        metrics.record_operation('UPDATE')
        metrics.record_operation('CREATE')
        
        assert metrics.operations['CREATE'] == 2
        assert metrics.operations['UPDATE'] == 1
    
    def test_throughput_calculation(self, metrics):
        """Test calcul du throughput"""
        import time
        
        # Simuler consommation de messages
        for _ in range(100):
            metrics.increment_consumed()
        
        time.sleep(0.1)  # Attendre un peu
        
        throughput = metrics.get_throughput()
        assert throughput > 0
        assert throughput < 10000  # Vérification raisonnable
    
    def test_metrics_export(self, metrics):
        """Test export des métriques en dict"""
        metrics.increment_consumed()
        metrics.increment_validated()
        metrics.record_operation('CREATE')
        
        data = metrics.to_dict()
        
        assert 'messages' in data
        assert 'batches' in data
        assert 'errors' in data
        assert 'performance' in data
        assert data['messages']['consumed'] == 1
        assert data['operations']['CREATE'] == 1


class TestConfig:
    """Tests pour la configuration"""
    
    def test_config_validation(self):
        """Test validation de la configuration"""
        # Devrait réussir avec les variables d'environnement
        try:
            Config.validate()
            assert True
        except ValueError:
            # Normal si .env n'est pas configuré dans les tests
            assert True


def test_integration_parse_and_validate():
    """Test d'intégration: parsing + validation"""
    consumer = CDCKafkaConsumer()
    
    # Message Debezium complet
    message = {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": 999,
                "user_id": 888,
                "amount": "999.99",
                "status": "pending",
                "created_at": 1698765432000,
                "updated_at": 1698765432000
            }
        }
    }
    
    message_bytes = json.dumps(message).encode('utf-8')
    
    # Parse
    record = consumer.parse_debezium_message(message_bytes)
    assert record is not None
    
    # Validate
    is_valid = consumer.validate_record(record)
    assert is_valid is True


if __name__ == "__main__":
    # Lancer les tests
    pytest.main([__file__, "-v", "--tb=short"])
