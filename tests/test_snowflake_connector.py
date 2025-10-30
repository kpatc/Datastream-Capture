"""
Tests unitaires pour snowflake_connector.py
"""
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from decimal import Decimal
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from snowflake_connector import SnowflakeConnector


class TestSnowflakeConnector:
    """Tests for SnowflakeConnector class"""
    
    @pytest.fixture
    def connector(self):
        """Create a connector instance for testing"""
        with patch('snowflake_connector.Config'):
            connector = SnowflakeConnector()
            connector.config.SNOWFLAKE_ACCOUNT = 'test_account'
            connector.config.SNOWFLAKE_USER = 'test_user'
            connector.config.SNOWFLAKE_PASSWORD = 'test_pass'
            connector.config.SNOWFLAKE_DATABASE = 'TEST_DB'
            connector.config.SNOWFLAKE_SCHEMA = 'PUBLIC'
            connector.config.SNOWFLAKE_WAREHOUSE = 'TEST_WH'
            connector.config.SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
            connector.config.SNOWFLAKE_TABLE = 'TRANSACTIONS'
            return connector
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_get_connection_success(self, mock_connect, connector):
        """Test successful Snowflake connection"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        with connector.get_connection() as conn:
            assert conn is not None
        
        mock_connect.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_get_connection_failure(self, mock_connect, connector):
        """Test connection failure handling"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception):
            with connector.get_connection() as conn:
                pass
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_create_table_if_not_exists(self, mock_connect, connector):
        """Test table creation"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        connector.create_table_if_not_exists()
        
        mock_cursor.execute.assert_called_once()
        assert 'CREATE TABLE IF NOT EXISTS' in mock_cursor.execute.call_args[0][0]
        mock_cursor.close.assert_called_once()
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_insert_batch_empty(self, mock_connect, connector):
        """Test insert with empty records list"""
        connector.insert_batch([])
        
        # Should not attempt connection
        mock_connect.assert_not_called()
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_insert_batch_create_operation(self, mock_connect, connector):
        """Test inserting records with CREATE operation"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        records = [
            {
                'transaction_id': 1,
                'user_id': 101,
                'amount': 150.50,
                'status': 'completed',
                'created_at': '2025-10-30 14:00:00',
                'updated_at': '2025-10-30 14:00:00',
                'cdc_operation': 'CREATE'
            },
            {
                'transaction_id': 2,
                'user_id': 102,
                'amount': 200.00,
                'status': 'pending',
                'created_at': '2025-10-30 14:01:00',
                'updated_at': '2025-10-30 14:01:00',
                'cdc_operation': 'CREATE'
            }
        ]
        
        connector.insert_batch(records)
        
        # Should execute MERGE for each record
        assert mock_cursor.execute.call_count == len(records)
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_insert_batch_update_operation(self, mock_connect, connector):
        """Test inserting records with UPDATE operation"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        records = [{
            'transaction_id': 1,
            'user_id': 101,
            'amount': 999.99,
            'status': 'completed',
            'created_at': '2025-10-30 14:00:00',
            'updated_at': '2025-10-30 15:00:00',
            'cdc_operation': 'UPDATE'
        }]
        
        connector.insert_batch(records)
        
        mock_cursor.execute.assert_called_once()
        assert 'MERGE' in mock_cursor.execute.call_args[0][0]
        mock_conn.commit.assert_called_once()
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_insert_batch_delete_operation(self, mock_connect, connector):
        """Test inserting records with DELETE operation"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        records = [{
            'transaction_id': 3,
            'user_id': 103,
            'amount': 75.25,
            'status': 'completed',
            'created_at': '2025-10-30 14:00:00',
            'updated_at': '2025-10-30 14:00:00',
            'cdc_operation': 'DELETE'
        }]
        
        connector.insert_batch(records)
        
        mock_cursor.execute.assert_called_once()
        # DELETE operation should trigger DELETE in MERGE
        assert 'DELETE' in mock_cursor.execute.call_args[0][0].upper()
        mock_conn.commit.assert_called_once()
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_insert_batch_error_rollback(self, mock_connect, connector):
        """Test rollback on insert error"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Insert failed")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        records = [{
            'transaction_id': 1,
            'user_id': 101,
            'amount': 150.50,
            'status': 'completed',
            'created_at': '2025-10-30 14:00:00',
            'updated_at': '2025-10-30 14:00:00',
            'cdc_operation': 'CREATE'
        }]
        
        with pytest.raises(Exception):
            connector.insert_batch(records)
        
        mock_conn.rollback.assert_called_once()
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_get_record_count(self, mock_connect, connector):
        """Test getting record count"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'COUNT': 100}
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        count = connector.get_record_count()
        
        assert count == 100
        mock_cursor.execute.assert_called_once()
        assert 'SELECT COUNT(*)' in mock_cursor.execute.call_args[0][0]
    
    @patch('snowflake_connector.snowflake.connector.connect')
    def test_get_record_count_error(self, mock_connect, connector):
        """Test get_record_count returns 0 on error"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        count = connector.get_record_count()
        
        assert count == 0
    
    def test_connection_parameters(self, connector):
        """Test connection uses correct parameters"""
        with patch('snowflake_connector.snowflake.connector.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            with connector.get_connection():
                pass
            
            call_args = mock_connect.call_args
            assert call_args[1]['account'] == 'test_account'
            assert call_args[1]['user'] == 'test_user'
            assert call_args[1]['password'] == 'test_pass'
            assert call_args[1]['database'] == 'TEST_DB'
            assert call_args[1]['schema'] == 'PUBLIC'
            assert call_args[1]['warehouse'] == 'TEST_WH'
            assert call_args[1]['role'] == 'ACCOUNTADMIN'
