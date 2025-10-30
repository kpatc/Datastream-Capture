#!/usr/bin/env python3
"""
Script to create database and tables in Snowflake
"""
import sys
import os

# Add src path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config
import snowflake.connector


def setup_snowflake_database():
    """Create database, schema and tables in Snowflake"""
    print("=" * 60)
    print("Setup Snowflake Database for CDC")
    print("=" * 60)
    print()
    
    try:
        # Connect without specifying database
        print("1. Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=Config.SNOWFLAKE_ACCOUNT,
            user=Config.SNOWFLAKE_USER,
            password=Config.SNOWFLAKE_PASSWORD,
            warehouse=Config.SNOWFLAKE_WAREHOUSE,
            role=Config.SNOWFLAKE_ROLE
        )
        cursor = conn.cursor()
        print("   ✓ Connected")
        print()
        
        # Create database
        print(f"2. Creating database {Config.SNOWFLAKE_DATABASE}...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Config.SNOWFLAKE_DATABASE}")
        print(f"   ✓ Database {Config.SNOWFLAKE_DATABASE} created or already exists")
        
        # Use database
        cursor.execute(f"USE DATABASE {Config.SNOWFLAKE_DATABASE}")
        print()
        
        # Create schema
        print(f"3. Creating schema {Config.SNOWFLAKE_SCHEMA}...")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {Config.SNOWFLAKE_SCHEMA}")
        cursor.execute(f"USE SCHEMA {Config.SNOWFLAKE_SCHEMA}")
        print(f"   ✓ Schema {Config.SNOWFLAKE_SCHEMA} created or already exists")
        print()
        
        # Create transactions table
        print("4. Creating TRANSACTIONS table...")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {Config.SNOWFLAKE_TABLE} (
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
        """
        cursor.execute(create_table_sql)
        print(f"   ✓ Table {Config.SNOWFLAKE_TABLE} created")
        print()
        
        # Create ACTIVE_TRANSACTIONS view
        print("5. Creating ACTIVE_TRANSACTIONS view...")
        cursor.execute("""
        CREATE OR REPLACE VIEW ACTIVE_TRANSACTIONS AS
        SELECT 
            transaction_id,
            user_id,
            amount,
            status,
            created_at,
            updated_at,
            cdc_timestamp
        FROM TRANSACTIONS
        WHERE cdc_operation != 'DELETE'
        ORDER BY updated_at DESC
        """)
        print("   ✓ Vue ACTIVE_TRANSACTIONS créée")
        print()
        
        # Créer la vue TRANSACTION_ANALYTICS
        print("6. Création de la vue TRANSACTION_ANALYTICS...")
        cursor.execute("""
        CREATE OR REPLACE VIEW TRANSACTION_ANALYTICS AS
        SELECT 
            DATE(created_at) AS transaction_date,
            status,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            MIN(amount) AS min_amount,
            MAX(amount) AS max_amount
        FROM TRANSACTIONS
        WHERE cdc_operation != 'DELETE'
        GROUP BY DATE(created_at), status
        ORDER BY transaction_date DESC, status
        """)
        print("   ✓ View TRANSACTION_ANALYTICS created")
        print()
        
        # Create USER_TRANSACTION_SUMMARY view
        print("7. Creating USER_TRANSACTION_SUMMARY view...")
        cursor.execute("""
        CREATE OR REPLACE VIEW USER_TRANSACTION_SUMMARY AS
        SELECT 
            user_id,
            COUNT(*) AS total_transactions,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_transaction_amount,
            MAX(updated_at) AS last_transaction_date
        FROM TRANSACTIONS
        WHERE cdc_operation != 'DELETE'
        GROUP BY user_id
        ORDER BY total_amount DESC
        """)
        print("   ✓ View USER_TRANSACTION_SUMMARY created")
        print()
        
        # Verify created objects
        print("8. Verifying created objects...")
        cursor.execute(f"SHOW TABLES IN SCHEMA {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}")
        tables = cursor.fetchall()
        print(f"   ✓ Tables created: {len(tables)}")
        for table in tables:
            print(f"     - {table[1]}")
        
        cursor.execute(f"SHOW VIEWS IN SCHEMA {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}")
        views = cursor.fetchall()
        print(f"   ✓ Views created: {len(views)}")
        for view in views:
            print(f"     - {view[1]}")
        
        cursor.close()
        conn.close()
        
        print()
        print("=" * 60)
        print("✓ SNOWFLAKE SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print()
        print("You can now:")
        print("  1. Test connection: python test_snowflake_connection.py")
        print("  2. Run pipeline: ../scripts/run_pipeline.sh")
        print()
        
        return True
        
    except Exception as e:
        print()
        print("=" * 60)
        print("✗ SETUP ERROR")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = setup_snowflake_database()
    sys.exit(0 if success else 1)
