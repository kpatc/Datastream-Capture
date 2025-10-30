#!/usr/bin/env python3
"""
Script to test Snowflake connection with your credentials
"""
import sys
import os

# Add src path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config
from snowflake_connector import SnowflakeConnector


def test_snowflake_connection():
    """Test Snowflake connection"""
    print("=" * 60)
    print("Snowflake Connection Test")
    print("=" * 60)
    print()
    
    try:
        # Validate configuration
        print("1. Validating configuration...")
        Config.validate()
        print()
        
        # Create connector
        print("2. Creating Snowflake connector...")
        connector = SnowflakeConnector()
        print()
        
        # Test connection
        print("3. Testing connection...")
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check version
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            print(f"   ✓ Snowflake version: {version}")
            
            # Check account
            cursor.execute("SELECT CURRENT_ACCOUNT()")
            account = cursor.fetchone()[0]
            print(f"   ✓ Account: {account}")
            
            # Check user
            cursor.execute("SELECT CURRENT_USER()")
            user = cursor.fetchone()[0]
            print(f"   ✓ User: {user}")
            
            # Check role
            cursor.execute("SELECT CURRENT_ROLE()")
            role = cursor.fetchone()[0]
            print(f"   ✓ Role: {role}")
            
            # Check database
            cursor.execute("SELECT CURRENT_DATABASE()")
            db = cursor.fetchone()[0]
            print(f"   ✓ Database: {db}")
            
            # Check warehouse
            cursor.execute("SELECT CURRENT_WAREHOUSE()")
            wh = cursor.fetchone()[0]
            print(f"   ✓ Warehouse: {wh}")
            
            cursor.close()
        
        print()
        print("4. Creating TRANSACTIONS table...")
        connector.create_table_if_not_exists()
        print()
        
        # Verify table exists
        print("5. Verifying table...")
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {Config.SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{Config.SNOWFLAKE_SCHEMA}' 
                AND TABLE_NAME = '{Config.SNOWFLAKE_TABLE}'
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                print(f"   ✓ Table {Config.SNOWFLAKE_TABLE} exists")
                
                # Count records
                count = connector.get_record_count()
                print(f"   ✓ Number of records: {count}")
            else:
                print(f"   ✗ Table {Config.SNOWFLAKE_TABLE} does not exist")
            
            cursor.close()
        
        print()
        print("=" * 60)
        print("✓ ALL TESTS PASSED SUCCESSFULLY!")
        print("=" * 60)
        print()
        print("You can now run the pipeline:")
        print("  ./scripts/run_pipeline.sh")
        print()
        
        return True
        
    except Exception as e:
        print()
        print("=" * 60)
        print("✗ CONNECTION ERROR")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        print("Verify:")
        print("  1. Your credentials in .env are correct")
        print("  2. Your Snowflake account is active")
        print("  3. The warehouse COMPUTE_WH exists and is available")
        print("  4. The database CDC_DB exists")
        print()
        return False


if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
