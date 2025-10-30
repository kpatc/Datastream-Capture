#!/usr/bin/env python3
"""
Script pour créer la database et les tables dans Snowflake
"""
import sys
import os

# Ajouter le chemin src
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config
import snowflake.connector


def setup_snowflake_database():
    """Créer la database, schema et tables dans Snowflake"""
    print("=" * 60)
    print("Setup Snowflake Database pour CDC")
    print("=" * 60)
    print()
    
    try:
        # Connexion sans spécifier de database
        print("1. Connexion à Snowflake...")
        conn = snowflake.connector.connect(
            account=Config.SNOWFLAKE_ACCOUNT,
            user=Config.SNOWFLAKE_USER,
            password=Config.SNOWFLAKE_PASSWORD,
            warehouse=Config.SNOWFLAKE_WAREHOUSE,
            role=Config.SNOWFLAKE_ROLE
        )
        cursor = conn.cursor()
        print("   ✓ Connecté")
        print()
        
        # Créer la database
        print(f"2. Création de la database {Config.SNOWFLAKE_DATABASE}...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Config.SNOWFLAKE_DATABASE}")
        print(f"   ✓ Database {Config.SNOWFLAKE_DATABASE} créée ou existe déjà")
        
        # Utiliser la database
        cursor.execute(f"USE DATABASE {Config.SNOWFLAKE_DATABASE}")
        print()
        
        # Créer le schema
        print(f"3. Création du schema {Config.SNOWFLAKE_SCHEMA}...")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {Config.SNOWFLAKE_SCHEMA}")
        cursor.execute(f"USE SCHEMA {Config.SNOWFLAKE_SCHEMA}")
        print(f"   ✓ Schema {Config.SNOWFLAKE_SCHEMA} créé ou existe déjà")
        print()
        
        # Créer la table transactions
        print("4. Création de la table TRANSACTIONS...")
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
        print(f"   ✓ Table {Config.SNOWFLAKE_TABLE} créée")
        print()
        
        # Créer la vue ACTIVE_TRANSACTIONS
        print("5. Création de la vue ACTIVE_TRANSACTIONS...")
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
        print("   ✓ Vue TRANSACTION_ANALYTICS créée")
        print()
        
        # Créer la vue USER_TRANSACTION_SUMMARY
        print("7. Création de la vue USER_TRANSACTION_SUMMARY...")
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
        print("   ✓ Vue USER_TRANSACTION_SUMMARY créée")
        print()
        
        # Vérifier les objets créés
        print("8. Vérification des objets créés...")
        cursor.execute(f"SHOW TABLES IN SCHEMA {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}")
        tables = cursor.fetchall()
        print(f"   ✓ Tables créées: {len(tables)}")
        for table in tables:
            print(f"     - {table[1]}")
        
        cursor.execute(f"SHOW VIEWS IN SCHEMA {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}")
        views = cursor.fetchall()
        print(f"   ✓ Vues créées: {len(views)}")
        for view in views:
            print(f"     - {view[1]}")
        
        cursor.close()
        conn.close()
        
        print()
        print("=" * 60)
        print("✓ SETUP SNOWFLAKE TERMINÉ AVEC SUCCÈS!")
        print("=" * 60)
        print()
        print("Vous pouvez maintenant:")
        print("  1. Tester la connexion: python test_snowflake_connection.py")
        print("  2. Lancer le pipeline: ../scripts/run_pipeline.sh")
        print()
        
        return True
        
    except Exception as e:
        print()
        print("=" * 60)
        print("✗ ERREUR LORS DU SETUP")
        print("=" * 60)
        print(f"Erreur: {e}")
        print()
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = setup_snowflake_database()
    sys.exit(0 if success else 1)
