#!/usr/bin/env python3
"""
Script pour tester la connexion Snowflake avec vos credentials
"""
import sys
import os

# Ajouter le chemin src
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config
from snowflake_connector import SnowflakeConnector


def test_snowflake_connection():
    """Tester la connexion à Snowflake"""
    print("=" * 60)
    print("Test de connexion Snowflake")
    print("=" * 60)
    print()
    
    try:
        # Valider la configuration
        print("1. Validation de la configuration...")
        Config.validate()
        print()
        
        # Créer le connecteur
        print("2. Création du connecteur Snowflake...")
        connector = SnowflakeConnector()
        print()
        
        # Tester la connexion
        print("3. Test de connexion...")
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            
            # Vérifier la version
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            print(f"   ✓ Snowflake version: {version}")
            
            # Vérifier le compte
            cursor.execute("SELECT CURRENT_ACCOUNT()")
            account = cursor.fetchone()[0]
            print(f"   ✓ Account: {account}")
            
            # Vérifier le user
            cursor.execute("SELECT CURRENT_USER()")
            user = cursor.fetchone()[0]
            print(f"   ✓ User: {user}")
            
            # Vérifier le role
            cursor.execute("SELECT CURRENT_ROLE()")
            role = cursor.fetchone()[0]
            print(f"   ✓ Role: {role}")
            
            # Vérifier la database
            cursor.execute("SELECT CURRENT_DATABASE()")
            db = cursor.fetchone()[0]
            print(f"   ✓ Database: {db}")
            
            # Vérifier le warehouse
            cursor.execute("SELECT CURRENT_WAREHOUSE()")
            wh = cursor.fetchone()[0]
            print(f"   ✓ Warehouse: {wh}")
            
            cursor.close()
        
        print()
        print("4. Création de la table TRANSACTIONS...")
        connector.create_table_if_not_exists()
        print()
        
        # Vérifier que la table existe
        print("5. Vérification de la table...")
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
                print(f"   ✓ Table {Config.SNOWFLAKE_TABLE} existe")
                
                # Compter les enregistrements
                count = connector.get_record_count()
                print(f"   ✓ Nombre d'enregistrements: {count}")
            else:
                print(f"   ✗ Table {Config.SNOWFLAKE_TABLE} n'existe pas")
            
            cursor.close()
        
        print()
        print("=" * 60)
        print("✓ TOUS LES TESTS SONT PASSÉS AVEC SUCCÈS!")
        print("=" * 60)
        print()
        print("Vous pouvez maintenant lancer le pipeline:")
        print("  ./scripts/run_pipeline.sh")
        print()
        
        return True
        
    except Exception as e:
        print()
        print("=" * 60)
        print("✗ ERREUR DE CONNEXION")
        print("=" * 60)
        print(f"Erreur: {e}")
        print()
        print("Vérifiez:")
        print("  1. Vos credentials dans .env sont corrects")
        print("  2. Votre compte Snowflake est actif")
        print("  3. Le warehouse COMPUTE_WH existe et est disponible")
        print("  4. La database CDC_DB existe")
        print()
        return False


if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
