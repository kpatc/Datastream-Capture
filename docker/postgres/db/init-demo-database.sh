#!/bin/bash
# Script d'initialisation PostgreSQL pour CDC Real-time Data Warehouse
# Ce script s'exécute automatiquement au premier démarrage du container

set -e
export PGPASSWORD=$POSTGRES_PASSWORD;

echo "========================================="
echo "Initialisation de la base PostgreSQL CDC"
echo "========================================="

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  -- Activer la réplication logique (nécessaire pour Debezium)
  ALTER SYSTEM SET wal_level = logical;
  
  -- Créer la table transactions dans le schema public
  CREATE TABLE IF NOT EXISTS public.transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  
  -- Insérer des données de test initiales
  INSERT INTO public.transactions (user_id, amount, status) VALUES
    (101, 150.50, 'completed'),
    (102, 200.00, 'pending'),
    (103, 75.25, 'completed'),
    (101, 300.00, 'completed'),
    (104, 125.75, 'pending'),
    (105, 450.00, 'completed'),
    (102, 80.50, 'failed'),
    (106, 220.00, 'pending'),
    (103, 175.25, 'completed'),
    (107, 500.00, 'completed');
  
  -- Afficher un résumé
  SELECT 'Table transactions créée avec ' || COUNT(*) || ' enregistrements initiaux' as status 
  FROM public.transactions;
  
EOSQL

echo "✓ Initialisation terminée avec succès!"
echo "✓ Table 'transactions' créée dans le schema 'public'"
echo "✓ WAL level configuré en 'logical' pour Debezium CDC"