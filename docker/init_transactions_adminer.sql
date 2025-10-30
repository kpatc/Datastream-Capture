-- Script SQL pour créer et initialiser la table transactions
-- À exécuter dans Adminer (http://localhost:7775)
-- Base de données: postgres, Schéma: public

-- 1. Créer la table transactions
CREATE TABLE IF NOT EXISTS public.transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Insérer des données de test initiales
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
    (107, 500.00, 'completed'),
    (108, 95.00, 'cancelled'),
    (109, 380.75, 'completed'),
    (110, 265.50, 'pending'),
    (101, 420.00, 'completed'),
    (111, 155.25, 'failed');

-- 3. Vérifier que la table est créée et contient les données
SELECT COUNT(*) as total_transactions FROM public.transactions;

-- 4. Afficher quelques transactions
SELECT * FROM public.transactions ORDER BY transaction_id LIMIT 10;

-- 5. Vérifier la configuration WAL (nécessaire pour Debezium CDC)
SHOW wal_level;

-- Si wal_level n'est pas 'logical', exécutez cette commande (nécessite redémarrage):
-- ALTER SYSTEM SET wal_level = logical;
-- Puis redémarrer PostgreSQL: docker compose restart postgres
