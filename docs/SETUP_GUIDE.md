# CDC to Snowflake - Guide de Configuration

## Vue d'ensemble

Ce projet implémente un pipeline de Change Data Capture (CDC) en temps réel qui capture les changements dans une table PostgreSQL `transactions` et les charge automatiquement dans Snowflake pour le data warehousing.

### Architecture

```
PostgreSQL (Source)
    ↓
Debezium CDC Connector
    ↓
Apache Kafka (Streaming)
    ↓
Schema Registry (Validation)
    ↓
Pipeline Python (Validation & Transformation)
    ↓
Snowflake (Data Warehouse)
```

## Prérequis

1. **Docker & Docker Compose** - Pour l'infrastructure CDC
2. **Python 3.8+** - Pour le pipeline
3. **Compte Snowflake** - Pour le stockage des données

## Installation

### Étape 1: Configuration de l'infrastructure CDC

```bash
# Démarrer tous les services
cd docker
docker compose up -d

# Vérifier que tous les services sont actifs
docker compose ps
```

Services disponibles:
- **Kafka** (localhost:9092)
- **Zookeeper** (localhost:2181)
- **Schema Registry** (localhost:8081)
- **Kafka Connect** (localhost:8083)
- **PostgreSQL** (localhost:5433)
- **Adminer** (localhost:7775) - Interface PostgreSQL
- **Kowl** (localhost:8080) - Interface Kafka

### Étape 2: Créer le connecteur Debezium

```bash
# Créer le connecteur pour la table transactions
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
http://localhost:8083/connectors/ -d @transaction_connector.json

# Vérifier le statut du connecteur
curl http://localhost:8083/connectors/postgres-cdc-transactions/status | jq
```

### Étape 3: Configuration Snowflake

#### 3.1. Exécuter le script SQL de setup

Connectez-vous à Snowflake et exécutez:
```bash
# Utiliser le script fourni
cat scripts/setup_snowflake.sql
```

Ou via SnowSQL:
```bash
snowsql -a <your_account> -u <your_user> -f scripts/setup_snowflake.sql
```

#### 3.2. Configurer les credentials

```bash
# Copier le template
cp .env.example .env

# Éditer avec vos credentials Snowflake
nano .env
```

Remplir les variables:
```env
SNOWFLAKE_ACCOUNT=xyz12345.us-east-1
SNOWFLAKE_USER=votre_username
SNOWFLAKE_PASSWORD=votre_password
SNOWFLAKE_DATABASE=CDC_WAREHOUSE
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Étape 4: Installation du pipeline Python

```bash
# Créer un environnement virtuel
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt
```

## Utilisation

### Lancer le pipeline

```bash
# Option 1: Script automatique
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh

# Option 2: Manuel
source .venv/bin/activate
cd src
python pipeline.py
```

### Générer des données de test

```bash
# Lancer le script de test
python scripts/test_pipeline.py
```

Le script propose:
1. Générer des transactions (INSERT)
2. Modifier des transactions (UPDATE)
3. Supprimer des transactions (DELETE)
4. Générer une charge continue

### Vérifier les données dans Snowflake

```sql
-- Voir toutes les transactions
SELECT * FROM CDC_WAREHOUSE.PUBLIC.TRANSACTIONS
ORDER BY cdc_timestamp DESC
LIMIT 100;

-- Voir uniquement les transactions actives (non supprimées)
SELECT * FROM CDC_WAREHOUSE.PUBLIC.ACTIVE_TRANSACTIONS
LIMIT 100;

-- Analytics par jour et statut
SELECT * FROM CDC_WAREHOUSE.PUBLIC.TRANSACTION_ANALYTICS
ORDER BY transaction_date DESC;

-- Résumé par utilisateur
SELECT * FROM CDC_WAREHOUSE.PUBLIC.USER_TRANSACTION_SUMMARY
LIMIT 50;
```

## Fonctionnalités du Pipeline

### Validation des données

Le pipeline valide automatiquement:
- ✅ `transaction_id` existe et est unique
- ✅ `user_id` est un entier positif
- ✅ `amount` est un nombre valide (si présent)
- ✅ `status` fait partie des valeurs autorisées

### Gestion des opérations CDC

Le pipeline gère tous les types d'opérations:
- **CREATE** (c) - Nouvelles transactions → INSERT dans Snowflake
- **UPDATE** (u) - Modifications → UPDATE dans Snowflake
- **DELETE** (d) - Suppressions → DELETE ou marquage dans Snowflake
- **READ** (r) - Snapshot initial → INSERT dans Snowflake

### Traitement par batch

- Taille de batch configurable (défaut: 100 records)
- Timeout configurable (défaut: 5 secondes)
- Commit atomique après chargement réussi

## Monitoring

### Vérifier l'état du pipeline

```bash
# Logs du pipeline
tail -f logs/pipeline.log  # Si configuré

# Statistiques en temps réel
# Le pipeline affiche automatiquement:
# - Messages consommés
# - Records validés
# - Records chargés dans Snowflake
# - Erreurs
```

### Monitorer Kafka

Accéder à Kowl UI: http://localhost:8080
- Voir les topics
- Consulter les messages
- Vérifier les consumer groups

### Monitorer PostgreSQL

Accéder à Adminer: http://localhost:7775
- Server: postgres
- Username: postgres
- Password: postgres
- Database: postgres

## Architecture du Code

```
cdc_realtime_project/
├── src/
│   ├── config.py              # Configuration centralisée
│   ├── kafka_consumer.py      # Consumer Kafka + parsing Debezium
│   ├── snowflake_connector.py # Connexion et chargement Snowflake
│   └── pipeline.py            # Orchestration principale
├── scripts/
│   ├── setup_snowflake.sql    # Setup Snowflake DB/Tables
│   ├── test_pipeline.py       # Génération de données test
│   └── run_pipeline.sh        # Lancement automatique
├── docker/
│   ├── docker-compose.yml     # Infrastructure CDC
│   └── transaction_connector.json  # Config Debezium
├── .env.example               # Template configuration
└── requirements.txt           # Dépendances Python
```

## Troubleshooting

### Le connecteur Debezium ne démarre pas

```bash
# Vérifier les logs
docker logs kafka-connect

# Vérifier le statut
curl http://localhost:8083/connectors/postgres-cdc-transactions/status | jq

# Recréer le connecteur
curl -X DELETE http://localhost:8083/connectors/postgres-cdc-transactions
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
http://localhost:8083/connectors/ -d @transaction_connector.json
```

### Pas de messages dans Kafka

```bash
# Vérifier que le WAL est activé dans PostgreSQL
docker exec -it postgres psql -U postgres -c "SHOW wal_level;"
# Doit être: logical

# Vérifier la réplication
docker exec -it postgres psql -U postgres -c "SELECT * FROM pg_replication_slots;"
```

### Erreurs de connexion Snowflake

```bash
# Tester la connexion
python -c "
from src.config import Config
from src.snowflake_connector import SnowflakeConnector
Config.validate()
conn = SnowflakeConnector()
with conn.get_connection() as c:
    print('✓ Connexion Snowflake réussie!')
"
```

### Consumer Kafka en retard

```bash
# Vérifier le lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe \
  --group snowflake-consumer-group
```

## Optimisations

### Performance Snowflake

1. **Augmenter la taille du warehouse**
```sql
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'SMALL';
```

2. **Clustering sur la table**
```sql
ALTER TABLE TRANSACTIONS CLUSTER BY (created_at, user_id);
```

3. **Créer des index**
```sql
-- Snowflake n'a pas d'index traditionnels, mais utilise le clustering
-- et les search optimization services
ALTER TABLE TRANSACTIONS ADD SEARCH OPTIMIZATION;
```

### Performance Pipeline

Ajuster dans `.env`:
```env
BATCH_SIZE=500          # Augmenter pour plus de throughput
BATCH_TIMEOUT=10        # Augmenter pour des batchs plus gros
```

## Prochaines Étapes

1. **Alerting**: Intégrer avec Slack/Email pour les erreurs
2. **Metrics**: Ajouter Prometheus + Grafana
3. **Data Quality**: Ajouter Great Expectations
4. **Schema Evolution**: Gérer les changements de schéma
5. **Multi-tables**: Étendre à d'autres tables

## Support

Pour toute question ou problème:
1. Vérifier les logs: `docker compose logs -f`
2. Consulter la documentation Debezium: https://debezium.io/
3. Vérifier le statut des services: `docker compose ps`

## License

Ce projet est un exemple éducatif pour le CDC en temps réel.
