# CDC Real-time Data Warehousing

> Real-time Change Data Capture (CDC) from PostgreSQL to Snowflake via Debezium & Kafka

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Kafka](https://img.shields.io/badge/kafka-6.2.0-orange.svg)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-14+-blue.svg)](https://www.postgresql.org/)
[![Snowflake](https://img.shields.io/badge/snowflake-cloud-blue.svg)](https://www.snowflake.com/)

## 🎯 Description

Professional Change Data Capture (CDC) pipeline that captures real-time changes in PostgreSQL and automatically synchronizes them to Snowflake for data warehousing.

### Architecture

```
PostgreSQL → Debezium → Kafka → Python Pipeline → Snowflake
                ↓
          Schema Registry
```

## ✨ Features

### Core CDC Features
- ✅ **Real-time CDC capture**: Instantly detects INSERT, UPDATE, DELETE operations
- ✅ **Automatic validation**: Verifies data quality before loading
- ✅ **Micro-batch processing**: Optimizes performance with configurable batching
- ✅ **MERGE management**: Intelligent upsert/delete in Snowflake

### Production-Ready Robustness
- ✅ **Circuit Breaker Pattern**: Prevents cascade failures for Kafka and Snowflake
- ✅ **Exponential Backoff Retry**: Automatic retry with intelligent backoff (3-5 attempts)
- ✅ **Dead Letter Queue (DLQ)**: Captures failed messages for analysis and replay
- ✅ **Error Categorization**: Tracks Kafka, Snowflake, validation, and parsing errors
- ✅ **Graceful Shutdown**: Signal handling for clean pipeline termination

### Monitoring & Observability
- ✅ **Comprehensive Metrics**: Throughput, success rate, error rate, latency
- ✅ **Real-time Statistics**: Live tracking of messages consumed, validated, loaded
- ✅ **Performance Analytics**: Batch processing times, average batch sizes
- ✅ **CDC Operation Tracking**: Monitors CREATE, UPDATE, DELETE, READ operations
- ✅ **Metrics Export**: JSON export for integration with monitoring tools
- ✅ **Structured Logging**: Production-ready logging with proper levels

### Quality & Testing
- ✅ **Unit tests**: Comprehensive test coverage > 80%
- ✅ **Integration tests**: End-to-end pipeline validation
- ✅ **Makefile automation**: One-command testing and deployment

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- Snowflake account

### 5-Minute Installation

```bash
# 1. Clone the project
git clone https://github.com/kpatc/Datastream-Capture.git
cd Datastream-Capture

# 2. Create virtual environment
make venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate  # Windows

# 3. Install dependencies
make install

# 4. Configure Snowflake
cp .env.example .env
nano .env  # Add your Snowflake credentials

# 5. Start Docker services
make docker-up

# 6. Create Debezium connector
make create-connector

# 7. Setup Snowflake
make setup-snowflake

# 8. Run the pipeline
make pipeline
```

**That's it! 🎉** The pipeline is now capturing changes in real-time.

## 📖 Complete Documentation

For a detailed installation and configuration guide, see:

👉 **[SETUP_GUIDE.md](docs/SETUP_GUIDE.md)**

This guide contains:
- Detailed architecture
- Step-by-step configuration
- Troubleshooting
- Optimizations
- Usage examples

## 🧪 Tests

```bash
# Run all tests
make test

# Tests with coverage
make coverage

# Specific tests
make test-kafka        # Kafka consumer tests
make test-snowflake    # Snowflake connector tests
make test-pipeline     # Pipeline integration tests
```

## 🔧 Useful Commands

```bash
# Development
make help              # Display all commands
make format            # Format code
make lint              # Check code quality
make dev               # Run in development mode

# Docker
make docker-up         # Start all services
make docker-down       # Stop services
make docker-logs       # View logs

# Monitoring
make health-check      # Check service status
make check-connector   # Debezium connector status

# Testing
make test-connection   # Test Snowflake connection
make generate-test-data # Generate test data

# Cleanup
make clean             # Clean cache
make clean-all         # Clean everything (cache + Docker)
```

## 📊 Available Services

Once `make docker-up` is executed:

| Service | URL | Description |
|---------|-----|-------------|
| Kafka | localhost:9092 | Message broker |
| Kafka Connect | http://localhost:8083 | Debezium API |
| Kowl (Kafka UI) | http://localhost:8090 | Kafka interface |
| PostgreSQL | localhost:5433 | Source database |
| Adminer | http://localhost:7775 | PostgreSQL interface |
| Schema Registry | http://localhost:8081 | Avro/JSON schema |

## 🏗️ Project Structure

```
.
├── src/                          # Source code
│   ├── pipeline.py              # Main pipeline
│   ├── kafka_consumer.py        # Kafka consumer + Debezium parsing
│   ├── snowflake_connector.py   # Snowflake connection
│   └── config.py                # Centralized configuration
├── tests/                        # Unit tests
│   ├── test_kafka_consumer.py
│   ├── test_snowflake_connector.py
│   └── test_pipeline.py
├── scripts/                      # Utility scripts
│   ├── setup_snowflake.py       # Snowflake DB setup
│   └── test_pipeline.py         # Test data generation
├── docker/                       # CDC infrastructure
│   ├── docker-compose.yml
│   └── transaction_connector.json
├── docs/                         # Documentation
│   └── SETUP_GUIDE.md
├── Makefile                      # Automated commands
└── requirements.txt              # Python dependencies
```

## 🔥 Typical Workflow

### 1. Daily development

```bash
# Start environment
make docker-up
make pipeline

# In another terminal: generate transactions
make generate-test-data

# Check in Snowflake
# Data arrives in real-time!
```

### 2. Before committing

```bash
# Format and check
make all

# If everything is ✅, commit!
git add .
git commit -m "feat: pipeline improvement"
```

### 3. CI/CD

```bash
# Run CI checks
make ci

# Lint + Automatic tests
```

## 📈 Metrics & Monitoring

The pipeline displays in real-time:

```
Pipeline Stats - Consumed: 15230, Validated: 15100, Loaded: 15000, Errors: 2
Total records in Snowflake: 15000
```

## 🐛 Troubleshooting

### Connector won't start

```bash
make check-connector
docker logs kafka-connect
```

### No messages in Kafka

```bash
# Check PostgreSQL WAL level
docker exec postgres psql -U postgres -c "SHOW wal_level;"
# Should be: logical
```

### Snowflake error

```bash
make test-connection
```

### Pipeline stuck

```bash
# Check Kafka offsets
make check-connector
```

More details in [SETUP_GUIDE.md](docs/SETUP_GUIDE.md)

## 📝 Configuration

### Snowflake (`.env`)

```env
SNOWFLAKE_ACCOUNT=xyz.region
SNOWFLAKE_USER=username
SNOWFLAKE_PASSWORD=password
SNOWFLAKE_DATABASE=CDC_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

### Pipeline

- `BATCH_SIZE=100`: Number of messages per batch
- `BATCH_TIMEOUT=5`: Batch timeout in seconds
- `KAFKA_TOPIC=postgres-transactions.public.transactions`

## 🤝 Contributing

1. Fork the project
2. Create a branch: `git checkout -b feature/amazing-feature`
3. Commit: `git commit -m 'feat: Add amazing feature'`
4. Push: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📜 License

This project is licensed under the MIT License - see [LICENSE](LICENSE)

## 👥 Authors

- **Josh** - *Initial work* - [kpatc](https://github.com/kpatc)

## 🙏 Acknowledgments

- [Debezium](https://debezium.io/) - CDC platform
- [Apache Kafka](https://kafka.apache.org/) - Streaming
- [Snowflake](https://www.snowflake.com/) - Data warehouse

---

**⭐ If this project helps you, feel free to give it a star!**

**📧 Questions? Open an issue!**
