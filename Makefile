.PHONY: help test coverage lint format clean install dev docker-up docker-down docker-restart all

help:
	@echo "=========================================="
	@echo "CDC Real-time Data Warehousing - Makefile"
	@echo "=========================================="
	@echo ""
	@echo "Available commands:"
	@echo ""
	@echo "  📦 Dependencies & Setup:"
	@echo "    make install          - Install Python dependencies"
	@echo "    make venv             - Create virtual environment"
	@echo ""
	@echo "  🧪 Testing:"
	@echo "    make test             - Run all tests"
	@echo "    make test-unit        - Run unit tests only"
	@echo "    make test-kafka       - Test Kafka consumer"
	@echo "    make test-snowflake   - Test Snowflake connector"
	@echo "    make coverage         - Run tests with coverage report"
	@echo ""
	@echo "  🔍 Code Quality:"
	@echo "    make lint             - Run linting checks (flake8)"
	@echo "    make format           - Format code (black, isort)"
	@echo "    make type-check       - Run type checking (mypy)"
	@echo ""
	@echo "  🚀 Running:"
	@echo "    make dev              - Run pipeline in development mode"
	@echo "    make pipeline         - Run CDC pipeline"
	@echo "    make test-pipeline    - Test pipeline with sample data"
	@echo ""
	@echo "  🐳 Docker:"
	@echo "    make docker-up        - Start all Docker services (Kafka, Postgres, etc.)"
	@echo "    make docker-down      - Stop all Docker services"
	@echo "    make docker-restart   - Restart Docker services"
	@echo "    make docker-logs      - View Docker logs"
	@echo ""
	@echo "  🧹 Cleanup:"
	@echo "    make clean            - Clean cache files"
	@echo "    make clean-all        - Clean everything (cache + Docker volumes)"
	@echo ""
	@echo "  ✨ All-in-one:"
	@echo "    make all              - Run install, format, lint, test"
	@echo "    make ci               - Run CI checks (lint + test)"
	@echo ""

all: install format lint test
	@echo ""
	@echo "✅ All checks passed! Ready to run 'make pipeline'"

ci: lint test
	@echo ""
	@echo "✅ CI checks passed!"

# ==================== Dependencies ====================

venv:
	@echo "🔧 Creating virtual environment..."
	python3 -m venv .venv
	@echo "✅ Virtual environment created!"
	@echo "Activate with: source .venv/bin/activate"

install:
	@echo "📦 Installing dependencies..."
	pip install --upgrade pip
	pip install -r requirements.txt
	@echo "✅ Dependencies installed!"

install-dev:
	@echo "📦 Installing development dependencies..."
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install pytest pytest-cov pytest-mock flake8 black isort mypy
	@echo "✅ Development dependencies installed!"

# ==================== Testing ====================

test:
	@echo "🧪 Running all tests..."
	pytest tests/ -v --tb=short

test-unit:
	@echo "🧪 Running unit tests..."
	pytest tests/test_*.py -v

test-kafka:
	@echo "🧪 Testing Kafka consumer..."
	pytest tests/test_kafka_consumer.py -v

test-snowflake:
	@echo "🧪 Testing Snowflake connector..."
	pytest tests/test_snowflake_connector.py -v

test-pipeline:
	@echo "🧪 Testing pipeline..."
	pytest tests/test_pipeline.py -v

coverage:
	@echo "📊 Running tests with coverage..."
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term --cov-report=xml
	@echo ""
	@echo "✅ Coverage report generated!"
	@echo "📄 HTML report: htmlcov/index.html"
	@echo "📄 XML report: coverage.xml"

# ==================== Code Quality ====================

lint:
	@echo "🔍 Running linting checks..."
	@flake8 src/ tests/ --max-line-length=127 --extend-ignore=E203,W503 --exclude=.venv,__pycache__,.pytest_cache || true
	@echo "✅ Linting complete!"

format:
	@echo "✨ Formatting code..."
	@black src/ tests/ --exclude='/(\.venv|__pycache__|\.pytest_cache)/' --line-length=127 || true
	@isort src/ tests/ --skip .venv --skip __pycache__ --skip .pytest_cache --profile black || true
	@echo "✅ Code formatted!"

type-check:
	@echo "🔍 Running type checks..."
	@mypy src/ --ignore-missing-imports || true
	@echo "✅ Type checking complete!"

# ==================== Running ====================

dev:
	@echo "🚀 Starting pipeline in development mode..."
	@echo "Environment: development"
	cd src && python3 pipeline.py

pipeline:
	@echo "🚀 Starting CDC pipeline..."
	cd src && python3 pipeline.py

test-connection:
	@echo "🔌 Testing connections..."
	cd scripts && python3 test_snowflake_connection.py

generate-test-data:
	@echo "📝 Generating test data..."
	cd scripts && python3 test_pipeline.py

# ==================== Docker ====================

docker-up:
	@echo "🐳 Starting Docker services..."
	cd docker && docker compose up -d
	@echo ""
	@echo "✅ Services started!"
	@echo "📊 Kafka: http://localhost:9092"
	@echo "📊 Kowl (Kafka UI): http://localhost:8090"
	@echo "🗄️  PostgreSQL: localhost:5433"
	@echo "🔧 Adminer: http://localhost:7775"
	@echo "🔌 Kafka Connect: http://localhost:8083"

docker-down:
	@echo "🐳 Stopping Docker services..."
	cd docker && docker compose down
	@echo "✅ Services stopped!"

docker-restart:
	@echo "🐳 Restarting Docker services..."
	cd docker && docker compose restart
	@echo "✅ Services restarted!"

docker-logs:
	@echo "📋 Showing Docker logs..."
	cd docker && docker compose logs -f

docker-ps:
	@echo "🐳 Docker containers status:"
	cd docker && docker compose ps

docker-clean:
	@echo "🧹 Cleaning Docker resources..."
	cd docker && docker compose down -v
	@echo "✅ Docker volumes removed!"

# ==================== Debezium Connector ====================

create-connector:
	@echo "🔌 Creating Debezium connector..."
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
		http://localhost:8083/connectors/ -d @docker/transaction_connector.json
	@echo ""
	@echo "✅ Connector created!"

check-connector:
	@echo "🔍 Checking connector status..."
	curl -s http://localhost:8083/connectors/cdc-transactions/status | jq
	@echo ""

delete-connector:
	@echo "🗑️  Deleting connector..."
	curl -X DELETE http://localhost:8083/connectors/cdc-transactions
	@echo ""
	@echo "✅ Connector deleted!"

list-connectors:
	@echo "📋 Listing connectors..."
	curl -s http://localhost:8083/connectors | jq
	@echo ""

# ==================== Snowflake ====================

setup-snowflake:
	@echo "❄️  Setting up Snowflake database..."
	cd scripts && python3 setup_snowflake.py

# ==================== Cleanup ====================

clean:
	@echo "🧹 Cleaning cache files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
	rm -rf .coverage htmlcov coverage.xml
	rm -rf .mypy_cache
	@echo "✅ Cache cleaned!"

clean-logs:
	@echo "🧹 Cleaning log files..."
	rm -rf logs/*.log logs/*.json
	@echo "✅ Logs cleaned!"

clean-all: clean clean-logs docker-clean
	@echo "✅ Everything cleaned!"

# ==================== Help for common workflows ====================

quick-start:
	@echo "🚀 Quick Start Guide:"
	@echo ""
	@echo "1. Start Docker services:"
	@echo "   make docker-up"
	@echo ""
	@echo "2. Create Debezium connector:"
	@echo "   make create-connector"
	@echo ""
	@echo "3. Setup Snowflake:"
	@echo "   make setup-snowflake"
	@echo ""
	@echo "4. Run pipeline:"
	@echo "   make pipeline"
	@echo ""
	@echo "5. Generate test data (in another terminal):"
	@echo "   make generate-test-data"

health-check:
	@echo "🏥 Health Check:"
	@echo ""
	@echo "📊 Kafka:"
	@curl -s http://localhost:8083/ > /dev/null && echo "  ✅ Kafka Connect is UP" || echo "  ❌ Kafka Connect is DOWN"
	@echo ""
	@echo "🗄️  PostgreSQL:"
	@docker exec postgres pg_isready -U postgres > /dev/null 2>&1 && echo "  ✅ PostgreSQL is UP" || echo "  ❌ PostgreSQL is DOWN"
	@echo ""
	@echo "🔌 Debezium Connector:"
	@curl -s http://localhost:8083/connectors/cdc-transactions/status | grep -q '"state":"RUNNING"' && echo "  ✅ Connector is RUNNING" || echo "  ❌ Connector is NOT running"
