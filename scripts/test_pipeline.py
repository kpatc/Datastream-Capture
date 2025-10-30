#!/usr/bin/env python3
"""
Test script to verify the CDC pipeline setup and generate test data
"""
import sys
import os
import time
import psycopg2
from datetime import datetime
import random

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config


def generate_test_transactions(num_transactions=10):
    """Generate test transactions in PostgreSQL"""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()
        
        print(f"Generating {num_transactions} test transactions...")
        
        statuses = ['pending', 'completed', 'failed', 'cancelled']
        
        for i in range(num_transactions):
            user_id = random.randint(1, 100)
            amount = round(random.uniform(10.0, 1000.0), 2)
            status = random.choice(statuses)
            
            cursor.execute(
                """
                INSERT INTO public.transactions (user_id, amount, status)
                VALUES (%s, %s, %s)
                RETURNING transaction_id
                """,
                (user_id, amount, status)
            )
            
            transaction_id = cursor.fetchone()[0]
            print(f"Created transaction {transaction_id}: user={user_id}, amount={amount}, status={status}")
            
            conn.commit()
            time.sleep(0.5)  # Small delay between transactions
        
        print(f"\nSuccessfully generated {num_transactions} transactions!")
        
        # Show current transaction count
        cursor.execute("SELECT COUNT(*) FROM public.transactions")
        count = cursor.fetchone()[0]
        print(f"Total transactions in PostgreSQL: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error generating test transactions: {e}")
        sys.exit(1)


def update_random_transaction():
    """Update a random transaction to test CDC updates"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()
        
        # Get a random transaction
        cursor.execute("SELECT transaction_id FROM public.transactions ORDER BY RANDOM() LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            transaction_id = result[0]
            new_status = random.choice(['completed', 'failed'])
            
            cursor.execute(
                """
                UPDATE public.transactions 
                SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE transaction_id = %s
                """,
                (new_status, transaction_id)
            )
            
            conn.commit()
            print(f"Updated transaction {transaction_id} to status: {new_status}")
        else:
            print("No transactions found to update")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error updating transaction: {e}")


def delete_random_transaction():
    """Delete a random transaction to test CDC deletes"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()
        
        # Get a random transaction
        cursor.execute("SELECT transaction_id FROM public.transactions ORDER BY RANDOM() LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            transaction_id = result[0]
            
            cursor.execute(
                "DELETE FROM public.transactions WHERE transaction_id = %s",
                (transaction_id,)
            )
            
            conn.commit()
            print(f"Deleted transaction {transaction_id}")
        else:
            print("No transactions found to delete")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error deleting transaction: {e}")


def verify_kafka_connectivity():
    """Verify Kafka connectivity"""
    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
        
        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            consumer_timeout_ms=5000
        )
        
        topics = consumer.topics()
        print(f"✓ Kafka is accessible")
        print(f"Available topics: {topics}")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"✗ Kafka connectivity error: {e}")
        return False


def verify_postgres_connectivity():
    """Verify PostgreSQL connectivity"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"✓ PostgreSQL is accessible")
        print(f"Version: {version[:50]}...")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ PostgreSQL connectivity error: {e}")
        return False


def main():
    """Main test function"""
    print("=" * 60)
    print("CDC Pipeline Test Script")
    print("=" * 60)
    print()
    
    # Verify connectivity
    print("Verifying connectivity...")
    print("-" * 60)
    postgres_ok = verify_postgres_connectivity()
    kafka_ok = verify_kafka_connectivity()
    print()
    
    if not (postgres_ok and kafka_ok):
        print("Please ensure all services are running:")
        print("  cd docker && docker compose up -d")
        sys.exit(1)
    
    # Menu
    while True:
        print("-" * 60)
        print("Select an action:")
        print("1. Generate test transactions (INSERT)")
        print("2. Update a random transaction (UPDATE)")
        print("3. Delete a random transaction (DELETE)")
        print("4. Generate continuous load (10 transactions)")
        print("5. Exit")
        print("-" * 60)
        
        choice = input("Enter choice (1-5): ").strip()
        
        if choice == '1':
            num = input("Number of transactions to generate (default: 10): ").strip()
            num = int(num) if num.isdigit() else 10
            generate_test_transactions(num)
        elif choice == '2':
            update_random_transaction()
        elif choice == '3':
            delete_random_transaction()
        elif choice == '4':
            print("Generating continuous load...")
            for i in range(10):
                generate_test_transactions(1)
                if i % 3 == 0:
                    update_random_transaction()
                time.sleep(1)
            print("Continuous load complete!")
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice, please try again")
        
        print()


if __name__ == "__main__":
    main()
