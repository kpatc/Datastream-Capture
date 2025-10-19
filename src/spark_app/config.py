import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.inventory.customers")

SNOWFLAKE_OPTIONS = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),
    "sfDatabase": os.getenv("SNOWFLAKE_DB"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WH"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASS")
}

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
