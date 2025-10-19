from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from config import KAFKA_BROKER, KAFKA_TOPIC, SNOWFLAKE_OPTIONS, SLACK_WEBHOOK_URL
import requests

def send_slack_message(message: str):
    """Send alerts to Slack"""
    if SLACK_WEBHOOK_URL:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})

def main():
    # 1️⃣ Initialize Spark
    spark = SparkSession.builder \
        .appName("CDC-RealTime-Streaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    send_slack_message("🚀 Spark job started successfully!")

    # 2️⃣ Define Kafka source
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 3️⃣ Parse CDC data (Debezium produces JSON payloads)
    schema = StructType() \
        .add("op", StringType()) \
        .add("after", StructType()
             .add("id", StringType())
             .add("first_name", StringType())
             .add("last_name", StringType())
             .add("email", StringType()))

    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 4️⃣ Write to Snowflake
    query = df_parsed.writeStream \
        .format("snowflake") \
        .options(**SNOWFLAKE_OPTIONS) \
        .option("dbtable", "CUSTOMERS_STREAM") \
        .option("checkpointLocation", "/tmp/snowflake_checkpoints") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
