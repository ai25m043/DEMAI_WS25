# stream-processor/spark_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, countDistinct, count, window
from pyspark.sql.types import StructType, StringType, BooleanType, LongType, TimestampType
import os

# Kafka + Mongo settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wikipedia.changes")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "wikipedia")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "page_scores")

# Define the schema of the incoming Kafka JSON messages
schema = StructType()
schema = schema.add("title", StringType())
schema = schema.add("user", StringType())
schema = schema.add("bot", BooleanType())
schema = schema.add("timestamp", LongType())
schema = schema.add("type", StringType())
schema = schema.add("comment", StringType())

# Create Spark session
spark = SparkSession.builder \
    .appName("WikipediaStreamProcessor") \
    .master("spark://spark-master:7077") \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.database", MONGO_DB) \
    .config("spark.mongodb.write.collection", MONGO_COLLECTION) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON and cast
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Add a processing timestamp column
from pyspark.sql.functions import current_timestamp
json_df = json_df.withColumn("processed_at", current_timestamp())

# Group by page title in tumbling windows (e.g. 1-minute)
aggregated = json_df \
    .groupBy(
        window(col("processed_at"), "60 seconds"),
        col("title")
    ) \
    .agg(
        count("*").alias("num_edits"),
        countDistinct("user").alias("num_editors"),
        count(col("comment").contains("revert")).alias("num_reverts")
    ) \
    .withColumn("controversy_score",
        (col("num_reverts") * 2 + col("num_editors") + 0.5 * col("num_edits")) / 10
    )

# Rename fields to flatten nested structure
result_df = aggregated.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "title", "num_edits", "num_editors", "num_reverts", "controversy_score"
)

# Write to MongoDB using Mongo Spark Connector
query = result_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .outputMode("complete") \
    .start()

query.awaitTermination()