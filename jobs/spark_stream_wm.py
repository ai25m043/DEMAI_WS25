#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka (wm-recentchange) -> Postgres (wm_recent_change)
- Persist only rows that already have page_country_code (from bridge) AND at least one topic
  matched from wm_topic where enabled=true AND key != 'other'.
- Never write 'other'.
- Topic matching by exact token (lowercased) to avoid substring false-positives.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

APP_NAME = "wm-stream-to-postgres-with-topics"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "wm-recentchange")
KAFKA_GROUP_ID  = os.getenv("KAFKA_GROUP_ID", "wm-stream-postgres")

PG_HOST = os.getenv("PG_HOST", "wm-postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "demai")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?stringtype=unspecified"

RC_TABLE     = "wm_recent_change"
TOPIC_TABLE  = "wm_topic"
CHECKPOINT   = os.getenv("CHECKPOINT", "/tmp/chk-wm-topics")

length_schema = T.StructType([
    T.StructField("old", T.IntegerType(), True),
    T.StructField("new", T.IntegerType(), True),
])

wm_schema = T.StructType([
    T.StructField("meta", T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("dt", T.StringType(), True),
    ]), True),
    T.StructField("timestamp",          T.LongType(),   True),
    T.StructField("server_url",         T.StringType(), True),
    T.StructField("server_name",        T.StringType(), True),
    T.StructField("server_script_path", T.StringType(), True),
    T.StructField("wiki",               T.StringType(), True),
    T.StructField("namespace",          T.IntegerType(),True),
    T.StructField("title",              T.StringType(), True),
    T.StructField("type",               T.StringType(), True),
    T.StructField("comment",            T.StringType(), True),
    T.StructField("user",               T.StringType(), True),
    T.StructField("bot",                T.BooleanType(),True),
    T.StructField("minor",              T.BooleanType(),True),
    T.StructField("patrolled",          T.BooleanType(),True),
    T.StructField("length",             length_schema,  True),

    # Enriched by bridge
    T.StructField("page_qid",           T.StringType(), True),
    T.StructField("page_country_qid",   T.StringType(), True),
    T.StructField("page_country_code",  T.StringType(), True),
    T.StructField("geo_method",         T.StringType(), True),
    T.StructField("geo_confidence",     T.IntegerType(),True),
])

spark = (
    SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---- Load enabled topics (excluding 'other') and explode keywords ----
topics_kw = (
    spark.read.format("jdbc")
         .option("url", JDBC_URL)
         .option("dbtable", TOPIC_TABLE)
         .option("user", PG_USER)
         .option("password", PG_PASS)
         .option("driver", "org.postgresql.Driver")
         .load()
         .where((F.col("enabled") == F.lit(True)) & (F.col("key") != F.lit("other")))
         .select(F.col("key").alias("topic_key"), F.col("keywords"))
         .withColumn("kw", F.explode_outer("keywords"))
         .where(F.col("kw").isNotNull() & (F.length(F.col("kw")) > 0))
         .withColumn("kw_lc", F.lower(F.col("kw")))
         .select("topic_key", "kw_lc")
         .dropDuplicates()
)
topics_kw = F.broadcast(topics_kw)

def write_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        print(f"[wm] micro-batch {batch_id}: rows=0 (skip)")
        return

    # Parse JSON and keep original JSON as 'raw' (STRING)
    parsed = (
        batch_df
        .select(F.col("value").cast("string").alias("json"))
        .select(F.from_json(F.col("json"), wm_schema).alias("r"), F.col("json").alias("raw"))
        .select("r.*", "raw")
    )

    # Events must already have a country from bridge
    parsed = parsed.filter(F.col("page_country_code").isNotNull())

    # Materialize meta_id and dt early
    parsed = parsed.withColumn("meta_id", F.col("meta.id"))
    parsed = parsed.withColumn("dt_ts", F.to_timestamp(F.col("meta.dt")))

    # Build lowercase text and tokenize to words: exact token match eliminates substring false-positives ('award' ≠ 'war')
    text_lc = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("title"), F.lit("")),
            F.coalesce(F.col("comment"), F.lit(""))
        )
    )
    tokens = F.expr("array_distinct(filter(split(%s, '[^a-z0-9]+'), x -> length(x) > 0))" % ("text_lc"))
    parsed = parsed.withColumn("text_lc", text_lc).withColumn("tokens", tokens)

    # Explode tokens and join to topic keywords
    tokened = parsed.withColumn("tok", F.explode_outer(F.col("tokens")))
    joined = tokened.join(topics_kw, tokened["tok"] == topics_kw["kw_lc"], "left")

    # Aggregate back per meta_id, collect matched topics
    cols_keep = [
        "meta_id", "dt_ts", "timestamp", "wiki", "server_name", "server_url", "server_script_path",
        "title", "namespace", "type", "comment", "user", "bot", "minor", "patrolled",
        "length", "page_qid", "page_country_qid", "page_country_code", "geo_method", "geo_confidence", "raw"
    ]
    matched = (
        joined.groupBy("meta_id")
              .agg(
                  F.first(F.struct(*[F.col(c) for c in cols_keep])).alias("row"),
                  F.collect_set("topic_key").alias("topic_keys")
              )
              .select(
                  F.col("row.*"),
                  F.expr("filter(topic_keys, x -> x is not null)").alias("topics")
              )
    )

    # Keep only rows with ≥1 topic
    out = (
        matched
        .filter(F.size(F.col("topics")) > 0)
        .select(
            F.col("meta_id"),
            F.col("dt_ts").alias("dt"),
            F.col("timestamp").alias("timestamp_unix"),
            F.col("wiki"),
            F.col("server_name"),
            F.col("server_url"),
            F.col("server_script_path"),

            F.lit(None).cast("bigint").alias("page_id"),
            F.col("title").alias("title"),
            F.col("namespace").cast("int").alias("namespace"),
            F.col("type").alias("type"),
            F.col("comment").alias("comment"),
            F.col("user").alias("user_text"),
            F.col("bot").alias("bot"),
            F.col("minor").alias("minor"),
            F.col("patrolled").alias("patrolled"),
            F.col("length.old").cast("int").alias("old_len"),
            F.col("length.new").cast("int").alias("new_len"),
            F.greatest(F.col("length.new") - F.col("length.old"), F.lit(0)).alias("added"),
            F.greatest(F.col("length.old") - F.col("length.new"), F.lit(0)).alias("removed"),

            F.col("page_qid").alias("page_qid"),
            F.col("page_country_qid").alias("page_country_qid"),
            F.col("page_country_code").alias("page_country_code"),
            F.col("geo_method").alias("geo_method"),
            F.col("geo_confidence").cast("smallint").alias("geo_confidence"),

            F.lit(None).cast("double").alias("page_lat"),
            F.lit(None).cast("double").alias("page_lon"),
            F.lit(None).cast("string").alias("page_geohash"),

            F.col("raw").cast("string").alias("raw"),
            F.col("topics").alias("topics")
        )
        .dropDuplicates(["meta_id"])
    )

    cnt = out.count()
    print(f"[wm] micro-batch {batch_id}: rows={cnt}")
    out.show(5, truncate=False)

    (
        out.write
           .format("jdbc")
           .option("url", JDBC_URL)
           .option("dbtable", RC_TABLE)
           .option("user", PG_USER)
           .option("password", PG_PASS)
           .option("driver", "org.postgresql.Driver")
           .mode("append")
           .save()
    )

def main():
    df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", KAFKA_TOPIC)
             .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
             .option("kafka.group.id", KAFKA_GROUP_ID)
             .option("failOnDataLoss", "false")
             .load()
    )

    query = (
        df.writeStream
          .outputMode("append")
          .foreachBatch(write_batch)
          .option("checkpointLocation", CHECKPOINT)
          .start()
    )
    print("=== Streaming Query Started ===")
    query.awaitTermination()

if __name__ == "__main__":
    main()
