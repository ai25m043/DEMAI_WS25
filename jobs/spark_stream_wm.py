#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stream Wikimedia RecentChange events from Kafka -> PostgreSQL.

Fixes included:
- Proper keyword/topic matching (lowercased text vs lowercased keywords).
- Avoids "everything becomes 'other'" by joining on substring condition and
  collecting all matched topic keys; falls back to ['other'] only if none match.
- Resolves ambiguous `meta_id` after groupBy by excluding it from the aggregated struct.
- Stores the original raw JSON from Kafka into `raw`.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------
# Environment / defaults
# -----------------------------
APP_NAME = "wm-stream-to-postgres"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "wm-recentchange")
KAFKA_GROUP_ID  = os.getenv("KAFKA_GROUP_ID", "wm-stream-postgres")

PG_HOST = os.getenv("PG_HOST", "wm-postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "demai")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

JDBC_URL     = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?stringtype=unspecified"
RC_TABLE     = "wm_recent_change"
TOPIC_TABLE  = "wm_topic"

CHECKPOINT   = os.getenv("CHECKPOINT", "/tmp/chk-wm")

# -----------------------------
# Wikimedia RC schema (subset)
# -----------------------------
meta_schema = T.StructType([
    T.StructField("uri",        T.StringType(), True),
    T.StructField("request_id", T.StringType(), True),
    T.StructField("id",         T.StringType(), True),   # meta.id (string-ish)
    T.StructField("domain",     T.StringType(), True),
    T.StructField("stream",     T.StringType(), True),
    T.StructField("dt",         T.StringType(), True),   # ISO8601 string
    T.StructField("topic",      T.StringType(), True),
    T.StructField("partition",  T.IntegerType(), True),
    T.StructField("offset",     T.LongType(), True),
])

length_schema = T.StructType([
    T.StructField("old", T.IntegerType(), True),
    T.StructField("new", T.IntegerType(), True),
])

revision_schema = T.StructType([
    T.StructField("old", T.LongType(), True),
    T.StructField("new", T.LongType(), True),
])

wm_schema = T.StructType([
    T.StructField("$schema",            T.StringType(), True),
    T.StructField("meta",               meta_schema,    True),
    T.StructField("id",                 T.LongType(),   True),   # rc id (not stored)
    T.StructField("type",               T.StringType(), True),
    T.StructField("namespace",          T.IntegerType(),True),
    T.StructField("title",              T.StringType(), True),
    T.StructField("title_url",          T.StringType(), True),
    T.StructField("comment",            T.StringType(), True),
    T.StructField("timestamp",          T.LongType(),   True),
    T.StructField("user",               T.StringType(), True),
    T.StructField("bot",                T.BooleanType(),True),
    T.StructField("notify_url",         T.StringType(), True),
    T.StructField("minor",              T.BooleanType(),True),
    T.StructField("patrolled",          T.BooleanType(),True),
    T.StructField("length",             length_schema,  True),
    T.StructField("revision",           revision_schema,True),
    T.StructField("server_url",         T.StringType(), True),
    T.StructField("server_name",        T.StringType(), True),   # e.g. en.wikipedia.org
    T.StructField("server_script_path", T.StringType(), True),
    T.StructField("wiki",               T.StringType(), True),   # short key like enwiki
    T.StructField("parsedcomment",      T.StringType(), True),
])

# -----------------------------
# Spark session
# -----------------------------
spark = (
    SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Load topics once (dimension)
# -----------------------------
topics_dim = (
    spark.read
         .format("jdbc")
         .option("url", JDBC_URL)
         .option("dbtable", TOPIC_TABLE)
         .option("user", PG_USER)
         .option("password", PG_PASS)
         .option("driver", "org.postgresql.Driver")
         .load()
         .where(F.col("enabled") == F.lit(True))
         .select(
             F.col("key").alias("topic_key"),
             F.col("keywords")
         )
)

# Explode keywords and lower-case for matching; ignore NULL/empty
topics_kw = (
    topics_dim
      .withColumn("kw", F.explode_outer(F.col("keywords")))
      .where(F.col("kw").isNotNull() & (F.length(F.col("kw")) > 0))
      .withColumn("kw_lc", F.lower(F.col("kw")))
      .select("topic_key", "kw_lc")
      .dropDuplicates()
)

# Broadcast (tiny dimension)
topics_kw = F.broadcast(topics_kw)

# -----------------------------
# Transform helpers
# -----------------------------
def parse_wm_json(df_json):
    """
    Parse the incoming Kafka JSON into typed columns and derive
    target table fields. Keep the original JSON in 'raw'.
    """
    # Parse JSON to struct 'r'
    parsed = df_json.select(
        F.from_json(
            F.col("json"),
            wm_schema,
            {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
        ).alias("r"),
        F.col("json").alias("raw")  # keep original JSON string
    )

    r = parsed.select("r.*", "raw")

    base = (
        r
        .withColumn("meta_id", F.col("meta.id"))
        .withColumn("dt", F.to_timestamp(F.col("meta.dt")))  # ISO8601 -> Timestamp
        .withColumn("wiki", F.col("server_name"))
        .withColumn("page_id", F.lit(None).cast("bigint"))
        .withColumn("title", F.col("title"))
        .withColumn("namespace", F.col("namespace").cast("int"))
        .withColumn("type", F.col("type"))
        .withColumn("user_text", F.col("user"))
        .withColumn("comment", F.col("comment"))
        .withColumn("old_len", F.col("length.old").cast("int"))
        .withColumn("new_len", F.col("length.new").cast("int"))
        .withColumn("added", F.greatest(F.col("new_len") - F.col("old_len"), F.lit(0)))
        .withColumn("removed", F.greatest(F.col("old_len") - F.col("new_len"), F.lit(0)))
        .dropDuplicates(["meta_id"])
    )

    # Build a lowercased blob for keyword matching
    text_lc = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("title"), F.lit("")),
            F.coalesce(F.col("comment"), F.lit("")),
            F.coalesce(F.col("parsedcomment"), F.lit(""))
        )
    )
    base = base.withColumn("text_lc", text_lc)

    return base


def attach_topics(parsed_df):
    """
    Attach topic array by substring keyword matching on text_lc.
    Falls back to ['other'] if no matches.
    """
    # Substring match condition; instr > 0 == contains
    cond = F.instr(parsed_df.text_lc, F.col("kw_lc")) > 0

    # Avoid duplicate/ambiguous meta_id by excluding it from the struct
    cols_for_struct = [c for c in parsed_df.columns if c not in ("meta_id", "text_lc")]

    matched = (
        parsed_df
          .join(topics_kw, cond, how="left")
          .groupBy("meta_id")
          .agg(
              F.first(F.struct(*[F.col(c) for c in cols_for_struct])).alias("row"),
              F.collect_set("topic_key").alias("topic_keys")
          )
    )

    out = (
        matched
          .select(
              "meta_id",
              F.col("row.*"),
              F.when(
                  (F.col("topic_keys").isNull()) | (F.size(F.col("topic_keys")) == 0),
                  F.array(F.lit("other"))
              ).otherwise(F.col("topic_keys")).alias("topics")
          )
          .select(
              "meta_id", "dt", "wiki", "page_id", "title", "namespace", "type",
              "user_text", "comment", "old_len", "new_len", "added", "removed",
              "topics", "raw"
          )
    )
    return out

# -----------------------------
# foreachBatch
# -----------------------------
def write_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        print(f"[wm] micro-batch {batch_id}: rows=0 (skip)")
        return

    parsed = parse_wm_json(batch_df)
    out = attach_topics(parsed)

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

# -----------------------------
# Main stream
# -----------------------------
def main():
    df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", KAFKA_TOPIC)
             .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
             .option("kafka.group.id", KAFKA_GROUP_ID)
             .option("failOnDataLoss", "false")
             # Optional: tune session timeout if you frequently restart
             # .option("kafka.session.timeout.ms", "10000")
             .load()
             .select(F.col("value").cast("string").alias("json"))
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
