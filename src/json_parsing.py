"""
JSON and Semi-Structured RDD Conversion

Common patterns for handling JSON string RDDs from Kafka, files, or APIs.
"""

import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, col, get_json_object, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType
)

spark = SparkSession.builder.appName("JSONParsing").getOrCreate()


# =============================================================================
# METHOD A: spark.read.json() - Quick schema inference
# =============================================================================

def demo_read_json():
    """
    Use spark.read.json() for quick exploration.
    Note: This infers schema, avoid in production on large data.
    """
    json_rdd = spark.sparkContext.parallelize([
        '{"id":1,"event":"click","user":{"name":"alice","tier":"gold"},"tags":["promo","new"]}',
        '{"id":2,"event":"view","user":{"name":"bob","tier":"silver"},"tags":["organic"]}',
    ])

    # Infers nested schema automatically
    df = spark.read.json(json_rdd)
    df.printSchema()
    df.show(truncate=False)
    return df


# =============================================================================
# METHOD B: from_json() with explicit schema (RECOMMENDED)
# =============================================================================

def get_event_schema() -> StructType:
    """Define explicit schema for event JSON."""
    return StructType([
        StructField("id",    LongType(),   False),
        StructField("event", StringType(), True),
        StructField("user",  StructType([
            StructField("name", StringType(), True),
            StructField("tier", StringType(), True),
        ]), True),
        StructField("tags", ArrayType(StringType()), True),
    ])


def demo_from_json():
    """
    Use from_json() with explicit schema - recommended for production.
    Works with streaming DataFrames too.
    """
    json_rdd = spark.sparkContext.parallelize([
        '{"id":1,"event":"click","user":{"name":"alice","tier":"gold"},"tags":["promo","new"]}',
        '{"id":2,"event":"view","user":{"name":"bob","tier":"silver"},"tags":["organic"]}',
    ])

    # Convert to single-column DataFrame first
    raw_df = json_rdd.toDF(["raw_json"])

    # Parse with explicit schema
    event_schema = get_event_schema()

    parsed_df = (
        raw_df
        .withColumn("parsed", from_json(col("raw_json"), event_schema))
        .select("parsed.*")  # Flatten the struct
    )

    parsed_df.printSchema()
    parsed_df.show(truncate=False)

    # Access nested fields
    parsed_df.select("id", "event", "user.name", "user.tier").show()

    # Explode array field
    parsed_df.select("id", explode("tags").alias("tag")).show()

    return parsed_df


# =============================================================================
# METHOD C: mapPartitions + json.loads (Complex parse logic)
# =============================================================================

def parse_json_partition(records):
    """
    Parse JSON records with custom error handling.

    Uses mapPartitions for efficiency (one iterator per partition).
    """
    for raw in records:
        try:
            obj = json.loads(raw)
            yield Row(
                id        = obj.get("id"),
                event     = obj.get("event"),
                user_name = obj.get("user", {}).get("name"),
                user_tier = obj.get("user", {}).get("tier"),
                tag_count = len(obj.get("tags", [])),
                _error    = None,
            )
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            yield Row(
                id        = None,
                event     = "_PARSE_ERROR",
                user_name = None,
                user_tier = None,
                tag_count = 0,
                _error    = str(e)[:100],
            )


def demo_map_partitions():
    """
    Use mapPartitions for custom JSON parsing logic.
    Good for complex transformations or error handling.
    """
    json_rdd = spark.sparkContext.parallelize([
        '{"id":1,"event":"click","user":{"name":"alice"}}',
        '{"id":2,"event":"view"}',  # missing user
        'not valid json',           # parse error
        '{"id":3,"event":"purchase","user":{"name":"carol","tier":"gold"},"tags":["sale"]}',
    ])

    flat_schema = StructType([
        StructField("id",        LongType(),    True),
        StructField("event",     StringType(),  True),
        StructField("user_name", StringType(),  True),
        StructField("user_tier", StringType(),  True),
        StructField("tag_count", LongType(),    True),
        StructField("_error",    StringType(),  True),
    ])

    df = spark.createDataFrame(
        json_rdd.mapPartitions(parse_json_partition),
        flat_schema
    )

    print("All records:")
    df.show(truncate=False)

    print("Parse errors:")
    df.filter(col("_error").isNotNull()).show(truncate=False)

    return df


# =============================================================================
# METHOD D: get_json_object for selective extraction
# =============================================================================

def demo_get_json_object():
    """
    Use get_json_object to extract specific fields without full parsing.
    Useful when you only need a few fields from complex JSON.
    """
    json_rdd = spark.sparkContext.parallelize([
        '{"id":1,"metadata":{"timestamp":"2024-01-15","source":"web"},"payload":{"action":"click"}}',
        '{"id":2,"metadata":{"timestamp":"2024-01-16","source":"mobile"},"payload":{"action":"scroll"}}',
    ])

    raw_df = json_rdd.toDF(["json_string"])

    # Extract specific fields using JSON path
    extracted_df = raw_df.select(
        get_json_object(col("json_string"), "$.id").alias("id"),
        get_json_object(col("json_string"), "$.metadata.timestamp").alias("timestamp"),
        get_json_object(col("json_string"), "$.metadata.source").alias("source"),
        get_json_object(col("json_string"), "$.payload.action").alias("action"),
    )

    extracted_df.show()
    return extracted_df


# =============================================================================
# KAFKA PATTERN: Binary to JSON DataFrame
# =============================================================================

def demo_kafka_pattern():
    """
    Pattern for Kafka messages: binary value → string → parsed JSON.

    In real Kafka streaming, you'd use:
    spark.readStream.format("kafka")...
    """
    # Simulating Kafka-like messages (key, value as bytes)
    kafka_rdd = spark.sparkContext.parallelize([
        (b"key1", b'{"user_id":1,"action":"login"}'),
        (b"key2", b'{"user_id":2,"action":"purchase","amount":99.99}'),
    ])

    # Convert to DataFrame with raw columns
    raw_df = kafka_rdd.toDF(["key", "value"])

    # Cast binary to string and parse
    message_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("action",  StringType(), True),
        StructField("amount",  StringType(), True),  # Optional field
    ])

    parsed_df = (
        raw_df
        .withColumn("key_str", col("key").cast("string"))
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("parsed", from_json(col("value_str"), message_schema))
        .select(
            col("key_str").alias("message_key"),
            col("parsed.*")
        )
    )

    parsed_df.show()
    return parsed_df


if __name__ == "__main__":
    print("=== Method A: spark.read.json() ===")
    demo_read_json()

    print("\n=== Method B: from_json() with schema ===")
    demo_from_json()

    print("\n=== Method C: mapPartitions ===")
    demo_map_partitions()

    print("\n=== Method D: get_json_object ===")
    demo_get_json_object()

    print("\n=== Kafka Pattern ===")
    demo_kafka_pattern()
