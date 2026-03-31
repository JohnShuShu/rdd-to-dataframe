"""
Full ETL Pipeline Pattern: RDD → DataFrame → Delta Lake

The most common real-world pattern: legacy code ingests binary or text data
into an RDD, runs custom logic, then needs to join with catalog data and
write to Delta Lake.

Pipeline flow:
Raw Files / Kafka → RDD (Parse/Validate) → DataFrame (Transform/Join) → Delta Lake
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, current_timestamp, to_date, when
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# STEP 1: Schema Definition (define ONCE, reuse everywhere)
# =============================================================================

CLAIM_SCHEMA = StructType([
    StructField("claim_id",       StringType(),  False),
    StructField("patient_id",     LongType(),    False),
    StructField("provider_npi",   StringType(),  True),
    StructField("procedure_code", StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("service_date",   StringType(),  True),
    StructField("_parse_errors",  StringType(),  True),  # Audit field
])


# =============================================================================
# STEP 2: RDD Parsing Function (runs on workers)
# =============================================================================

def parse_claim_line(line: str) -> Row:
    """
    Parse pipe-delimited claim record.

    Returns Row with _parse_errors field for tracking issues.
    This function runs on workers - no SparkSession access here!
    """
    errors = []

    try:
        parts = line.split("|")
        if len(parts) < 6:
            raise ValueError(f"Expected 6 fields, got {len(parts)}")

        claim_id       = parts[0].strip()
        patient_id     = int(parts[1].strip())
        provider_npi   = parts[2].strip() or None
        procedure_code = parts[3].strip() or None
        amount         = float(parts[4].strip())
        service_date   = parts[5].strip() or None

        # Business validations
        if amount < 0:
            errors.append("NEGATIVE_AMOUNT")
        if provider_npi and not re.match(r"^\d{10}$", provider_npi):
            errors.append("INVALID_NPI")
        if procedure_code and not re.match(r"^[A-Z0-9]{5}$", procedure_code):
            errors.append("INVALID_PROCEDURE_CODE")

        return Row(
            claim_id=claim_id,
            patient_id=patient_id,
            provider_npi=provider_npi,
            procedure_code=procedure_code,
            amount=amount,
            service_date=service_date,
            _parse_errors=",".join(errors) if errors else None,
        )

    except Exception as e:
        return Row(
            claim_id="_UNPARSEABLE",
            patient_id=-1,
            provider_npi=None,
            procedure_code=None,
            amount=0.0,
            service_date=None,
            _parse_errors=f"PARSE_FAIL:{str(e)[:200]}",
        )


# =============================================================================
# STEP 3: Main Pipeline
# =============================================================================

def run_claims_pipeline(
    spark: SparkSession,
    input_path: str,
    output_table: str,
    provider_table: str = None
):
    """
    Execute the full claims ETL pipeline.

    Args:
        spark: SparkSession
        input_path: Path to raw claim files
        output_table: Target Delta table
        provider_table: Optional provider lookup table for enrichment
    """
    logger.info(f"Starting claims pipeline: {input_path} → {output_table}")

    # ─────────────────────────────────────────────────────────────────────────
    # STAGE 1: RDD - Parse raw text files
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("Stage 1: Parsing raw files to RDD")

    raw_rdd = spark.sparkContext.textFile(input_path)

    # Skip header if present
    header = raw_rdd.first()
    if header.startswith("claim_id"):
        raw_rdd = raw_rdd.filter(lambda line: line != header)

    # Parse to Row objects
    parsed_rdd = raw_rdd.map(parse_claim_line)

    # ─────────────────────────────────────────────────────────────────────────
    # STAGE 2: Convert to DataFrame (EXIT RDD WORLD)
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("Stage 2: Converting to DataFrame")

    claims_df = spark.createDataFrame(parsed_rdd, CLAIM_SCHEMA)

    # Cache for multiple operations
    claims_df.cache()

    # Log parsing stats
    total_count = claims_df.count()
    error_count = claims_df.filter(col("_parse_errors").isNotNull()).count()
    logger.info(f"Parsed {total_count} records, {error_count} with errors")

    # ─────────────────────────────────────────────────────────────────────────
    # STAGE 3: DataFrame Transformations
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("Stage 3: Applying transformations")

    # Add audit columns
    claims_df = claims_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", lit(input_path)) \
        .withColumn("service_date_parsed", to_date(col("service_date"), "yyyy-MM-dd"))

    # Categorize by amount
    claims_df = claims_df.withColumn(
        "amount_tier",
        when(col("amount") < 100, "low")
        .when(col("amount") < 1000, "medium")
        .when(col("amount") < 10000, "high")
        .otherwise("very_high")
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STAGE 4: Optional Enrichment (join with lookup tables)
    # ─────────────────────────────────────────────────────────────────────────
    if provider_table:
        logger.info(f"Stage 4: Enriching with provider data from {provider_table}")

        providers_df = spark.table(provider_table).select(
            col("npi").alias("provider_npi"),
            col("name").alias("provider_name"),
            col("specialty").alias("provider_specialty"),
        )

        claims_df = claims_df.join(
            providers_df,
            on="provider_npi",
            how="left"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # STAGE 5: Write to Delta Lake
    # ─────────────────────────────────────────────────────────────────────────
    logger.info(f"Stage 5: Writing to {output_table}")

    # Separate valid and error records
    valid_df = claims_df.filter(col("_parse_errors").isNull())
    error_df = claims_df.filter(col("_parse_errors").isNotNull())

    # Write valid records
    valid_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("service_date_parsed") \
        .saveAsTable(output_table)

    # Write errors to separate table for review
    if error_df.count() > 0:
        error_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{output_table}_errors")

    # Cleanup
    claims_df.unpersist()

    logger.info("Pipeline complete!")

    return {
        "total_records": total_count,
        "valid_records": total_count - error_count,
        "error_records": error_count,
        "output_table": output_table,
    }


# =============================================================================
# DEMO with sample data
# =============================================================================

def demo_pipeline():
    """Run pipeline with sample data."""
    spark = SparkSession.builder \
        .appName("ClaimsETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()

    # Create sample data
    sample_data = [
        "CLM001|1001|1234567890|99213|150.00|2024-01-15",
        "CLM002|1002|9876543210|99214|275.50|2024-01-16",
        "CLM003|1003|invalid_npi|99215|500.00|2024-01-17",  # Invalid NPI
        "CLM004|1004|1111111111|XXXXX|50.00|2024-01-18",    # Invalid procedure
        "CLM005|bad_patient|2222222222|99213|100.00|2024-01-19",  # Parse error
    ]

    # Write sample data to temp file
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "claims.txt")
        with open(input_path, "w") as f:
            f.write("\n".join(sample_data))

        # Run pipeline (without Delta for demo)
        raw_rdd = spark.sparkContext.textFile(input_path)
        parsed_rdd = raw_rdd.map(parse_claim_line)
        claims_df = spark.createDataFrame(parsed_rdd, CLAIM_SCHEMA)

        print("=== Parsed Claims ===")
        claims_df.show(truncate=False)

        print("=== Records with Errors ===")
        claims_df.filter(col("_parse_errors").isNotNull()).show(truncate=False)


if __name__ == "__main__":
    demo_pipeline()
