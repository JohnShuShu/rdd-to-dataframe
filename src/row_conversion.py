"""
Row-Based Conversion Patterns

The Row class is the native Spark SQL type. Converting an RDD of Row objects
gives you the most control over null handling, nested structures, and field naming.
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

spark = SparkSession.builder.appName("RowConversion").getOrCreate()


# =============================================================================
# BASIC ROW CONVERSION
# =============================================================================

def parse_csv_to_row(line: str) -> Row:
    """
    Parse a CSV line into a Row object with proper null handling.

    Args:
        line: Comma-separated string

    Returns:
        Row object with named fields
    """
    parts = line.split(",")
    return Row(
        name = parts[0].strip(),
        age  = int(parts[1]) if parts[1].strip() else None,
        dept = parts[2].strip(),
    )


def demo_basic_row_conversion():
    """Demonstrate basic CSV to Row conversion."""
    raw_rdd = spark.sparkContext.parallelize([
        "alice,30,engineering",
        "bob,25,marketing",
        "carol,,engineering",  # missing age - handle nulls explicitly
    ])

    row_rdd = raw_rdd.map(parse_csv_to_row)

    schema = StructType([
        StructField("name", StringType(), False),
        StructField("age",  IntegerType(), True),
        StructField("dept", StringType(), True),
    ])

    df = spark.createDataFrame(row_rdd, schema)
    df.printSchema()
    df.show()
    return df


# =============================================================================
# NESTED ROW STRUCTURES
# =============================================================================

def demo_nested_rows():
    """Demonstrate nested Row structures for complex data."""
    # Define Row types for nested structures
    Address = Row("street", "city", "zip")
    Person  = Row("name", "age", "address")

    nested_rdd = spark.sparkContext.parallelize([
        Person("alice", 30, Address("123 Main", "Dallas", "75201")),
        Person("bob",   25, Address("456 Oak",  "Austin", "78701")),
    ])

    nested_schema = StructType([
        StructField("name", StringType(), False),
        StructField("age",  IntegerType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city",   StringType(), True),
            StructField("zip",    StringType(), True),
        ]), True),
    ])

    nested_df = spark.createDataFrame(nested_rdd, nested_schema)
    nested_df.printSchema()
    nested_df.show(truncate=False)

    # Access nested fields
    nested_df.select("name", "address.city").show()

    return nested_df


# =============================================================================
# ROW WITH ERROR HANDLING
# =============================================================================

def safe_parse_to_row(line: str) -> Row:
    """
    Parse line to Row with comprehensive error handling.

    Returns Row with _parse_error field for tracking issues.
    """
    try:
        parts = line.split(",")
        if len(parts) < 3:
            raise ValueError(f"Expected 3 fields, got {len(parts)}")

        return Row(
            name        = parts[0].strip() or None,
            age         = int(parts[1]) if parts[1].strip() else None,
            salary      = float(parts[2]) if parts[2].strip() else None,
            _parse_error = None,
        )
    except Exception as e:
        return Row(
            name        = None,
            age         = None,
            salary      = None,
            _parse_error = str(e)[:200],
        )


def demo_error_handling():
    """Demonstrate Row conversion with error tracking."""
    raw_rdd = spark.sparkContext.parallelize([
        "alice,30,95000",
        "bob,invalid,82000",  # invalid age
        "carol",              # missing fields
        "dave,25,not_a_number",  # invalid salary
    ])

    row_rdd = raw_rdd.map(safe_parse_to_row)

    schema = StructType([
        StructField("name",         StringType(), True),
        StructField("age",          IntegerType(), True),
        StructField("salary",       DoubleType(), True),
        StructField("_parse_error", StringType(), True),
    ])

    df = spark.createDataFrame(row_rdd, schema)

    print("All records:")
    df.show(truncate=False)

    print("Records with errors:")
    df.filter(df._parse_error.isNotNull()).show(truncate=False)

    print("Valid records:")
    df.filter(df._parse_error.isNull()).show(truncate=False)

    return df


# =============================================================================
# ROW FROM DICTIONARY
# =============================================================================

def dict_to_row(d: dict, expected_fields: list) -> Row:
    """
    Convert dictionary to Row with field validation.

    Args:
        d: Input dictionary
        expected_fields: List of field names to extract

    Returns:
        Row with specified fields (missing fields become None)
    """
    return Row(**{k: d.get(k) for k in expected_fields})


def demo_dict_to_row():
    """Demonstrate dictionary to Row conversion."""
    dict_rdd = spark.sparkContext.parallelize([
        {"id": 1, "name": "alice", "score": 95.0, "extra_field": "ignored"},
        {"id": 2, "name": "bob"},  # missing score
        {"id": 3, "score": 88.0},  # missing name
    ])

    EXPECTED_FIELDS = ["id", "name", "score"]

    row_rdd = dict_rdd.map(lambda d: dict_to_row(d, EXPECTED_FIELDS))

    schema = StructType([
        StructField("id",    IntegerType(), True),
        StructField("name",  StringType(), True),
        StructField("score", DoubleType(), True),
    ])

    df = spark.createDataFrame(row_rdd, schema)
    df.show()
    return df


if __name__ == "__main__":
    print("=== Basic Row Conversion ===")
    demo_basic_row_conversion()

    print("\n=== Nested Rows ===")
    demo_nested_rows()

    print("\n=== Error Handling ===")
    demo_error_handling()

    print("\n=== Dict to Row ===")
    demo_dict_to_row()
