"""
Schema Strategies for RDD to DataFrame Conversion

This module demonstrates different approaches to defining schemas
when converting RDDs to DataFrames in PySpark.

Key insight: Schema definition is the MOST IMPORTANT decision.
Inferred schemas trigger a full RDD scan - never use inference in production.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType, BooleanType, TimestampType,
    ArrayType, MapType, DateType
)

spark = SparkSession.builder.appName("SchemaStrategies").getOrCreate()


# =============================================================================
# APPROACH 1: Schema Inference (DEV/NOTEBOOK ONLY)
# =============================================================================
# Spark samples the entire RDD to figure out types. NEVER do this in prod!

def demo_schema_inference():
    """Demonstrate schema inference - avoid in production!"""
    rdd = spark.sparkContext.parallelize([
        ("alice", 30, 95000.0),
        ("bob",   25, 82000.0),
    ])

    # This triggers a full RDD scan to infer types
    df_inferred = rdd.toDF(["name", "age", "salary"])
    df_inferred.printSchema()
    return df_inferred


# =============================================================================
# APPROACH 2: Explicit StructType (PRODUCTION STANDARD)
# =============================================================================
# No scan. Schema is enforced at parse time. Mismatches raise AnalysisException.

def get_employee_schema() -> StructType:
    """Define explicit schema for employee data."""
    return StructType([
        StructField("name",       StringType(),    nullable=False),
        StructField("age",        IntegerType(),   nullable=True),
        StructField("salary",     DoubleType(),    nullable=True),
        StructField("active",     BooleanType(),   nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
    ])


def demo_explicit_schema():
    """Demonstrate explicit schema - recommended for production."""
    rdd = spark.sparkContext.parallelize([
        ("alice", 30, 95000.0, True, None),
        ("bob",   25, 82000.0, True, None),
    ])

    schema = get_employee_schema()
    df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    return df


# =============================================================================
# APPROACH 3: DDL String Schema
# =============================================================================
# Concise syntax, great for SQL practitioners.

def demo_ddl_schema():
    """Demonstrate DDL string schema definition."""
    rdd = spark.sparkContext.parallelize([
        ("alice", 30, 95000.0, "engineering"),
        ("bob",   25, 82000.0, "marketing"),
    ])

    ddl_schema = "name STRING NOT NULL, age INT, salary DOUBLE, dept STRING"
    df_ddl = spark.createDataFrame(rdd, ddl_schema)
    df_ddl.printSchema()
    return df_ddl


# =============================================================================
# APPROACH 4: Complex / Nested Schema
# =============================================================================
# For structs within structs, arrays, and maps.

def get_complex_schema() -> StructType:
    """Define a complex nested schema."""
    return StructType([
        StructField("user_id", LongType(), False),
        StructField("address", StructType([  # Nested struct
            StructField("street", StringType(), True),
            StructField("city",   StringType(), True),
            StructField("zip",    StringType(), True),
        ]), True),
        StructField("tags",   ArrayType(StringType()), True),  # Array
        StructField("scores", MapType(StringType(), DoubleType()), True),  # Map
    ])


def demo_nested_schema():
    """Demonstrate nested schema with structs, arrays, and maps."""
    from pyspark.sql import Row

    # Create nested data
    rdd = spark.sparkContext.parallelize([
        (1, ("123 Main St", "Dallas", "75201"), ["premium", "active"], {"math": 95.0, "english": 88.0}),
        (2, ("456 Oak Ave", "Austin", "78701"), ["basic"], {"math": 78.0}),
    ])

    schema = get_complex_schema()
    df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    df.show(truncate=False)
    return df


# =============================================================================
# APPROACH 5: Schema from Existing Catalog Table
# =============================================================================
# Reuse schema from Unity Catalog or Hive metastore.

def demo_catalog_schema(catalog_table: str):
    """Reuse schema from an existing catalog table."""
    # Get schema from existing table
    catalog_schema = spark.table(catalog_table).schema

    # Apply to new RDD
    rdd = spark.sparkContext.parallelize([...])  # Your data
    df = spark.createDataFrame(rdd, catalog_schema)
    return df


# =============================================================================
# UTILITY: Schema Comparison
# =============================================================================

def compare_schemas(schema1: StructType, schema2: StructType) -> dict:
    """Compare two schemas and report differences."""
    fields1 = {f.name: f for f in schema1.fields}
    fields2 = {f.name: f for f in schema2.fields}

    only_in_1 = set(fields1.keys()) - set(fields2.keys())
    only_in_2 = set(fields2.keys()) - set(fields1.keys())

    type_mismatches = []
    for name in set(fields1.keys()) & set(fields2.keys()):
        if str(fields1[name].dataType) != str(fields2[name].dataType):
            type_mismatches.append({
                "field": name,
                "type1": str(fields1[name].dataType),
                "type2": str(fields2[name].dataType),
            })

    return {
        "only_in_schema1": list(only_in_1),
        "only_in_schema2": list(only_in_2),
        "type_mismatches": type_mismatches,
        "compatible": len(only_in_1) == 0 and len(type_mismatches) == 0,
    }


if __name__ == "__main__":
    print("=== Schema Inference (avoid in prod) ===")
    demo_schema_inference()

    print("\n=== Explicit StructType (recommended) ===")
    demo_explicit_schema()

    print("\n=== DDL String Schema ===")
    demo_ddl_schema()

    print("\n=== Nested Schema ===")
    demo_nested_schema()
