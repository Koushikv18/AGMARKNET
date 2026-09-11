"""
process.py — PySpark cleaning and processing job for AGMARKNET data.

Reads raw CSVs (local or HDFS), applies a full cleaning pipeline,
derives date columns, standardises text, and writes Parquet.

Usage:
    python src/process.py               # full run (local mode)
    python src/process.py --sample      # smoke-test on 10,000 rows
    python src/process.py --master yarn # YARN cluster mode
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

load_dotenv()

# Set up HADOOP_HOME on Windows if winutils is present
if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
    candidate = Path.home() / "hadoop"
    if (candidate / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(candidate)
        os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")

# ── Config ────────────────────────────────────────────────────────────────────

SPARK_MASTER   = os.getenv("SPARK_MASTER", "local[*]")
USE_HDFS       = os.getenv("USE_HDFS", "false").lower() == "true"
HDFS_NAMENODE  = os.getenv("HDFS_NAMENODE", "hdfs://localhost:9000")
DATA_DIR       = os.getenv("DATA_DIR", "data")

RAW_LOCAL       = str(Path(DATA_DIR) / "raw")
PROCESSED_LOCAL = str(Path(DATA_DIR) / "processed")
RAW_HDFS        = f"{HDFS_NAMENODE}/user/agmarknet/raw"
PROCESSED_HDFS  = f"{HDFS_NAMENODE}/user/agmarknet/processed"

# ── Schema ────────────────────────────────────────────────────────────────────

RAW_SCHEMA = StructType([
    StructField("state",        StringType(), True),
    StructField("district",     StringType(), True),
    StructField("market",       StringType(), True),
    StructField("commodity",    StringType(), True),
    StructField("variety",      StringType(), True),
    StructField("grade",        StringType(), True),
    StructField("arrival_date", StringType(), True),   # parsed below
    StructField("min_price",    DoubleType(), True),
    StructField("max_price",    DoubleType(), True),
    StructField("modal_price",  DoubleType(), True),
])

# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark(master: str, app_name: str = "AGMARKNET-Process") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # tune shuffle partitions: 2x CPU cores for local, 2x num-executors for cluster
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )
    if USE_HDFS:
        builder = builder.config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    return builder.getOrCreate()


# ── Cleaning pipeline ─────────────────────────────────────────────────────────

def clean(df):
    """Apply full cleaning pipeline; returns cleaned DataFrame."""
    total_raw = df.count()
    print(f"\n{'='*60}")
    print(f"  Raw row count : {total_raw:,}")
    print(f"{'='*60}")

    # 1. Drop rows where critical fields are null / empty
    critical = ["commodity", "market", "arrival_date", "modal_price"]
    for col in critical:
        df = df.filter(F.col(col).isNotNull() & (F.trim(F.col(col)) != ""))

    # 2. Cast price columns to double (may already be double from schema,
    #    but raw CSVs sometimes sneak in string representations)
    for price_col in ["min_price", "max_price", "modal_price"]:
        df = df.withColumn(price_col, F.col(price_col).cast(DoubleType()))

    # 3. Filter invalid prices
    df = df.filter(
        (F.col("min_price")   > 0) &
        (F.col("max_price")   > 0) &
        (F.col("modal_price") > 0) &
        (F.col("min_price")   <= F.col("max_price"))
    )

    # 4. Parse arrival_date (dd/MM/yyyy from AGMARKNET)
    df = df.withColumn(
        "arrival_date",
        F.to_date(F.col("arrival_date"), "dd/MM/yyyy")
    )
    # Drop rows where parse produced null (malformed dates)
    df = df.filter(F.col("arrival_date").isNotNull())

    # 5. Derive year, month columns for partitioning & analysis
    df = df.withColumn("year",  F.year("arrival_date"))
    df = df.withColumn("month", F.month("arrival_date"))

    # 6. Standardise text: trim + initcap on string fields
    text_cols = ["state", "district", "market", "commodity", "variety", "grade"]
    for col in text_cols:
        df = df.withColumn(col, F.initcap(F.trim(F.col(col))))

    # 7. Filter to plausible year range
    current_year = datetime.now().year
    df = df.filter((F.col("year") >= 2013) & (F.col("year") <= current_year + 1))

    total_clean = df.count()
    dropped = total_raw - total_clean
    print(f"\n  Cleaned row count : {total_clean:,}")
    print(f"  Rows dropped      : {dropped:,}  ({100*dropped/max(total_raw,1):.1f}%)")
    print(f"{'='*60}\n")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="AGMARKNET PySpark processing job")
    p.add_argument("--sample", action="store_true",
                   help="Run on a 10,000-row sample (smoke test)")
    p.add_argument("--master", default=None,
                   help="Override SPARK_MASTER (e.g. yarn)")
    return p.parse_args()


def main():
    args = parse_args()
    master = args.master or SPARK_MASTER

    spark = build_spark(master)
    spark.sparkContext.setLogLevel("WARN")

    # ── Read raw CSVs ─────────────────────────────────────────────────────────
    raw_path = RAW_HDFS if USE_HDFS else RAW_LOCAL
    processed_path = PROCESSED_HDFS if USE_HDFS else PROCESSED_LOCAL

    print(f"Reading raw CSVs from: {raw_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")    # use explicit schema
        .option("mode", "DROPMALFORMED")   # drop garbled rows rather than corrupting alignment
        .option("recursiveFileLookup", "true")
        .schema(RAW_SCHEMA)
        .csv(raw_path)
    )

    if args.sample:
        df = df.limit(10_000)
        print("SAMPLE MODE — limited to 10,000 rows")

    # ── Cache (small sample) or not (large data — Parquet handles this) ────────
    if args.sample:
        df.cache()

    # ── Clean ─────────────────────────────────────────────────────────────────
    df_clean = clean(df)

    # ── Print schema ──────────────────────────────────────────────────────────
    df_clean.printSchema()

    # ── Show sample rows ──────────────────────────────────────────────────────
    print("Sample cleaned rows:")
    df_clean.show(10, truncate=False)

    # ── Write Parquet ─────────────────────────────────────────────────────────
    print(f"Writing Parquet to: {processed_path}")
    (
        df_clean
        .repartition("year", "state")
        .write
        .mode("overwrite")
        .partitionBy("year", "state")
        .parquet(processed_path)
    )
    print("Process job complete.")
    spark.stop()


if __name__ == "__main__":
    main()
