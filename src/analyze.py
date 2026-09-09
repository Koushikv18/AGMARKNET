"""
analyze.py — Spark SQL analytics on the cleaned AGMARKNET Parquet dataset.

Produces 7 result CSV files in results/:
  1. top_commodities.csv         — top 10 by record count
  2. monthly_price_trend.csv     — national monthly avg modal price (last 12 m)
  3. monthly_price_by_commodity.csv — monthly avg per top-10 commodity
  4. top_states.csv              — top 5 states by total estimated trade value
  5. price_volatility.csv        — stddev of modal price per commodity
  6. market_inefficiency.csv     — avg price spread (max-min) per market
  7. arbitrage_signal.csv        — state deviation from national avg per day

Usage:
    python src/analyze.py
    python src/analyze.py --sample   (run on processed sample data)
"""

import argparse
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

SPARK_MASTER   = os.getenv("SPARK_MASTER", "local[*]")
USE_HDFS       = os.getenv("USE_HDFS", "false").lower() == "true"
HDFS_NAMENODE  = os.getenv("HDFS_NAMENODE", "hdfs://localhost:9000")
DATA_DIR       = os.getenv("DATA_DIR", "data")
RESULTS_DIR    = Path(os.getenv("RESULTS_DIR", "results"))

PROCESSED_LOCAL = str(Path(DATA_DIR) / "processed")
PROCESSED_HDFS  = f"{HDFS_NAMENODE}/user/agmarknet/processed"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark(master: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("AGMARKNET-Analyze")
        .master(master)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )
    if USE_HDFS:
        builder = builder.config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    return builder.getOrCreate()


# ── Helpers ───────────────────────────────────────────────────────────────────

def save(df_spark, name: str, show: bool = True) -> pd.DataFrame:
    """Collect Spark DataFrame → pandas, save CSV, print head."""
    pdf = df_spark.toPandas()
    path = RESULTS_DIR / f"{name}.csv"
    pdf.to_csv(path, index=False)
    print(f"\n{'─'*60}")
    print(f"  {name}  ({len(pdf)} rows) → {path}")
    print(f"{'─'*60}")
    if show:
        print(pdf.to_string(index=False))
    return pdf


# ── Queries ───────────────────────────────────────────────────────────────────

def q1_top_commodities(spark) -> None:
    """Top 10 commodities by record count."""
    result = spark.sql("""
        SELECT
            commodity,
            COUNT(*)          AS record_count,
            ROUND(AVG(modal_price), 2) AS avg_modal_price,
            ROUND(MIN(modal_price), 2) AS min_modal_price,
            ROUND(MAX(modal_price), 2) AS max_modal_price
        FROM mandi_data
        WHERE commodity IS NOT NULL
        GROUP BY commodity
        ORDER BY record_count DESC
        LIMIT 10
    """)
    save(result, "top_commodities")


def q2_monthly_price_trend(spark) -> None:
    """National monthly average modal price — last 12 months."""
    result = spark.sql("""
        SELECT
            year,
            month,
            DATE_FORMAT(arrival_date, 'yyyy-MM') AS year_month,
            ROUND(AVG(modal_price), 2)            AS avg_modal_price,
            COUNT(*)                               AS record_count
        FROM mandi_data
        WHERE arrival_date >= ADD_MONTHS(CURRENT_DATE(), -12)
        GROUP BY year, month, DATE_FORMAT(arrival_date, 'yyyy-MM')
        ORDER BY year_month
    """)
    save(result, "monthly_price_trend")


def q3_monthly_price_by_commodity(spark) -> None:
    """Monthly avg modal price for the top-10 commodities (last 12 months)."""
    # Build top-10 commodity list dynamically
    top10 = spark.sql("""
        SELECT commodity FROM (
            SELECT commodity, COUNT(*) AS cnt
            FROM mandi_data
            WHERE commodity IS NOT NULL
            GROUP BY commodity
            ORDER BY cnt DESC
            LIMIT 10
        )
    """).toPandas()["commodity"].tolist()

    top10_quoted = ", ".join(f"'{c}'" for c in top10)

    result = spark.sql(f"""
        SELECT
            commodity,
            DATE_FORMAT(arrival_date, 'yyyy-MM') AS year_month,
            ROUND(AVG(modal_price), 2)            AS avg_modal_price,
            COUNT(*)                               AS record_count
        FROM mandi_data
        WHERE arrival_date >= ADD_MONTHS(CURRENT_DATE(), -12)
          AND commodity IN ({top10_quoted})
        GROUP BY commodity, DATE_FORMAT(arrival_date, 'yyyy-MM')
        ORDER BY commodity, year_month
    """)
    save(result, "monthly_price_by_commodity", show=False)
    print(f"  (saved {len(result.toPandas())} rows — not printed for brevity)")


def q4_top_states(spark) -> None:
    """Top 5 states by estimated total trade value (modal_price × record count)."""
    result = spark.sql("""
        SELECT
            state,
            COUNT(*)                                AS record_count,
            ROUND(AVG(modal_price), 2)              AS avg_modal_price,
            ROUND(SUM(modal_price), 0)              AS total_price_sum,
            COUNT(DISTINCT market)                  AS unique_markets,
            COUNT(DISTINCT commodity)               AS unique_commodities
        FROM mandi_data
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY total_price_sum DESC
        LIMIT 5
    """)
    save(result, "top_states")


def q5_price_volatility(spark) -> None:
    """Price volatility: stddev of modal price per commodity."""
    result = spark.sql("""
        SELECT
            commodity,
            COUNT(*)                                        AS record_count,
            ROUND(AVG(modal_price), 2)                      AS avg_modal_price,
            ROUND(STDDEV(modal_price), 2)                   AS stddev_modal_price,
            ROUND(STDDEV(modal_price) / AVG(modal_price) * 100, 2) AS coeff_variation_pct,
            ROUND(MIN(modal_price), 2)                      AS min_price_ever,
            ROUND(MAX(modal_price), 2)                      AS max_price_ever
        FROM mandi_data
        WHERE commodity IS NOT NULL
          AND modal_price > 0
        GROUP BY commodity
        HAVING COUNT(*) > 100
        ORDER BY stddev_modal_price DESC
        LIMIT 20
    """)
    save(result, "price_volatility")


def q6_market_inefficiency(spark) -> None:
    """Market inefficiency: avg spread between max_price and min_price per market."""
    result = spark.sql("""
        SELECT
            state,
            market,
            COUNT(*)                                           AS record_count,
            ROUND(AVG(max_price - min_price), 2)               AS avg_price_spread,
            ROUND(AVG((max_price - min_price) / modal_price * 100), 2) AS avg_spread_pct,
            ROUND(AVG(modal_price), 2)                         AS avg_modal_price
        FROM mandi_data
        WHERE min_price > 0 AND max_price > 0 AND modal_price > 0
        GROUP BY state, market
        HAVING COUNT(*) > 50
        ORDER BY avg_spread_pct DESC
        LIMIT 20
    """)
    save(result, "market_inefficiency")


def q7_arbitrage_signal(spark) -> None:
    """
    Arbitrage / exploitation signal:
    For each commodity + date, compute each state's deviation from
    the national average modal price.  States with consistently high
    positive deviation may indicate price gouging or supply bottlenecks.
    """
    # Window for national average per commodity per date
    w = Window.partitionBy("commodity", "arrival_date")

    df = spark.table("mandi_data")

    arb = (
        df
        .filter(F.col("commodity").isNotNull() & (F.col("modal_price") > 0))
        .withColumn("national_avg",  F.avg("modal_price").over(w))
        .withColumn("deviation_pct",
                    (F.col("modal_price") - F.col("national_avg"))
                    / F.col("national_avg") * 100)
        .groupBy("state", "commodity")
        .agg(
            F.count("*").alias("record_count"),
            F.round(F.avg("modal_price"), 2).alias("avg_state_price"),
            F.round(F.avg("national_avg"), 2).alias("avg_national_price"),
            F.round(F.avg("deviation_pct"), 2).alias("avg_deviation_pct"),
            F.round(F.max("deviation_pct"), 2).alias("max_deviation_pct"),
        )
        .filter(F.col("record_count") > 50)
        .orderBy(F.desc("avg_deviation_pct"))
        .limit(30)
    )
    save(arb, "arbitrage_signal")


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="AGMARKNET Spark SQL analytics")
    p.add_argument("--sample", action="store_true",
                   help="Run on a small subset (for testing)")
    p.add_argument("--master", default=None,
                   help="Override SPARK_MASTER")
    return p.parse_args()


def main():
    args = parse_args()
    master = args.master or SPARK_MASTER

    spark = build_spark(master)
    spark.sparkContext.setLogLevel("WARN")

    processed_path = PROCESSED_HDFS if USE_HDFS else PROCESSED_LOCAL

    print(f"Reading processed Parquet from: {processed_path}")
    df = spark.read.parquet(processed_path)

    if args.sample:
        df = df.sample(fraction=0.01, seed=42)
        print("SAMPLE MODE — using 1% of data")

    df.createOrReplaceTempView("mandi_data")

    print(f"\nTotal records in view: {df.count():,}")
    print(f"Date range: {df.agg(F.min('arrival_date'), F.max('arrival_date')).collect()[0]}")
    print(f"Unique commodities: {df.select('commodity').distinct().count()}")
    print(f"Unique states: {df.select('state').distinct().count()}")
    print(f"Unique markets: {df.select('market').distinct().count()}")

    print("\n\n[Q1] Top 10 commodities by record count")
    q1_top_commodities(spark)

    print("\n[Q2] National monthly price trend (last 12 months)")
    q2_monthly_price_trend(spark)

    print("\n[Q3] Monthly price by top commodity")
    q3_monthly_price_by_commodity(spark)

    print("\n[Q4] Top 5 states by trade value")
    q4_top_states(spark)

    print("\n[Q5] Price volatility per commodity")
    q5_price_volatility(spark)

    print("\n[Q6] Market inefficiency (price spread)")
    q6_market_inefficiency(spark)

    print("\n[Q7] Arbitrage / exploitation signal")
    q7_arbitrage_signal(spark)

    print("\n\nAll analytics complete. Results saved to:", RESULTS_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
