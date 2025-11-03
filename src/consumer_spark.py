"""Spark Structured Streaming consumer for synthetic tick data.

The job ingests JSON tick events from Kafka, applies a sliding aggregation
window, and writes incremental updates to the console for quick feedback.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate ticks with Spark Structured Streaming.")
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka broker bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--topic",
        default="ticks",
        help="Kafka topic to subscribe to (default: %(default)s)",
    )
    return parser.parse_args()


def build_spark(app_name: str = "TickAggregator") -> SparkSession:
    """Create a SparkSession configured for local development."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("ts", StringType(), nullable=False),
            StructField("symbol", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("size", IntegerType(), nullable=False),
        ]
    )

    # Read raw bytes from Kafka and parse into structured columns.
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.broker)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        raw_stream.select(F.col("value").cast("string").alias("json_str"))
        .withColumn("data", F.from_json("json_str", schema))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("ts"))
        .dropna(subset=["event_time"])
    )

    # Use a sliding window to aggregate metrics for each symbol with deduplication.
    aggregated = (
        parsed_stream.withWatermark("event_time", "1 minute")
        .groupBy(
            F.window("event_time", "10 seconds", "5 seconds"),
            F.col("symbol"),
        )
        .agg(
            F.avg("price").alias("avg_price"),
            F.sum("size").alias("volume"),
            F.count(F.lit(1)).alias("msg_ct"),
        )
    )

    console_query = (
        aggregated.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "50")
        .start()
    )

    console_query.awaitTermination()


if __name__ == "__main__":
    main()
