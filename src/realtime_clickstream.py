from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    countDistinct,
    date_format,
    from_json,
    hour,
    lit,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("page", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("device_type", StringType(), False),
        StructField("country", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ]
)


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Beginner-friendly PySpark clickstream streaming job")
    parser.add_argument("--input", default="data/landing", help="Folder where JSON clickstream files arrive")
    parser.add_argument("--output", default="output/metrics", help="Folder for streaming parquet output")
    parser.add_argument("--checkpoint", default="output/checkpoints/clickstream", help="Checkpoint folder")
    parser.add_argument(
        "--sink",
        choices=["console", "parquet"],
        default="console",
        help="Where to send the aggregated metrics",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    checkpoint_path = Path(args.checkpoint)
    input_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    spark = build_spark("ECommerceClickstreamStreaming")
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("json")
        .schema(CLICKSTREAM_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .load(str(input_path))
    )

    events = (
        raw_stream.withColumn("event_ts", to_timestamp(col("event_time")))
        .withColumn("event_date", date_format(col("event_ts"), "yyyy-MM-dd"))
        .withColumn("event_hour", hour(col("event_ts")))
        .withColumn(
            "revenue",
            when(col("event_type") == "purchase", col("price") * col("quantity")).otherwise(lit(0.0)),
        )
        .withWatermark("event_ts", "10 minutes")
    )

    kpi_by_minute = (
        events.groupBy(window(col("event_ts"), "1 minute").alias("time_window"))
        .agg(
            count("*").alias("events"),
            approx_count_distinct("user_id").alias("unique_users"),
            spark_sum("revenue").alias("revenue"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            spark_sum(when(col("event_type") == "cart_add", 1).otherwise(0)).alias("cart_adds"),
        )
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("events"),
            col("unique_users"),
            col("revenue"),
            col("purchases"),
            col("cart_adds"),
            when(col("cart_adds") > 0, col("purchases") / col("cart_adds")).otherwise(lit(0.0)).alias(
                "cart_to_purchase_rate"
            ),
        )
    )

    top_pages = (
        events.groupBy(window(col("event_ts"), "5 minutes").alias("time_window"), col("page"))
        .agg(count("*").alias("page_views"))
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("page"),
            col("page_views"),
        )
    )

    conversion_view = (
        events.groupBy(window(col("event_ts"), "10 minutes").alias("time_window"))
        .agg(
            approx_count_distinct(when(col("event_type") == "session_start", col("session_id"))).alias("sessions"),
            approx_count_distinct(when(col("event_type") == "purchase", col("session_id"))).alias(
                "purchase_sessions"
            ),
        )
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("sessions"),
            col("purchase_sessions"),
            when(col("sessions") > 0, col("purchase_sessions") / col("sessions")).otherwise(lit(0.0)).alias(
                "session_conversion_rate"
            ),
        )
    )

    if args.sink == "console":
        query = (
            kpi_by_minute.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .option("checkpointLocation", str(checkpoint_path / "kpis"))
            .trigger(processingTime="10 seconds")
            .start()
        )
        query2 = (
            top_pages.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .option("checkpointLocation", str(checkpoint_path / "pages"))
            .trigger(processingTime="10 seconds")
            .start()
        )
        query3 = (
            conversion_view.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .option("checkpointLocation", str(checkpoint_path / "conversion"))
            .trigger(processingTime="10 seconds")
            .start()
        )
        spark.streams.awaitAnyTermination()
        query.stop()
        query2.stop()
        query3.stop()
    else:
        query = (
            kpi_by_minute.writeStream.outputMode("append")
            .format("parquet")
            .option("path", str(output_path / "kpis"))
            .option("checkpointLocation", str(checkpoint_path / "kpis"))
            .trigger(processingTime="10 seconds")
            .start()
        )
        query2 = (
            top_pages.writeStream.outputMode("append")
            .format("parquet")
            .option("path", str(output_path / "top_pages"))
            .option("checkpointLocation", str(checkpoint_path / "pages"))
            .trigger(processingTime="10 seconds")
            .start()
        )
        query3 = (
            conversion_view.writeStream.outputMode("append")
            .format("parquet")
            .option("path", str(output_path / "conversion"))
            .option("checkpointLocation", str(checkpoint_path / "conversion"))
            .trigger(processingTime="10 seconds")
            .start()
        )
        spark.streams.awaitAnyTermination()
        query.stop()
        query2.stop()
        query3.stop()


if __name__ == "__main__":
    main()
