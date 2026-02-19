

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, trim, when, lit,
    year, month, dayofmonth, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

EVENT_SCHEMA = StructType([
    StructField("InvoiceNumber", StringType(), True),
    StructField("ProductCode", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Quantity", StringType(), True),     # comes as string from producer
    StructField("InvoiceDate", StringType(), True),  # comes as string from producer
    StructField("UnitPrice", StringType(), True),    # comes as string from producer
    StructField("CustomerId", StringType(), True),
    StructField("Country", StringType(), True),
])

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # If you run locally:
        # .master("local[*]")
        .getOrCreate()
    )

def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming: Kafka -> clean -> Parquet")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="sales_events", help="Kafka topic name")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"], help="Kafka startingOffsets")
    parser.add_argument("--out", default="solution6/output/silver_parquet", help="Output path (Parquet)")
    parser.add_argument("--checkpoint", default="solution6/output/checkpoints/sales_silver", help="Checkpoint path")
    args = parser.parse_args()

    spark = build_spark("sales-stream-clean")
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        # .option("failOnDataLoss", "false")  # optional
        .load()
    )

    # Parse JSON
    json_df = raw.selectExpr("CAST(value AS STRING) as json_str")
    parsed = json_df.select(from_json(col("json_str"), EVENT_SCHEMA).alias("e")).select("e.*")

    # Basic cleaning & typing
    cleaned = (
        parsed
        .withColumn("InvoiceNumber", trim(col("InvoiceNumber")))
        .withColumn("ProductCode", trim(col("ProductCode")))
        .withColumn("ProductName", trim(col("ProductName")))
        .withColumn("Country", trim(col("Country")))
        .withColumn("CustomerId", trim(col("CustomerId")))
        # cast numeric
        .withColumn("Quantity", col("Quantity").cast(IntegerType()))
        .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))
    )

    # Parse datetime (try common formats; adjust if needed)
    # Your file often looks like: "2010-12-01 08:26:00" or "12/1/2010 8:26"
    dt1 = to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
    dt2 = to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
    dt3 = to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm")

    cleaned = cleaned.withColumn(
        "InvoiceTS",
        when(dt1.isNotNull(), dt1).when(dt2.isNotNull(), dt2).otherwise(dt3)
    )

    # Derive fields for analytics
    cleaned = (
        cleaned
        .withColumn("InvoiceDateOnly", to_date(col("InvoiceTS")))
        .withColumn("Year", year(col("InvoiceTS")))
        .withColumn("Month", month(col("InvoiceTS")))
        .withColumn("Day", dayofmonth(col("InvoiceTS")))
        .withColumn("TotalPrice", (col("Quantity") * col("UnitPrice")).cast(DoubleType()))
        # cancelled invoices often start with 'C' (if your dataset uses that)
        .withColumn("IsCancelled", col("InvoiceNumber").startswith("C"))
    )

    # Optional: filter obviously bad rows
    cleaned = cleaned.filter(col("InvoiceTS").isNotNull())
    cleaned = cleaned.filter(col("Quantity").isNotNull() & col("UnitPrice").isNotNull())

    # Write to Parquet (partitioned)
    query = (
        cleaned.writeStream
        .format("parquet")
        .option("path", args.out)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("Year", "Month")
        .outputMode("append")
        .start()
    )

    print(f"[RUNNING] Writing Parquet to: {args.out}")
    query.awaitTermination()

if __name__ == "__main__":
    main()
