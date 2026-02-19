

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct

def main():
    parser = argparse.ArgumentParser(description="Batch metrics from silver Parquet")
    parser.add_argument("--in", dest="inp", default="solution6/output/silver_parquet", help="Input Parquet path")
    parser.add_argument("--out", default="solution6/output/gold_metrics", help="Output folder")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("sales-batch-metrics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.inp)

    # مثال 1: فروش روزانه
    daily = (
        df.groupBy("InvoiceDateOnly")
        .agg(
            _sum("TotalPrice").alias("DailyRevenue"),
            countDistinct("InvoiceNumber").alias("DailyOrders"),
            countDistinct("CustomerId").alias("DailyCustomers"),
        )
        .orderBy("InvoiceDateOnly")
    )

    # مثال 2: فروش به تفکیک کشور
    by_country = (
        df.groupBy("Country")
        .agg(
            _sum("TotalPrice").alias("Revenue"),
            countDistinct("InvoiceNumber").alias("Orders"),
            countDistinct("CustomerId").alias("Customers"),
        )
        .orderBy(col("Revenue").desc())
    )

    daily_out = f"{args.out}/daily"
    country_out = f"{args.out}/by_country"

    daily.coalesce(1).write.mode("overwrite").option("header", True).csv(daily_out)
    by_country.coalesce(1).write.mode("overwrite").option("header", True).csv(country_out)

    print(f"[DONE] Wrote: {daily_out} and {country_out}")

if __name__ == "__main__":
    main()
