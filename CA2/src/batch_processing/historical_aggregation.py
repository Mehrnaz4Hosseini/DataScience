from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from configs import SPARK_MASTER
from src.batch_processing.utils.mongo_utils import save_aggregated_results
from datetime import datetime, timedelta

def create_historical_aggregations():
    spark = SparkSession.builder \
        .appName("HistoricalAggregation") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    # Read from MongoDB raw transactions
    df = spark.read \
        .format("mongo") \
        .option("uri", "mongodb://localhost:27017/darooghe.valid_transactions") \
        .load() \
        .withColumn("timestamp", F.to_timestamp("timestamp"))

    # 1. Summarized Transaction Data
    # Daily aggregation by merchant
    daily_merchant = df.groupBy(
        F.date_format("timestamp", "yyyy-MM-dd").alias("date"),
        "merchant_id",
        "merchant_category"
    ).agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("daily_volume"),
        F.avg("amount").alias("avg_transaction_amount")
    )
    
    # Weekly customer segment analysis
    weekly_segments = df.groupBy(
        F.year("timestamp").alias("year"),
        F.weekofyear("timestamp").alias("week_num"),
        "customer_type"
    ).agg(
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("amount").alias("weekly_volume")
    ).withColumn("week", F.concat(F.col("year"), F.lit("-"), F.col("week_num")))

    # 2. Commission Reports
    # Monthly commission by category
    monthly_commissions = df.groupBy(
        F.date_format("timestamp", "yyyy-MM").alias("month"),
        "merchant_category",
    ).agg(
        F.sum("commission_amount").alias("total_commission"),
        F.avg("commission_amount").alias("avg_commission"),
        (F.sum("commission_amount")/F.sum("amount")).alias("commission_ratio")
    )

    # Save all historical aggregations
    save_aggregated_results(daily_merchant, "daily_merchant_summary")
    save_aggregated_results(weekly_segments, "weekly_customer_segments")
    save_aggregated_results(monthly_commissions, "monthly_commission_reports")

    spark.stop()

if __name__ == "__main__":
    create_historical_aggregations()