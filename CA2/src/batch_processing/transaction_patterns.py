from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import config
from src.batch_processing.utils.mongo_utils import save_to_mongo

def analyze_patterns():
    spark = SparkSession.builder \
        .appName("CommissionAnalysis") \
        .master(config.SPARK_MASTER) \
        .config("spark.mongodb.input.uri", f"{config.MONGO_URI}/{config.MONGO_DB}.{config.TRANSACTIONS_COLLECTION}") \
        .config("spark.mongodb.output.uri", f"{config.MONGO_URI}/{config.MONGO_DB}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    df = spark.read \
        .format("mongo") \
        .option("uri", config.MONGO_URI) \
        .option("database", config.MONGO_DB) \
        .option("collection", config.TRANSACTIONS_COLLECTION) \
        .load()\
        .withColumn("timestamp", F.to_timestamp("timestamp"))

    # I. Temporal Patterns (Requirements I & II & V combined)
    temporal_analysis = df.withColumn("hour", F.hour("timestamp")) \
        .withColumn("day_of_week", F.dayofweek("timestamp")) \
        .withColumn("time_of_day", 
            F.when((F.col("hour") >= 6) & (F.col("hour") < 12), "morning")
             .when((F.col("hour") >= 12) & (F.col("hour") < 18), "afternoon")
             .otherwise("evening/night")) \
        .groupBy("day_of_week", "hour", "time_of_day", "merchant_category") \
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_volume"),
            F.avg("amount").alias("avg_amount")
        ).orderBy("day_of_week", "hour")
    
    # II. Peak Transaction Times (Requirement II)
    peak_times = temporal_analysis.orderBy(F.desc("transaction_count")).limit(10)

    # III. Customer Segmentation (Requirement III)
    customer_segments = df.groupBy("customer_id").agg(
        F.count("*").alias("transaction_frequency"),
        F.sum("amount").alias("total_spent"),
        F.avg("amount").alias("avg_transaction_size"),
        F.datediff(F.current_date(), F.max("timestamp")).alias("days_since_last_purchase")
    ).withColumn("segment",
        F.when(F.col("transaction_frequency") > 50, "High Frequency")
         .when(F.col("total_spent") > 5000000, "High Value")
         .when(F.col("days_since_last_purchase") < 7, "Active")
         .otherwise("Standard")
    )

    # IV. Merchant Category Comparison (Requirement IV)
    merchant_comparison = df.groupBy("merchant_category").agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_volume"),
        F.avg("amount").alias("avg_transaction_size"),
        F.avg(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("approval_rate")
    ).orderBy("total_transactions", ascending=False)

    # V. Time-of-Day Analysis (Requirement V)
    time_of_day_analysis = df.withColumn("time_of_day", 
        F.when((F.hour("timestamp") >= 6) & (F.hour("timestamp") < 12), "morning")
         .when((F.hour("timestamp") >= 12) & (F.hour("timestamp") < 18), "afternoon")
         .otherwise("evening/night")
    ).groupBy("time_of_day", "merchant_category").agg(
        F.count("*").alias("transaction_count"),
        F.avg("amount").alias("avg_amount")
    )

    # VI. Spending Trends Over Time (Requirement VI)
    weekly_trends = df.groupBy(
        F.window("timestamp", "1 week").alias("week"),
        "merchant_category"
    ).agg(
        F.count("*").alias("weekly_transactions"),
        F.sum("amount").alias("weekly_volume")
    )

    trend_window = Window.partitionBy("merchant_category").orderBy("week")

    spending_trends = weekly_trends.withColumn(
    "previous_week_volume", F.lag("weekly_volume").over(trend_window)
    ).withColumn(
        "volume_change", F.col("weekly_volume") - F.col("previous_week_volume")
    ).withColumn(
        "is_increasing", F.when(F.col("volume_change") > 0, True).otherwise(False)
    ).select(
        "week", "merchant_category", "weekly_transactions", "weekly_volume", 
        "previous_week_volume", "volume_change", "is_increasing"
    )

    save_to_mongo(temporal_analysis, "temporal_patterns")
    save_to_mongo(peak_times, "peak_transaction_times") 
    save_to_mongo(customer_segments, "customer_segments")
    save_to_mongo(merchant_comparison, "merchant_comparison")
    save_to_mongo(time_of_day_analysis, "time_of_day_analysis")
    save_to_mongo(spending_trends, "spending_trends")

    spark.stop()

if __name__ == "__main__":
    analyze_patterns()
