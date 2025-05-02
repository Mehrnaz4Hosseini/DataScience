from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from configs import KAFKA_BOOTSTRAP_SERVERS, SPARK_MASTER
from src.batch_processing.utils.mongo_utils import save_to_mongo

def run_commission_analysis():
    spark = SparkSession.builder \
        .appName("CommissionAnalysis") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "darooghe.valid_transactions") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", 
            "transaction_id STRING, "
            "timestamp TIMESTAMP, "
            "customer_id STRING, "
            "merchant_id STRING, "
            "merchant_category STRING, "
            "payment_method STRING, "
            "amount DOUBLE, "
            "status STRING, "
            "commission_type STRING, "
            "commission_amount DOUBLE, "
            "vat_amount DOUBLE, "
            "total_amount DOUBLE, "
            "customer_type STRING, "
            "risk_level INT"
        ).alias("data")) \
        .select("data.*")

    # I. Commission Efficiency
    efficiency_report = df.groupBy("merchant_category").agg(
        F.sum("commission_amount").alias("total_commissions"),
        F.avg("commission_amount").alias("avg_commission"),
        (F.sum("commission_amount")/F.sum("amount")).alias("commission_ratio"),
        F.count("*").alias("transaction_count")
    )

    # II. Optimal Commission Models
    profit_analysis = df.groupBy("merchant_category", "commission_type").agg(
        F.avg("commission_amount").alias("avg_commission"),
        (F.sum("commission_amount") - (F.sum("amount")*0.01)).alias("net_profit"),  # 1% processing cost
        F.count("*").alias("transaction_count")
    )
    
    # Find best commission type per category
    optimal_types = profit_analysis.groupBy("merchant_category").agg(
        F.max_by("commission_type", "net_profit").alias("optimal_commission_type"),
        F.max("net_profit").alias("estimated_profit")
    )

    save_to_mongo(efficiency_report, "commission_efficiency")
    save_to_mongo(profit_analysis, "commission_profitability")
    save_to_mongo(optimal_types, "optimal_commission_types")
    
    spark.stop()

if __name__ == "__main__":
    run_commission_analysis()
