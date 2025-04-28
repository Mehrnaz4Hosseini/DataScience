from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import config
from src.batch_processing.utils.mongo_utils import save_to_mongo

def run_commission_analysis():
    spark = SparkSession.builder \
        .appName("CommissionAnalysis") \
        .master(config.SPARK_MASTER) \
        .config("spark.mongodb.input.uri", f"{config.MONGO_URI}/{config.MONGO_DB}.{config.TRANSACTIONS_COLLECTION}") \
        .config("spark.mongodb.output.uri", f"{config.MONGO_URI}/{config.MONGO_DB}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    try:
        # Load data from MongoDB with proper options
        df = spark.read \
            .format("mongo") \
            .option("uri", config.MONGO_URI) \
            .option("database", config.MONGO_DB) \
            .option("collection", config.TRANSACTIONS_COLLECTION) \
            .load()
            
            
        if df.count() == 0:
            print("Warning: No data loaded from MongoDB!")
            spark.stop()
            return
            
    except Exception as e:
        print(f"Error loading data from MongoDB: {str(e)}")
        spark.stop()
        return

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

