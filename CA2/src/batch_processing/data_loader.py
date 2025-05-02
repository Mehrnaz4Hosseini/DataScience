from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from configs import KAFKA_BOOTSTRAP_SERVERS, SPARK_MASTER
from src.batch_processing.utils.mongo_utils import save_raw_transactions

def load_transactions_to_mongo():
    """Load all transactions from Kafka to MongoDB"""
    spark = SparkSession.builder \
        .appName("DataLoader") \
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
            "location STRUCT<lat:DOUBLE, lng:DOUBLE>, "
            "device_info MAP<STRING,STRING>, "
            "status STRING, "
            "commission_type STRING, "
            "commission_amount DOUBLE, "
            "vat_amount DOUBLE, "
            "total_amount DOUBLE, "
            "customer_type STRING, "
            "risk_level INT, "
            "failure_reason STRING"
        ).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", F.to_timestamp("timestamp"))

    save_raw_transactions(df)
    
    spark.stop()

if __name__ == "__main__":
    load_transactions_to_mongo()