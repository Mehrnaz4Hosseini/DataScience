from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import DataFrame, functions as F, Window
import logging
from configs import MONGO_URI, MONGO_DB
from src.batch_processing.utils.mongo_utils import save_to_mongo


logger = logging.getLogger(__name__)

class MerchantActivityAnalyzer:
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def analyze_merchant_patterns(self, transactions_df: DataFrame):
        """Run merchant activity and spike analyses"""
        try:
            logger.info("Starting merchant pattern analysis")
            
            # II. Merchant category activity by time of day
            category_activity_df = self.analyze_category_activity(transactions_df)
            save_to_mongo(category_activity_df, "merchant_category_activity")
            
            # III. Merchants with transaction spikes
            spike_merchants_df = self.identify_transaction_spikes(transactions_df)
            save_to_mongo(spike_merchants_df, "merchant_transaction_spikes")
            
            logger.info("Completed merchant pattern analysis")
            return category_activity_df, spike_merchants_df
        except Exception as e:
            logger.error(f"Error in merchant pattern analysis: {str(e)}")
            raise

    def analyze_category_activity(self, df: DataFrame) -> DataFrame:
        """Identify active times for each merchant category"""
        logger.info("Analyzing merchant category activity patterns")
        
        time_segments = [
            (0, 6, "Late Night (12AM-6AM)"),
            (6, 12, "Morning (6AM-12PM)"),
            (12, 18, "Afternoon (12PM-6PM)"), 
            (18, 24, "Evening (6PM-12AM)")
        ]
        
        segments_df = self.spark.createDataFrame(time_segments, ["start_hour", "end_hour", "time_segment"])
        
        result_df = (
            df.withColumn("transaction_hour", F.hour("timestamp"))
            .join(segments_df, 
                (F.col("transaction_hour") >= F.col("start_hour")) & 
                (F.col("transaction_hour") < F.col("end_hour")))
            .groupBy("merchant_category", "time_segment", "start_hour")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount")
            )
            .orderBy("merchant_category", "start_hour") 
        )
        
        return result_df

    def identify_transaction_spikes(self, df: DataFrame) -> DataFrame:
        """Detect merchants with abnormal transaction spikes"""
        logger.info("Identifying merchants with transaction spikes")
        
        hourly_counts = (
            df.withColumn("hour_window", F.date_trunc("hour", "timestamp"))
            .groupBy("merchant_id", "merchant_category", "hour_window")
            .agg(F.count("*").alias("hourly_count"))
        )
        
        window_spec = Window.partitionBy("merchant_id").orderBy("hour_window").rowsBetween(-24, -1)
        
        spike_df = (
            hourly_counts
            .withColumn("24h_avg", F.avg("hourly_count").over(window_spec))
            .withColumn("24h_stddev", F.stddev("hourly_count").over(window_spec))
            .withColumn("is_spike", 
                       (F.col("hourly_count") > (F.col("24h_avg") + 3 * F.col("24h_stddev"))) &
                       (F.col("24h_stddev").isNotNull()) &
                       (F.col("24h_avg") > 5)) 
            .filter(F.col("is_spike") == True)
            .groupBy("merchant_id", "merchant_category")
            .agg(
                F.count("*").alias("spike_count"),
                F.max("hourly_count").alias("peak_volume"),
                F.avg("hourly_count").alias("avg_normal_volume"),
                F.max("hour_window").alias("last_spike_time")
            )
            .orderBy(F.col("spike_count").desc())
        )
        
        return spike_df

if __name__ == "__main__":

    conf = SparkConf() \
        .setAppName("MerchantActivityAnalysis") \
        .set("spark.mongodb.input.uri", MONGO_URI) \
        .set("spark.mongodb.output.uri", MONGO_URI) \
        .set("spark.driver.host", "localhost") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g") \
        .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
    
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    try:
        transactions_df = spark.read \
            .format("mongo") \
            .option("uri", MONGO_URI) \
            .option("database", MONGO_DB) \
            .option("collection", "valid_transactions") \
            .load() \
            .withColumn("timestamp", F.to_timestamp("timestamp"))
        
        analyzer = MerchantActivityAnalyzer(spark)
        category_activity, transaction_spikes = analyzer.analyze_merchant_patterns(transactions_df)     
        
    finally:
        spark.stop()