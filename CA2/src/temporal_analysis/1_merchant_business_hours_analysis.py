from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import BooleanType
from timezonefinder import TimezoneFinder
from datetime import datetime
import pytz
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
            
            outside_hours_df = self.identify_transactions_outside_business_hours(transactions_df)
            save_to_mongo(outside_hours_df, "transactions_outside_business_hours")

            
            logger.info("Completed merchant pattern analysis")
            return outside_hours_df
        except Exception as e:
            logger.error(f"Error in merchant pattern analysis: {str(e)}")
            raise

    def identify_transactions_outside_business_hours(self, df: DataFrame) -> DataFrame:
        """Identify transactions that occurred outside merchant's local business hours (9AM–5PM)"""
        logger.info("Identifying transactions outside local business hours")

        tf_data = self.spark.sparkContext.broadcast(TimezoneFinder(in_memory=True))

        def is_outside_business_hours(lat, lon, timestamp):
            try:
                if lat is None or lon is None or timestamp is None:
                    return False
                
                tf = tf_data.value
                tz_name = tf.timezone_at(lng=lon, lat=lat)
                
                if tz_name is None:
                    return False
                    
                local_tz = pytz.timezone(tz_name)
                local_time = timestamp.astimezone(local_tz)
                hour = local_time.hour
                return hour < 9 or hour >= 17
            except Exception:
                return False

        is_outside_udf = F.udf(is_outside_business_hours, BooleanType())

        df_with_location = df.withColumn("latitude", F.col("location")["lat"]) \
                            .withColumn("longitude", F.col("location")["lng"])

        result_df = df_with_location.withColumn("outside_business_hours",
                                            is_outside_udf("latitude", "longitude", "timestamp")) \
                                .filter("outside_business_hours = true")

        return result_df

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark import SparkConf

    conf = SparkConf() \
        .setAppName("MerchantActivityAnalysis") \
        .set("spark.mongodb.input.uri", MONGO_URI) \
        .set("spark.mongodb.output.uri", MONGO_URI) \
        .set("spark.driver.host", "localhost") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g") \
        .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    try:
        transactions_df = spark.read \
            .format("mongo") \
            .option("uri", MONGO_URI) \
            .option("database", MONGO_DB) \
            .option("collection", "valid_transactions") \
            .load() \
            .withColumn("timestamp", F.to_timestamp("timestamp"))

        analyzer = MerchantActivityAnalyzer(spark)
        outside_hours_df= analyzer.analyze_merchant_patterns(transactions_df)

    finally:
        spark.stop()
