from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import atexit
import threading

from configs import *
from src.realtime_processing.schemas import transaction_schema

class DaroogheStreamProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        return SparkSession.builder \
            .appName("DaroogheRealTimeProcessor") \
            .master(SPARK_MASTER) \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "true") \
            .config("spark.sql.streaming.unsupportedOperationCheck", "true") \
            .getOrCreate()

    def process_stream(self):
        # 1. Read from Kafka (Requirement I)
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # 2. Parse with schema
        parsed_stream = raw_stream.select(
            from_json(col("value").cast("string"), transaction_schema).alias("data")
        ).select("data.*")

        # 3. Windowed processing (Requirement II)
        windowed_agg = parsed_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute", "20 seconds"),
                col("merchant_category")
            ).agg(
                count("*").alias("transaction_count"),
                avg("amount").alias("avg_amount"),
                sum("commission_amount").alias("total_commission")
            )
    
        # 4. Write to output topic (Requirement II)
        query = windowed_agg.select(
            to_json(struct("*")).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", OUTPUT_TOPIC) \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/windowed_agg") \
            .outputMode("append") \
            .start()

        return query

    
    def stop_gracefully(self, query):
        """Gracefully stop the streaming query"""
        def stop():
            query.stop()
        
        stopper = threading.Thread(target=stop)
        stopper.start()
        stopper.join(timeout=60)

if __name__ == "__main__":
    processor = DaroogheStreamProcessor()
    query = processor.process_stream()

    atexit.register(lambda: processor.stop_gracefully(query))
    
    query.awaitTermination()
