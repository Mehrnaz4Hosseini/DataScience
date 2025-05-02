from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import atexit
import threading
from pyspark.sql.window import Window
import time

from configs import *
from src.realtime_processing.schemas import transaction_schema

class DaroogheStreamProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.queries = {}
        
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

    def _load_customer_profiles(self):
        """Load customer profiles from MongoDB for amount anomaly detection"""
        try:
            return self.spark.read \
                .format("mongo") \
                .option("uri", "mongodb://localhost:27017/darooghe.aggregated_transactions_customer_profiles") \
                .option("connectionTimeoutMS", "5000") \
                .load() \
                .select(
                    "customer_id",
                    "avg_transaction_amount",
                    "stddev_transaction_amount",
                    "total_transactions"
                )
        except Exception as e:
            print(f"Error loading customer profiles: {e}")
            # Return empty DataFrame with schema if loading fails
            return self.spark.createDataFrame([], StructType([
                StructField("customer_id", StringType()),
                StructField("avg_transaction_amount", DoubleType()),
                StructField("stddev_transaction_amount", DoubleType()),
                StructField("total_transactions", LongType())
            ]))

    def _fraud_detection(self, parsed_stream):
        # Load customer profiles
        customer_profiles = self._load_customer_profiles()
        
        # A. Velocity check: More than 5 transactions from same customer in 2 minutes
        velocity_check = parsed_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "2 minutes"),
                col("customer_id")
            ).agg(
                count("*").alias("transaction_count"),
                collect_list(struct("*")).alias("transactions")
            ) \
            .filter(col("transaction_count") > 5) \
            .withColumn("fraud_type", lit("VELOCITY_CHECK")) \
            .withColumn("fraud_description", 
                    concat(lit("Customer "), col("customer_id"), 
                            lit(" made "), col("transaction_count"), 
                            lit(" transactions in 2 minutes (threshold: 5)"))) \
            .withColumn("fraud_timestamp", current_timestamp()) \
            .select(
                col("fraud_type"),
                col("fraud_description"),
                col("fraud_timestamp"),
                col("transactions")
            )
        
        # C. Amount anomaly: Transaction amount >1000% of customer's average
        amount_anomaly = parsed_stream \
            .join(customer_profiles, "customer_id", "left") \
            .withColumn("is_anomaly", 
                    (col("amount") > (col("avg_transaction_amount") * 10)) & 
                    (col("avg_transaction_amount").isNotNull())) \
            .filter(col("is_anomaly") == True) \
            .withColumn("fraud_type", lit("AMOUNT_ANOMALY")) \
            .withColumn("fraud_description", 
                    concat(lit("Customer "), col("customer_id"), 
                            lit(" made transaction of "), col("amount"), 
                            lit(" which is >1000% of their average ("), 
                            format_number(col("avg_transaction_amount"), 2), lit(")"))) \
            .withColumn("fraud_timestamp", current_timestamp()) \
            .select(
                col("fraud_type"),
                col("fraud_description"),
                col("fraud_timestamp"),
                struct("*").alias("transaction")
            )
        
        # Combine both fraud detection results
        combined_fraud_alerts = velocity_check.unionByName(
            amount_anomaly.select(
                col("fraud_type"),
                col("fraud_description"),
                col("fraud_timestamp"),
                array(col("transaction")).alias("transactions")
            ),
            allowMissingColumns=True
        )
        
        # Write fraud alerts to Kafka
        self.queries["fraud_alert_query"] = combined_fraud_alerts.select(
            to_json(struct("*")).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", "darooghe.fraud_alerts") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/fraud_detection") \
            .outputMode("append") \
            .start()

    def process_stream(self):
        # 1. Read from Kafka
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", "darooghe.valid_transactions") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # 2. Parse with schema
        parsed_stream = raw_stream.select(
            from_json(col("value").cast("string"), transaction_schema).alias("data")
        ).select("data.*")

        # Run fraud detection
        self._fraud_detection(parsed_stream)

        # 3. Windowed processing
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
    
        # 4. Write to output topic
        self.queries["main_query"] = windowed_agg.select(
            to_json(struct("*")).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", OUTPUT_TOPIC) \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/windowed_agg") \
            .outputMode("complete") \
            .start()

        # Commission Analytics
        # I.A. Total commission by type per minute
        commission_by_type = parsed_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("commission_type")
            ).agg(
                sum("commission_amount").alias("total_commission_by_type"),
                count("*").alias("transaction_count")
            )
        
        self.queries["commission_type_query"] = commission_by_type.select(
            to_json(struct(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("commission_type"),
                col("total_commission_by_type"),
                col("transaction_count")
            )).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", "darooghe.commission_by_type") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/commission_analytics/commission_by_type") \
            .outputMode("complete") \
            .start()

        # I.B. Commission ratio by merchant category
        commission_ratio = parsed_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("merchant_category")
            ).agg(
                (sum("commission_amount") / sum("amount")).alias("commission_ratio"),
                sum("commission_amount").alias("total_commission"),
                sum("amount").alias("total_amount")
            )
        
        self.queries["commission_ratio_query"] = commission_ratio.select(
            to_json(struct(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("merchant_category"),
                col("commission_ratio"),
                col("total_commission"),
                col("total_amount")
            )).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", "darooghe.commission_ratio") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/commission_analytics/commission_ratio") \
            .outputMode("complete") \
            .start()

        # I.C. Highest commission-generating merchants in 5-minute windows
        def process_top_merchants(df, epoch_id):
            # Convert to pandas DataFrame
            pdf = df.toPandas()
            
            # Sort by total_commission in descending order and get top 10
            top_10 = pdf.sort_values("total_commission", ascending=False).head(10)
            
            # Convert back to Spark DataFrame
            if not top_10.empty:
                spark_df = processor.spark.createDataFrame(top_10)
                
                # Write to Kafka
                spark_df.select(
                    to_json(struct(
                        col("window_start"),
                        col("window_end"),
                        col("merchant_id"),
                        col("total_commission"),
                        col("transaction_count")
                    )).alias("value")
                ).write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("topic", "darooghe.top_commission_merchants") \
                    .save()

        # Create the aggregated DataFrame without sorting
        top_merchants = parsed_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("merchant_id")
            ).agg(
                sum("commission_amount").alias("total_commission"),
                count("*").alias("transaction_count")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("merchant_id"),
                col("total_commission"),
                col("transaction_count")
            )

        # Start the streaming query with foreachBatch
        self.queries["top_merchants_query"] = top_merchants \
            .writeStream \
            .foreachBatch(process_top_merchants) \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/commission_analytics/top_merchants") \
            .outputMode("complete") \
            .start()

        return self.queries

    def stop_gracefully(self):
        """Gracefully stop all streaming queries and Spark session"""
        def stop():
            # Stop all queries first
            for name, query in self.queries.items():
                try:
                    print(f"Stopping query: {name}")
                    query.stop()
                    query.awaitTermination(10)  # Wait a bit for each query to stop
                except Exception as e:
                    print(f"Error stopping query {name}: {str(e)}")
            
            # Then stop Spark session
            try:
                print("Stopping Spark session")
                self.spark.stop()
            except Exception as e:
                print(f"Error stopping Spark session: {str(e)}")
        
        # Run the stop function in a separate thread with a timeout
        stopper = threading.Thread(target=stop)
        stopper.start()
        stopper.join(timeout=60)

if __name__ == "__main__":
    processor = DaroogheStreamProcessor()
    queries = processor.process_stream()

    # Register shutdown hook
    atexit.register(processor.stop_gracefully)
    
    try:
        # Wait for any query to terminate
        for query in queries.values():
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt, shutting down...")
    finally:
        processor.stop_gracefully()