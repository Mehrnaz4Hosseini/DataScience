from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import atexit
import threading
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import radians, sin, cos, sqrt, atan2, lit

from configs import *
from src.realtime_processing.schemas import transaction_schema


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371 
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

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
            return self.spark.createDataFrame([], StructType([
                StructField("customer_id", StringType()),
                StructField("avg_transaction_amount", DoubleType()),
                StructField("stddev_transaction_amount", DoubleType()),
                StructField("total_transactions", LongType())
            ]))

        
    def _fraud_detection(self, parsed_stream):
        customer_profiles = self._load_customer_profiles()
        
        parsed_stream = parsed_stream \
            .withColumn("latitude", col("location.lat")) \
            .withColumn("longitude", col("location.lng"))
        
        # A. Velocity check: More than 5 transactions from same customer in 2 minutes
        velocity_check = parsed_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "2 minutes", "30 seconds"),
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
        
        # B. Geographical impossibility: Transactions >50 km apart within 5 minutes
        def update_customer_state(key, values, state):
            transactions = list(values)
            prev_transaction = state.getOption().getOrElse(None)
            
            results = []
            if prev_transaction is not None:
                time_diff = (transactions[0]['timestamp'].timestamp() - 
                            prev_transaction['timestamp'].timestamp())
                
                if time_diff <= 300:
                    lat1 = prev_transaction['latitude']
                    lon1 = prev_transaction['longitude']
                    lat2 = transactions[0]['latitude']
                    lon2 = transactions[0]['longitude']
                    
                    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
                    dlat = lat2 - lat1
                    dlon = lon2 - lon1
                    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                    c = 2 * atan2(sqrt(a), sqrt(1-a))
                    distance_km = 6371 * c
                    
                    if distance_km > 50:
                        results.append({
                            'fraud_type': 'GEO_IMPOSSIBILITY',
                            'fraud_description': f"Customer {key[0]} made transactions {distance_km:.2f} km apart within 5 minutes",
                            'fraud_timestamp': datetime.now(),
                            'transactions': [prev_transaction, transactions[0]]
                        })

            state.update(transactions[0])
            
            return iter(results)
        
        # B. Geographical impossibility: Transactions >50 km apart within 5 minutes
        state_schema = StructType([
            StructField("transaction_id", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("customer_id", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
        ])
        
        output_schema = StructType([
            StructField("fraud_type", StringType()),
            StructField("fraud_description", StringType()),
            StructField("fraud_timestamp", TimestampType()),
            StructField("transactions", ArrayType(
                StructType([
                    StructField("transaction_id", StringType()),
                    StructField("timestamp", TimestampType()),
                    StructField("customer_id", StringType()),
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType()),
                ])
            ))
        ])
        
        geo_impossibility = parsed_stream \
            .groupBy("customer_id") \
            .applyInPandasWithState(
                lambda key, values, state: update_customer_state(key, values, state),
                output_schema,
                state_schema,
                "append",
                "ProcessingTimeTimeout"
            )
        
        # C. Amount anomaly: Transaction amount >1000% of customer's average
        amount_anomaly = parsed_stream \
            .join(customer_profiles, "customer_id", "left") \
            .withColumn("is_anomaly", 
                    (col("amount") > (col("avg_transaction_amount") * 1)) & 
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
        
        combined_fraud_alerts = velocity_check.unionByName(
            amount_anomaly.select(
                col("fraud_type"),
                col("fraud_description"),
                col("fraud_timestamp"),
                array(col("transaction")).alias("transactions")
            ),
            allowMissingColumns=True
        )
        
        self.queries["fraud_alert_query"] = combined_fraud_alerts.select(
            to_json(struct("*")).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", "darooghe.fraud_alerts") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/fraud_detection_3") \
            .outputMode("append") \
            .start()
    
    def process_stream(self):
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", "darooghe.valid_transactions") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        parsed_stream = raw_stream.select(
            from_json(col("value").cast("string"), transaction_schema).alias("data")
        ).select("data.*")

        self._fraud_detection(parsed_stream)

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
    
        self.queries["main_query"] = windowed_agg.select(
            to_json(struct("*")).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", OUTPUT_TOPIC) \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/windowed_agg_3") \
            .outputMode("complete") \
            .start()

        # Commission Analytics
        # I.A. Total commission by type per minute
        commission_by_type = parsed_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute", "20 seconds"),
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
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/commission_analytics_3/commission_by_type") \
            .outputMode("complete") \
            .start()

        # I.B. Commission ratio by merchant category
        commission_ratio = parsed_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute", "20 seconds"),
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
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/commission_analytics_3/commission_ratio") \
            .outputMode("complete") \
            .start()

        # I.C. Highest commission-generating merchants in 5-minute windows
        def process_top_merchants(df, epoch_id):
            pdf = df.toPandas()

            top_10 = pdf.sort_values("total_commission", ascending=False).head(10)

            if not top_10.empty:
                spark_df = processor.spark.createDataFrame(top_10)
                
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

        self.queries["top_merchants_query"] = top_merchants \
            .writeStream \
            .foreachBatch(process_top_merchants) \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/commission_analytics_3/top_merchants") \
            .outputMode("complete") \
            .start()

        return self.queries

    def stop_gracefully(self):
        """Gracefully stop all streaming queries and Spark session"""
        def stop():
            for name, query in self.queries.items():
                try:
                    print(f"Stopping query: {name}")
                    query.stop()
                    query.awaitTermination(10)
                except Exception as e:
                    print(f"Error stopping query {name}: {str(e)}")
            
            try:
                print("Stopping Spark session")
                self.spark.stop()
            except Exception as e:
                print(f"Error stopping Spark session: {str(e)}")
        
        stopper = threading.Thread(target=stop)
        stopper.start()
        stopper.join(timeout=60)

if __name__ == "__main__":
    processor = DaroogheStreamProcessor()
    queries = processor.process_stream()

    atexit.register(processor.stop_gracefully)
    
    try:
        for query in queries.values():
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt, shutting down...")
    finally:
        processor.stop_gracefully()