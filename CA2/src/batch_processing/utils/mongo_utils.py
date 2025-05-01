from pyspark.sql import DataFrame
import logging
from pymongo import MongoClient
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from configs import MONGO_URI, MONGO_DB, TRANSACTIONS_COLLECTION, AGGREGATED_COLLECTION


logger = logging.getLogger(__name__)

def save_to_mongo(df: DataFrame, collection: str):
    """Save DataFrame to MongoDB collection with error handling"""
    try:
        logger.info(f"Saving {df.count()} records to {collection}")
        
        df.write \
            .format("mongo") \
            .mode("overwrite") \
            .option("uri", MONGO_URI) \
            .option("database", MONGO_DB) \
            .option("collection", collection) \
            .save()
            
        logger.info(f"Successfully saved to {collection}")
    except Exception as e:
        logger.error(f"Error saving to MongoDB: {str(e)}")
        raise

def save_raw_transactions(df: DataFrame):
    """Save raw transactions to MongoDB with partitioning by date"""
    try:
        logger.info(f"Saving {df.count()} raw transactions to MongoDB")
        
        # Add partitioning by date
        df_with_partition = df.withColumn("partition_date", F.date_format("timestamp", "yyyyMMdd"))
        
        df_with_partition.write \
            .format("mongo") \
            .mode("overwrite") \
            .option("uri", MONGO_URI) \
            .option("database", MONGO_DB) \
            .option("collection", TRANSACTIONS_COLLECTION) \
            .option("partitionKey", "partition_date") \
            .save()
            
        logger.info("Successfully saved raw transactions")
        
        # implement_retention_policy()   # -> Retention policy but one of the TAs said it is not needed!
        
    except Exception as e:
        logger.error(f"Error saving raw transactions: {str(e)}")
        raise

def save_aggregated_results(df: DataFrame, collection_suffix: str):
    """Save aggregated results to MongoDB"""
    try:
        collection_name = f"{AGGREGATED_COLLECTION}_{collection_suffix}"
        logger.info(f"Saving {df.count()} records to {collection_name}")
        
        df.write \
            .format("mongo") \
            .mode("overwrite") \
            .option("uri", MONGO_URI) \
            .option("database", MONGO_DB) \
            .option("collection", collection_name) \
            .save()
            
        logger.info(f"Successfully saved to {collection_name}")
        
    except Exception as e:
        logger.error(f"Error saving aggregated results: {str(e)}")
        raise

def implement_retention_policy():
    """Implements 24-hour retention policy for raw transactions (commented out)"""
    # This would be called from save_raw_transactions if enabled
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    # Create TTL index for auto-expiration 
    db[TRANSACTIONS_COLLECTION].create_index(
        "timestamp", 
        expireAfterSeconds=86400  # 24 hours in seconds
    )
    
    # Alternative manual cleanup approach (commented out)
    # cutoff = datetime.utcnow() - timedelta(hours=24)
    # db[TRANSACTIONS_COLLECTION].delete_many({"timestamp": {"$lt": cutoff}})