from pyspark.sql import DataFrame
import logging
from src.batch_processing.utils.config import MONGO_URI, MONGO_DB

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

def implement_retention_policy():
    """Implements 24-hour retention for raw transactions"""
    return {
        # MongoDB TTL index (auto-deletes after 24 hours)
        "mongo_ttl": f"""
        db.transactions.createIndex(
            {{ "timestamp": 1 }}, 
            {{ expireAfterSeconds: 86400 }}  # 24h in seconds
        )""",
        
        # Spark pre-filter to avoid processing old data
        "spark_filter": "timestamp > current_timestamp() - interval 24 hours"
    }