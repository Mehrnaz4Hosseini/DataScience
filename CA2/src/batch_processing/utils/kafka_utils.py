from configs import KAFKA_BOOTSTRAP_SERVERS
from pyspark.sql import functions as F

def save_to_kafka(df, topic):
    print(f"\nWriting to {topic}. Sample data:")
    df.show(5)
    
    df.select(
        F.to_json(F.struct("*")).alias("value")
    ).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", topic) \
        .save()
    print("Write completed\n")