# Core transaction schema
from pyspark.sql.types import *

# Complete transaction schema matching your data_ingestion model
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("timestamp", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", MapType(StringType(), DoubleType()), True),
    StructField("device_info", MapType(StringType(), StringType()), True),
    StructField("status", StringType(), True),
    StructField("commission_type", StringType(), True),
    StructField("commission_amount", DoubleType(), True),
    StructField("vat_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("customer_type", StringType(), True),
    StructField("risk_level", IntegerType(), True),
    StructField("failure_reason", StringType(), True)
])