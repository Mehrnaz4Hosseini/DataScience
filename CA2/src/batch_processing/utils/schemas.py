# Core transaction schema
from pyspark.sql.types import *

# Complete transaction schema matching your data_ingestion model
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("customer_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_category", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("location", MapType(StringType(), DoubleType()), True),
    StructField("device_info", MapType(StringType(), StringType()), True),
    StructField("status", StringType(), False),
    StructField("commission_type", StringType(), False),
    StructField("commission_amount", DoubleType(), False),
    StructField("vat_amount", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("customer_type", StringType(), False),
    StructField("risk_level", IntegerType(), False),
    StructField("failure_reason", StringType(), True)
])