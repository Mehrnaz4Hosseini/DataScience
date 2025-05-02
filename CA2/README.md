
### **Part 1 (Set up):**

#### 1. First, completely stop any Kafka remnants:
```bash
pkill -f kafka
```

#### 2. Start Kafka in KRaft mode properly:
```bash
# Generate a new cluster ID
KAFKA_CLUSTER_ID=$(/opt/homebrew/bin/kafka-storage random-uuid)

# Format storage (creates fresh data directory)
/opt/homebrew/bin/kafka-storage format -t $KAFKA_CLUSTER_ID -c /opt/homebrew/etc/kafka/kraft/server.properties

# Start Kafka (keep this terminal open)
/opt/homebrew/bin/kafka-server-start /opt/homebrew/etc/kafka/kraft/server.properties
```

#### 3. Verify Kafka is running:
In a **new terminal**:
```bash
ps aux | grep kafka
```
Now you should see 2-3 Java processes related to Kafka.

#### 4. Check Kafka logs:
The terminal where you started Kafka should show:
```
[KafkaServer id=1] started (kafka.server.KafkaServer)
```
with no serious errors.

#### 5. Create your topic (in a new terminal):
```bash
/opt/homebrew/bin/kafka-topics --create \
  --topic darooghe.transactions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

```bash
/opt/homebrew/bin/kafka-topics --create \
  --topic darooghe.error_logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
  ```
```bash

```bash
/opt/homebrew/bin/kafka-topics --create \
  --topic darooghe.valid_transactions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

```bash
kafka-topics --create \
  --topic darooghe.realtime_metrics \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
  ```

```bash
kafka-topics --create \
  --topic darooghe.fraud_alerts \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
  ```

  ```bash
kafka-topics --create \
  --topic darooghe.commission_by_type \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
  ```

  ```bash
kafka-topics --create \
  --topic darooghe.commission_ratio \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
  ```

  ```bash
kafka-topics --create \
  --topic darooghe.top_commission_merchants\
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
  ```
  

#### 6. Test the connection:
```bash
nc -zv localhost 9092
```
Should return "succeeded" message.


#### 7. See topics:
```bash
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --list
```
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic darooghe.valid_transactions --from-beginning
```


Here's how to **completely stop Kafka and reset all data** to start fresh:

#### 1. Stop Kafka Process
In the terminal where Kafka is running, press:
```bash
Ctrl + C
```

#### 2. Kill Any Remaining Processes
```bash
pkill -f kafka
pkill -f java  # Kafka runs as a Java process
```

#### 3. Delete All Kafka Data
```bash
rm -rf /tmp/kraft-combined-logs/
```
*This removes all Kafka message data and offsets.*

#### 4. Verify Clean State
```bash
ps aux | grep kafka  # Should show no Kafka processes
ls /tmp/kraft-combined-logs  # Should show "No such file or directory"
```

---

### **Part 2 (Data Ingestion):**


#### 1. Run your producer:
```bash
export KAFKA_BROKER=localhost:9092
python data_generators/darooghe_pulse.py
```

#### 2. Run your consumer in another terminal:
```bash
python src/data_ingestion/consumer.py
```


---

### **Part 3 (Batch Processing):**

#### 1. Set up MongoDB:
```bash
brew services stop mongodb-community
pkill -f mongod

# Remove lock files
rm -f ~/data/db/mongod.lock

# Start MongoDB (keep terminal open)
brew services start mongodb-community
mongod --dbpath ~/data/db --logpath ~/data/db/mongod.log --logappend
```


#### 2. Create your topic (in a new terminal): -> I did it wrong, not this step, every ou

```bash
/opt/homebrew/bin/kafka-topics --create \
  --topic darooghe.spending_trends \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

do it for the `commission_efficiency, commission_profitability, optimal_commission_types, temporal_patterns, peak_transaction_times, customer_segments, merchant_comparison, time_of_day_analysis, spending_trends`



#### 2. Run Commission Analysis:
```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe.transactions" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
  src/batch_processing/commission_analysis.py
```


#### 3. Run Transaction Patterns:
```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe.transactions" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
    src/batch_processing/transaction_patterns.py
```


#### 4. View Results:

- MongoBD Collections
```bash
mongosh darooghe
show collections
db.peak_transaction_times.count()
db.commission_reports.findOne()
db.peak_transaction_times.find().limit(5).pretty()
db.time_patterns.find().sort({day_of_week:1, hour:1}).limit(5)
```
- Local JSON Files
```bash
ls -l src/batch_processing/results/
cat src/batch_processing/results/optimal_commissions.json/part-*
```  

#### 5. Load the data from `valid_transactions` to mongoDB:
```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  src/batch_processing/data_loader.py
```

#### 6. Run Historical Aggregation:
```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe.transactions" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
    src/batch_processing/historical_aggregation.py
```

#### 5. Stop MongoBD:
```bash
# In the terminal running mongod
Ctrl+C
# Or from another terminal:
pkill -f mongod
```

---

### **Part 4 (Real Time Processing):**

#### 1. Spark streaming app:

```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1" \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe.aggregated_transactions_customer_profiles" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
  src/realtime_processing/streaming_app.py
```


#### 2. See the insights topic in kafka:
Run this in terminal
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic darooghe.insights --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic darooghe.fraud_alerts --from-beginning
```

---

### **Part 5 (Visualization):**

#### 1. Spark streaming app:

```bash
python src/realtime_processing/1_1_transaction_volume_realtime.py
python src/visualization/1_2_transaction_volume_historical.py
python src/visualization/2_merchant_analysis.py
python src/visualization/3_user_activity.py
```


---

### **Part 6 (bonus):**

#### 1. Temporal Analysis:

```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1" \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
src/temporal_analysis/merchant_activity_analysis.py
```
```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  --packages "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1" \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
  src/temporal_analysis/1_merchant_business_hours_analysis.py
```