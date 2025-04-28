
### **Part1 (Set up):**

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

#### 6. Test the connection:
```bash
nc -zv localhost 9092
```
Should return "succeeded" message.



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

### **Part2 (Data Ingestion):**


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
mongod --dbpath ~/data/db --logpath ~/data/db/mongod.log --logappend
```

#### 2. Initialize Collections:
```bash
mongosh
use darooghe
db.createCollection("transactions")
exit
```

#### 3. Run Commission Analysis:
```bash
spark-submit \
  --conf "spark.driver.extraJavaOptions=-Djava.security.manager=allow" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.manager=allow" \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe.transactions" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
  src/batch_processing/commission_analysis.py
```

#### 4. Run Transaction Patterns:
```bash
spark-submit \
  --conf "spark.driver.extraJavaOptions=-Djava.security.manager=allow" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.manager=allow" \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  --conf "spark.mongodb.input.uri=mongodb://localhost:27017/darooghe.transactions" \
  --conf "spark.mongodb.output.uri=mongodb://localhost:27017/darooghe" \
    src/batch_processing/transaction_patterns.py
```

#### 5. View Results:

- MongoBD Collections
```bash
mongosh darooghe
show collections
db.commssion_reports.count()
db.commission_reports.findOne()
db.commission_reports.find().limit(5).pretty()
db.time_patterns.find().sort({day_of_week:1, hour:1}).limit(5)
```
- Local JSON Files
```bash
ls -l src/batch_processing/results/
cat src/batch_processing/results/optimal_commissions.json/part-*
```  


#### 6. Stop MongoBD:
```bash
# In the terminal running mongod
Ctrl+C
# Or from another terminal:
pkill -f mongod
```


