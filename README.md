
### Step-by-Step Solution:

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

#### 7. Run your producer:
```bash
export KAFKA_BROKER=localhost:9092
python data_generators/darooghe_pulse.py
```

#### 8. Run your consumer in another terminal:
```bash
python src/data_ingestion/consumer.py
```


Here's how to **completely stop Kafka and reset all data** to start fresh:

### 1. Stop Kafka Process
In the terminal where Kafka is running, press:
```bash
Ctrl + C
```

### 2. Kill Any Remaining Processes
```bash
pkill -f kafka
pkill -f java  # Kafka runs as a Java process
```

### 3. Delete All Kafka Data
```bash
rm -rf /tmp/kraft-combined-logs/
```
*This removes all Kafka message data and offsets.*

### 4. Verify Clean State
```bash
ps aux | grep kafka  # Should show no Kafka processes
ls /tmp/kraft-combined-logs  # Should show "No such file or directory"
```

### 5. When Resting Fresh:
1. **Generate new cluster ID**:
   ```bash
   KAFKA_CLUSTER_ID=$(/opt/homebrew/bin/kafka-storage random-uuid)
   echo $KAFKA_CLUSTER_ID
   ```

2. **Format storage**:
   ```bash
   /opt/homebrew/bin/kafka-storage format -t $KAFKA_CLUSTER_ID -c /opt/homebrew/etc/kafka/kraft/server.properties
   ```

3. **Start fresh**:
   ```bash
   /opt/homebrew/bin/kafka-server-start /opt/homebrew/etc/kafka/kraft/server.properties
   ```

### 6. Recreate Your Topics
In a new terminal:
```bash
/opt/homebrew/bin/kafka-topics --create \
  --topic darooghe.transactions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### Key Notes:
- This gives you a **completely clean slate**
- All previous messages will be deleted
- You'll need to recreate all topics
- Your producer will generate fresh data from scratch