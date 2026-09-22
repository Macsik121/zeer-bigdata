#!/bin/bash
# Zeer Big Data - Post-ADCM Verification
# Run on MASTER node after ADCM cluster deployment

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $*${NC}"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $*${NC}"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*${NC}"; }

log "=== Post-ADCM Verification ==="

# 1. HDFS Verification
log "1. Checking HDFS..."
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|Under replicated blocks|Missing blocks"

if hdfs dfsadmin -report | grep -q "Live datanodes (3)"; then
    log "✓ HDFS: 3 Live DataNodes"
else
    warn "HDFS: DataNode count not 3"
fi

# Create base directories
log "Creating HDFS base directories..."
hdfs dfs -mkdir -p /zeer/logs /zeer/analytics /zeer/models /spark-logs /user/hive/warehouse /app-logs
hdfs dfs -chmod -R 777 /zeer /spark-logs /user/hive/warehouse /app-logs
hdfs dfs -ls /zeer

# 2. YARN Verification
log "2. Checking YARN..."
yarn node -list -all
yarn node -list | grep -E "Total|RUNNING|UNHEALTHY"

RUNNING_NODES=$(yarn node -list 2>/dev/null | grep -c "RUNNING" || echo "0")
if [[ "$RUNNING_NODES" -eq 3 ]]; then
    log "✓ YARN: 3 RUNNING NodeManagers"
else
    warn "YARN: Only $RUNNING_NODES NodeManagers RUNNING"
fi

# 3. ZooKeeper Verification
log "3. Checking ZooKeeper..."
for host in zeer-worker1 zeer-worker2 zeer-worker3; do
    STATUS=$(ssh hadoop@$host "zkServer.sh status 2>/dev/null | grep Mode" || echo "UNKNOWN")
    log "  $host: $STATUS"
done

# 4. Kafka Verification
log "4. Checking Kafka..."
for host in zeer-worker1 zeer-worker2 zeer-worker3; do
    if ssh hadoop@$host "jps | grep -q Kafka"; then
        log "  ✓ $host: Kafka process running"
    else
        warn "  ✗ $host: Kafka NOT running"
    fi
done

# Check topics
log "Checking Kafka topics..."
kafka-topics.sh --bootstrap-server zeer-worker1:9092 --list

# 5. Create Kafka topics if not exist
log "Ensuring Kafka topics exist..."
TOPICS=("zeer-user-events:6:3" "zeer-crash-events:6:3" "zeer-injection-events:6:3")

for topic_spec in "${TOPICS[@]}"; do
    IFS=':' read -r TOPIC PARTITIONS REPLICATION <<< "$topic_spec"
    if kafka-topics.sh --bootstrap-server zeer-worker1:9092 --list | grep -q "^${TOPIC}$"; then
        log "  Topic $TOPIC already exists"
    else
        log "  Creating topic $TOPIC..."
        kafka-topics.sh --create --topic "$TOPIC" \
            --bootstrap-server zeer-worker1:9092 \
            --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
    fi
done

kafka-topics.sh --bootstrap-server zeer-worker1:9092 --list

# 6. Hive Verification
log "5. Checking Hive..."
if ssh hadoop@zeer-master "netstat -tlnp 2>/dev/null | grep -q ':9083'"; then
    log "  ✓ Hive Metastore port 9083 listening"
else
    warn "  ✗ Hive Metastore not listening on 9083"
fi

if ssh hadoop@zeer-master "netstat -tlnp 2>/dev/null | grep -q ':10000'"; then
    log "  ✓ HiveServer2 port 10000 listening"
else
    warn "  ✗ HiveServer2 not listening on 10000"
fi

# 7. Spark Verification
log "6. Checking Spark..."
if ssh hadoop@zeer-master "netstat -tlnp 2>/dev/null | grep -q ':18080'"; then
    log "  ✓ Spark History Server UI on 18080"
else
    warn "  ✗ Spark History Server not on 18080"
fi

# 8. Flume Verification
log "7. Checking Flume..."
for host in zeer-worker1 zeer-worker2; do
    if ssh hadoop@$host "pgrep -f flume-ng >/dev/null"; then
        log "  ✓ $host: Flume agent running"
    else
        warn "  ✗ $host: Flume agent NOT running"
    fi
done

# 9. Test Kafka Producer/Consumer
log "8. Testing Kafka producer/consumer..."
echo "test-message-$(date +%s)" | kafka-console-producer.sh --bootstrap-server zeer-worker1:9092 --topic zeer-user-events

timeout 10 kafka-console-consumer.sh --bootstrap-server zeer-worker1:9092 \
    --topic zeer-user-events --from-beginning --max-messages 1 || true

# 10. Test HDFS write/read
log "9. Testing HDFS write/read..."
echo "hdfs-test-$(date +%s)" | hdfs dfs -put - /zeer/test-write.txt
hdfs dfs -cat /zeer/test-write.txt
hdfs dfs -rm /zeer/test-write.txt
log "  ✓ HDFS read/write working"

# 11. Spark submit test
log "10. Testing Spark submit (Pi estimation)..."
spark-submit --master yarn \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_*.jar 10 2>&1 | tail -20

log "=== Verification Complete ==="
log "Check output above for any warnings"