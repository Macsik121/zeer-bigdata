# ADCM Service Configuration for Zeer Big Data Cluster

## Service Addition Order
1. ZooKeeper
2. HDFS
3. YARN
4. Spark
5. Kafka
6. Hive
7. Flume

---

## 1. ZooKeeper Configuration
**Nodes**: zeer-worker1, zeer-worker2, zeer-worker3 (all 3)

| Parameter | Value |
|-----------|-------|
| zookeeper.tickTime | 2000 |
| zookeeper.initLimit | 10 |
| zookeeper.syncLimit | 5 |
| zookeeper.dataDir | /opt/zookeeper/data |
| zookeeper.clientPort | 2181 |
| zookeeper.servers | zeer-worker1:2888:3888;zeer-worker2:2888:3888;zeer-worker3:2888:3888 |

**myid files** (set manually on each node after install):
- zeer-worker1: `echo "1" > /opt/zookeeper/data/myid`
- zeer-worker2: `echo "2" > /opt/zookeeper/data/myid`
- zeer-worker3: `echo "3" > /opt/zookeeper/data/myid`

---

## 2. HDFS Configuration
**NameNode**: zeer-master
**DataNodes**: zeer-worker1, zeer-worker2, zeer-worker3
**SecondaryNameNode**: zeer-worker3

| Parameter | Value | Description |
|-----------|-------|-------------|
| dfs.replication | 2 | 2 replicas for 3 DataNodes |
| dfs.blocksize | 134217728 | 128MB blocks |
| dfs.namenode.name.dir | file:///opt/hadoop/hdfs/namenode | |
| dfs.datanode.data.dir | file:///opt/hadoop/hdfs/datanode | |
| dfs.namenode.checkpoint.dir | file:///opt/hadoop/hdfs/namesecondary | |
| dfs.namenode.handler.count | 20 | |
| dfs.datanode.handler.count | 20 | |
| dfs.permissions.enabled | false | For home lab |
| dfs.webhdfs.enabled | true | |

---

## 3. YARN Configuration
**ResourceManager**: zeer-master
**NodeManagers**: zeer-worker1, zeer-worker2, zeer-worker3

| Parameter | Value | Description |
|-----------|-------|-------------|
| yarn.resourcemanager.hostname | zeer-master | |
| yarn.nodemanager.resource.memory-mb | 12288 | 12GB of 16GB |
| yarn.nodemanager.resource.cpu-vcores | 6 | 6 of 8 threads |
| yarn.scheduler.minimum-allocation-mb | 512 | |
| yarn.scheduler.maximum-allocation-mb | 12288 | |
| yarn.scheduler.minimum-allocation-vcores | 1 | |
| yarn.scheduler.maximum-allocation-vcores | 6 | |
| yarn.nodemanager.vmem-check-enabled | false | Disable virtual memory check |
| yarn.nodemanager.pmem-check-enabled | false | Disable physical memory check |
| yarn.nodemanager.aux-services | mapreduce_shuffle,spark_shuffle | |
| yarn.log-aggregation-enable | true | |
| yarn.log-aggregation.retain-seconds | 604800 | 7 days |
| yarn.nodemanager.remote-app-log-dir | /app-logs | HDFS path |

**Container Resources** (per node):
- Total: 12GB RAM, 6 vcores
- Max container: 12GB, 6 vcores
- Min container: 512MB, 1 vcore

---

## 4. Spark Configuration
**History Server**: zeer-master
**Clients**: All nodes

| Parameter | Value | Description |
|-----------|-------|-------------|
| spark.master | yarn | Run on YARN |
| spark.deploy.mode | cluster | Cluster mode |
| spark.executor.memory | 6g | Per executor |
| spark.executor.cores | 3 | Per executor |
| spark.executor.instances | 6 | 2 per worker × 3 workers |
| spark.driver.memory | 2g | |
| spark.default.parallelism | 18 | 3 executors × 6 cores |
| spark.sql.shuffle.partitions | 18 | |
| spark.eventLog.enabled | true | |
| spark.eventLog.dir | hdfs://zeer-master:8020/spark-logs | |
| spark.history.fs.logDirectory | hdfs://zeer-master:8020/spark-logs | |
| spark.yarn.archive | hdfs://zeer-master:8020/spark-jars/spark-libs.jar | |
| spark.serializer | org.apache.spark.serializer.KryoSerializer | |

**Executor Distribution**:
- Worker1: 2 executors (6 cores, 12GB)
- Worker2: 2 executors (6 cores, 12GB)
- Worker3: 2 executors (6 cores, 12GB)
- Total: 6 executors, 18 cores, 36GB

---

## 5. Kafka Configuration
**Brokers**: zeer-worker1, zeer-worker2, zeer-worker3 (all 3)

| Parameter | Value | Description |
|-----------|-------|-------------|
| broker.id | 1,2,3 | Unique per broker |
| listeners | PLAINTEXT://0.0.0.0:9092 | |
| advertised.listeners | PLAINTEXT://zeer-workerX:9092 | Per broker |
| log.dirs | /opt/kafka/kafka-logs | |
| num.partitions | 6 | 2 per broker |
| default.replication.factor | 3 | Full replication |
| min.insync.replicas | 2 | |
| zookeeper.connect | zeer-worker1:2181,zeer-worker2:2181,zeer-worker3:2181 | |
| log.retention.hours | 168 | 7 days |
| log.segment.bytes | 1073741824 | 1GB segments |
| log.retention.check.interval.ms | 300000 | 5 min |
| num.network.threads | 8 | |
| num.io.threads | 16 | |
| socket.send.buffer.bytes | 102400 | |
| socket.receive.buffer.bytes | 102400 | |
| socket.request.max.bytes | 104857600 | 100MB |
| compression.type | snappy | |

**Topics to Create** (after Kafka start):

| Topic | Partitions | Replication | Purpose |
|-------|------------|-------------|---------|
| zeer-user-events | 6 | 3 | User action logs |
| zeer-crash-events | 6 | 3 | Crash logs from game client |
| zeer-injection-events | 6 | 3 | Injection/security logs |

---

## 6. Hive Configuration
**Metastore**: zeer-master
**HiveServer2**: zeer-master
**Database**: PostgreSQL (external or ADCM-managed)

| Parameter | Value |
|-----------|-------|
| hive.metastore.uris | thrift://zeer-master:9083 |
| hive.server2.thrift.port | 10000 |
| hive.server2.thrift.http.port | 10001 |
| hive.execution.engine | spark |
| hive.metastore.warehouse.dir | hdfs://zeer-master:8020/user/hive/warehouse |
| hive.metastore.schema.verification | false |
| javax.jdo.option.ConnectionURL | jdbc:postgresql://zeer-master:5432/hive |
| javax.jdo.option.ConnectionDriverName | org.postgresql.Driver |
| hive.server2.enable.doAs | false |

---

## 7. Flume Configuration
**Agents**: zeer-worker1, zeer-worker2 (2 agents for HA)

**Agent Name**: `zeer-kafka-hdfs-agent`

**Source** (Kafka):
```
agent.sources = kafka-source
agent.sources.kafka-source.type = org.apache.flume.source.kafka.KafkaSource
agent.sources.kafka-source.kafka.bootstrap.servers = zeer-worker1:9092,zeer-worker2:9092,zeer-worker3:9092
agent.sources.kafka-source.kafka.topics = zeer-user-events,zeer-crash-events,zeer-injection-events
agent.sources.kafka-source.kafka.consumer.group.id = flume-consumer
agent.sources.kafka-source.channels = memory-channel
agent.sources.kafka-source.interceptors = timestamp-interceptor
agent.sources.kafka-source.interceptors.timestamp-interceptor.type = timestamp
```

**Channel** (Memory):
```
agent.channels = memory-channel
agent.channels.memory-channel.type = memory
agent.channels.memory-channel.capacity = 100000
agent.channels.memory-channel.transactionCapacity = 10000
agent.channels.memory-channel.keep-alive = 30
```

**Sink** (HDFS):
```
agent.sinks = hdfs-sink
agent.sinks.hdfs-sink.type = hdfs
agent.sinks.hdfs-sink.hdfs.path = hdfs://zeer-master:8020/zeer/logs/%{topic}/%Y/%m/%d
agent.sinks.hdfs-sink.hdfs.filePrefix = events
agent.sinks.hdfs-sink.hdfs.fileSuffix = .log
agent.sinks.hdfs-sink.hdfs.rollInterval = 600
agent.sinks.hdfs-sink.hdfs.rollSize = 134217728
agent.sinks.hdfs-sink.hdfs.rollCount = 0
agent.sinks.hdfs-sink.hdfs.fileType = DataStream
agent.sinks.hdfs-sink.hdfs.writeFormat = Text
agent.sinks.hdfs-sink.hdfs.batchSize = 1000
agent.sinks.hdfs-sink.hdfs.idleTimeout = 0
agent.sinks.hdfs-sink.hdfs.callTimeout = 30000
agent.sinks.hdfs-sink.channel = memory-channel
```

**HDFS Directory Structure Created**:
```
/zeer/logs/
├── zeer-user-events/
│   ├── 2026/
│   │   ├── 09/
│   │   │   ├── 21/
│   │   │   │   ├── events.12345.log
│   │   │   │   └── ...
├── zeer-crash-events/
│   └── ...
└── zeer-injection-events/
    └── ...
```

---

## Deployment Checklist

- [ ] ZooKeeper: 3 nodes, myid set, ensemble formed (1 leader, 2 followers)
- [ ] HDFS: NameNode formatted, 3 DataNodes live, replication=2
- [ ] YARN: RM active, 3 NMs RUNNING
- [ ] Spark: History Server UI accessible on zeer-master:18080
- [ ] Kafka: 3 brokers, 3 topics created with 6 partitions, RF=3
- [ ] Hive: Metastore initialized, HiveServer2 running
- [ ] Flume: 2 agents running, consuming from Kafka, writing to HDFS
- [ ] All services GREEN in ADCM UI