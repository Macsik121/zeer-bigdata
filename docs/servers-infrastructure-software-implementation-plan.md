Zeer Marketplace Big Data Infrastructure - Software Setup Plan

Context

This plan details the complete software setup for a 4-node Big Data cluster (1 Master/NameNode + 3 Worker/DataNodes) running Ubuntu Server 22.04, implementing the ArenaData Cluster Manager (ADCM) and ArenaData Hyperwave (ADH) integration with the full Hadoop ecosystem for processing Zeer Marketplace logs (Action Logs, Crash Logs, Inject Logs).

Architecture Overview

Master Node (Laptop - 192.168.1.5):     Worker Nodes (3 PCs - 192.168.1.10-12):
├── NameNode                            ├── DataNode (×3)
├── ResourceManager                     ├── NodeManager (×3)
├── Hive Metastore                      ├── Spark Worker (×3)
├── Spark History Server                ├── Kafka Broker (×3)
├── Node.js API Collector               ├── ZooKeeper (×3)
├── ADCM Server                         ├── Flume Agent (×2-3)
├── Prometheus/Grafana                  └── ADH Agent
└── ADH Manager

Phase 1: Base OS & Network Setup (All 4 Nodes)

1.1 Ubuntu Server 22.04 Installation

- Install Ubuntu Server 22.04 LTS on all 4 machines
- Create user hadoop with sudo privileges on all nodes
- Set static IPs:
  - Master: 192.168.1.5 (zeer-master)
  - Worker1: 192.168.1.10 (zeer-worker1)
  - Worker2: 192.168.1.11 (zeer-worker2)
  - Worker3: 192.168.1.12 (zeer-worker3)

1.2 /etc/hosts Configuration (All Nodes)

192.168.1.5   zeer-master
192.168.1.10  zeer-worker1
192.168.1.11  zeer-worker2
192.168.1.12  zeer-worker3

1.3 SSH Passwordless Setup

# On all nodes
sudo apt update && sudo apt install -y openssh-server
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# On Master only - copy to all nodes
ssh-copy-id hadoop@zeer-master
ssh-copy-id hadoop@zeer-worker1
ssh-copy-id hadoop@zeer-worker2
ssh-copy-id hadoop@zeer-worker3

# Verify
ssh zeer-worker1 hostname

1.4 Base Packages & Java

sudo apt update && sudo apt upgrade -y
sudo apt install -y vim net-tools curl wget git rsync python3 python3-pip \
    build-essential openjdk-11-jdk ntp chrony
# Verify Java
java -version  # Should be 11.x

1.5 Time Synchronization

sudo systemctl enable chrony
sudo systemctl start chrony
# On Master: edit /etc/chrony/chrony.conf to allow LAN clients
# On Workers: set server to zeer-master

1.6 Firewall & Kernel Tuning

sudo ufw disable  # For home lab; or configure specific ports
# Kernel params for Hadoop
echo 'vm.swappiness=1' | sudo tee -a /etc/sysctl.conf
echo 'net.core.somaxconn=65535' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p

Phase 2: ArenaData Cluster Manager (ADCM) Installation (Master Only)

2.1 ADCM Prerequisites

# Docker & Docker Compose
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker hadoop
newgrp docker

2.2 ADCM Deployment

# Create ADCM directory
mkdir -p /opt/adcm && cd /opt/adcm

# Download ADCM docker-compose
wget https://github.com/arenadata/adcm/releases/latest/download/docker-compose.yml

# Configure ADCM
# Edit docker-compose.yml:
# - Set ADCM_VERSION=latest
# - Map ports: 8000:80 (UI), 9443:443 (API)
# - Set volumes for persistence

# Start ADCM
docker-compose up -d
# Access UI at http://192.168.1.5:8000 (admin/admin)

2.3 ADCM License & Initial Config

- Upload license in ADCM UI
- Configure cluster name: zeer-bigdata-cluster
- Add SSH credentials for all nodes

Phase 3: ArenaData Hyperwave (ADH) Installation via ADCM

3.1 Add Hosts to ADCM

1. ADCM UI → Hosts → Add Host
2. Add all 4 hosts with FQDN/IP and SSH credentials
3. Run "Host Check" action on each

3.2 Create Cluster & Add Services

1. Clusters → Add Cluster → Name: zeer-cluster
2. Services → Add Services in order:
   - ZooKeeper (3 nodes: worker1, worker2, worker3)
   - HDFS (NameNode: master; DataNodes: worker1,2,3)
   - YARN (ResourceManager: master; NodeManagers: worker1,2,3)
   - Spark (History Server: master; Clients: all)
   - Kafka (Brokers: worker1,2,3)
   - Hive (Metastore: master; HiveServer2: master)
   - Flume (Agents: worker1, worker2)

3.3 Configure Service Parameters via ADCM

HDFS:
- dfs.replication = 2
- dfs.blocksize = 134217728 (128MB)
- dfs.namenode.name.dir = /opt/hadoop/hdfs/namenode
- dfs.datanode.data.dir = /opt/hadoop/hdfs/datanode

YARN:
- yarn.nodemanager.resource.memory-mb = 12288 (12GB of 16GB)
- yarn.nodemanager.resource.cpu-vcores = 6
- yarn.scheduler.maximum-allocation-mb = 12288

Kafka:
- num.partitions = 6
- default.replication.factor = 3
- min.insync.replicas = 2
- log.retention.hours = 168

Spark:
- spark.executor.memory = 6g
- spark.executor.cores = 3
- spark.default.parallelism = 18

3.4 Deploy Cluster via ADCM

1. Run "Install" action on cluster
2. Run "Start" action on cluster
3. Verify all services green in ADCM UI

Phase 4: Manual Verification & Configuration (Post-ADCM)

4.1 HDFS Verification

# On Master
hdfs dfsadmin -report
# Should show 3 Live DataNodes

# Create base directories
hdfs dfs -mkdir -p /zeer/logs /zeer/analytics /zeer/models /spark-logs
hdfs dfs -chmod -R 777 /zeer

4.2 YARN Verification

yarn node -list
# Should show 3 RUNNING NodeManagers

4.3 ZooKeeper Verification

# On each worker
zkServer.sh status
# One leader, two followers

4.4 Kafka Setup - Create Topics

# On any worker
kafka-topics.sh --create --topic zeer-user-events \
    --bootstrap-server zeer-worker1:9092 \
    --partitions 6 --replication-factor 3

kafka-topics.sh --create --topic zeer-crash-events \
    --bootstrap-server zeer-worker1:9092 \
    --partitions 6 --replication-factor 3

kafka-topics.sh --create --topic zeer-injection-events \
    --bootstrap-server zeer-worker1:9092 \
    --partitions 6 --replication-factor 3

kafka-topics.sh --list --bootstrap-server zeer-worker1:9092

4.5 Hive Metastore Initialization

# On Master
schematool -dbType postgres -initSchema
# Or use ADCM's Hive service initialization

4.6 Flume Configuration (Worker1 & Worker2)

Create /opt/flume/conf/flume-kafka-hdfs.conf:
agent1.sources = kafka-source
agent1.channels = memory-channel
agent1.sinks = hdfs-sink

agent1.sources.kafka-source.type = org.apache.flume.source.kafka.KafkaSource
agent1.sources.kafka-source.kafka.bootstrap.servers = zeer-worker1:9092,zeer-worker2:9092,zeer-worker3:9092
agent1.sources.kafka-source.kafka.topics = zeer-user-events,zeer-crash-events,zeer-injection-events
agent1.sources.kafka-source.kafka.consumer.group.id = flume-consumer
agent1.sources.kafka-source.channels = memory-channel

agent1.channels.memory-channel.type = memory
agent1.channels.memory-channel.capacity = 10000

agent1.sinks.hdfs-sink.type = hdfs
agent1.sinks.hdfs-sink.hdfs.path = hdfs://zeer-master:8020/zeer/logs/%{topic}/%Y/%m/%d
agent1.sinks.hdfs-sink.hdfs.filePrefix = events
agent1.sinks.hdfs-sink.hdfs.fileSuffix = .log
agent1.sinks.hdfs-sink.hdfs.rollInterval = 600
agent1.sinks.hdfs-sink.hdfs.rollSize = 134217728
agent1.sinks.hdfs-sink.hdfs.fileType = DataStream
agent1.sinks.hdfs-sink.channel = memory-channel

Phase 5: Node.js API Collector Setup (Master)

5.1 Node.js Environment

# Install Node.js 20 LTS
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Verify
node --version  # v20.x
npm --version

5.2 Project Setup

mkdir -p /opt/zeer-api && cd /opt/zeer-api
npm init -y

npm install express kafkajs axios winston dotenv cors helmet
npm install -D typescript @types/node @types/express ts-node nodemon

5.3 TypeScript Config

// tsconfig.json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true
  },
  "include": ["src/**/*"]
}

5.4 Environment Configuration

env
# .env
KAFKA_BROKERS=zeer-worker1:9092,zeer-worker2:9092,zeer-worker3:9092
KAFKA_CLIENT_ID=zeer-api-collector
KAFKA_TOPICS_USER=zeer-user-events
KAFKA_TOPICS_CRASH=zeer-crash-events
KAFKA_TOPICS_INJECTION=zeer-injection-events

HDFS_NAMENODE=zeer-master
HDFS_PORT=8020
HDFS_USER=hadoop

SPARK_MASTER=yarn
SPARK_DEPLOY_MODE=cluster

YARN_RM_HOST=zeer-master
YARN_RM_PORT=8088

PORT=3000
NODE_ENV=production

5.5 Kafka Producer Module

// src/kafka/producer.ts
import { Kafka, Producer, ProducerRecord } from 'kafkajs';
import { createLogger } from '../utils/logger';

const logger = createLogger('KafkaProducer');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID!,
  brokers: process.env.KAFKA_BROKERS!.split(','),
  retry: { initialRetryTime: 100, retries: 8 }
});

const producer = kafka.producer();

export async function connectProducer(): Promise<void> {
  await producer.connect();
  logger.info('Kafka producer connected');
}

export async function sendLog(topic: string, logData: any): Promise<void> {
  await producer.send({
    topic,
    messages: [{
      key: logData.userId || 'anonymous',
      value: JSON.stringify(logData),
      timestamp: Date.now().toString()
    }]
  });
}

export const sendUserAction = (data: any) => sendLog(process.env.KAFKA_TOPICS_USER!, data);
export const sendCrashLog = (data: any) => sendLog(process.env.KAFKA_TOPICS_CRASH!, data);
export const sendInjectionLog = (data: any) => sendLog(process.env.KAFKA_TOPICS_INJECTION!, data);

export async function disconnectProducer(): Promise<void> {
  await producer.disconnect();
}

5.6 Express API Server

// src/app.ts
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { sendUserAction, sendCrashLog, sendInjectionLog, connectProducer, disconnectProducer } from './kafka/producer';
import { submitSparkJob } from './spark/jobRunner';
import { getClusterMetrics, listApplications } from './yarn/yarnClient';

const app = express();
app.use(helmet());
app.use(cors());
app.use(express.json());

// Health check
app.get('/health', (_, res) => res.json({ status: 'ok' }));

// Log ingestion endpoints
app.post('/api/logs/user-action', async (req, res) => {
  try {
    await sendUserAction({ ...req.body, timestamp: new Date().toISOString() });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/logs/crash', async (req, res) => {
  try {
    await sendCrashLog({ ...req.body, timestamp: new Date().toISOString() });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/logs/injection', async (req, res) => {
  try {
    await sendInjectionLog({ ...req.body, timestamp: new Date().toISOString() });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Analytics endpoints
app.post('/api/analytics/user-actions', async (req, res) => {
  try {
    const result = await submitSparkJob({
      className: 'com.zeer.analytics.UserActionsAnalyzer',
      jarPath: '/opt/zeer/jars/zeer-analytics.jar',
      args: [`--date=${req.body.date || new Date().toISOString().split('T')[0]}`, '--output=/zeer/analytics/user-actions']
    });
    res.json({ success: true, applicationId: result.applicationId });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Monitoring endpoints
app.get('/api/cluster/metrics', async (_, res) => {
  try {
    const metrics = await getClusterMetrics();
    res.json(metrics);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/cluster/applications', async (_, res) => {
  try {
    const apps = await listApplications('RUNNING');
    res.json(apps);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

const PORT = process.env.PORT || 3000;

async function start() {
  await connectProducer();
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Zeer Big Data API running on port ${PORT}`);
  });
}

process.on('SIGTERM', async () => {
  await disconnectProducer();
  process.exit(0);
});

start();

5.7 Spark Job Runner

// src/spark/jobRunner.ts
import { exec } from 'child_process';
import { promisify } from 'util';
import { createLogger } from '../utils/logger';

const logger = createLogger('SparkJobRunner');
const execPromise = promisify(exec);

export async function submitSparkJob(config: {
  className: string;
  jarPath: string;
  args?: string[];
  executorMemory?: string;
  driverMemory?: string;
  executorCores?: number;
}): Promise<{ success: boolean; applicationId?: string; output: string }> {
  const {
    className, jarPath, args = [],
    executorMemory = '6g', driverMemory = '2g', executorCores = 3
  } = config;

  const cmd = `
    spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${driverMemory} \
    --executor-memory ${executorMemory} \
    --executor-cores ${executorCores} \
    --class ${className} \
    ${jarPath} \
    ${args.join(' ')}
  `.replace(/\s+/g, ' ').trim();

  try {
    const { stdout } = await execPromise(cmd);
    const appIdMatch = stdout.match(/application_\d+_\d+/);
    return { success: true, applicationId: appIdMatch?.[0], output: stdout };
  } catch (error) {
    logger.error('Spark job failed', error);
    throw error;
  }
}

5.8 YARN REST Client

// src/yarn/yarnClient.ts
import axios from 'axios';
import { createLogger } from '../utils/logger';

const logger = createLogger('YarnClient');
const YARN_URL = `http://${process.env.YARN_RM_HOST}:${process.env.YARN_RM_PORT}/ws/v1/cluster`;

export async function getClusterMetrics() {
  const { data } = await axios.get(`${YARN_URL}/metrics`);
  return data.clusterMetrics;
}

export async function listApplications(state = 'RUNNING') {
  const { data } = await axios.get(`${YARN_URL}/apps`, { params: { state } });
  return data.apps?.app || [];
}

5.9 Systemd Service for Node.js API

# /etc/systemd/system/zeer-api.service
[Unit]
Description=Zeer Big Data API Collector
After=network.target

[Service]
Type=simple
User=hadoop
WorkingDirectory=/opt/zeer-api
ExecStart=/usr/bin/node dist/app.js
Restart=on-failure
RestartSec=10
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target

sudo systemctl daemon-reload
sudo systemctl enable zeer-api
sudo systemctl start zeer-api

Phase 6: Spark Analytics Jobs Development

6.1 Project Structure

zeer-analytics/
├── build.sbt
├── src/
│   └── main/
│       └── scala/
│           └── com/zeer/analytics/
│               ├── UserActionsAnalyzer.scala
│               ├── CrashAnalyzer.scala
│               └── InjectionAnalyzer.scala

6.2 build.sbt

name := "zeer-analytics"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

6.3 UserActionsAnalyzer (Batch)

// UserActionsAnalyzer.scala
package com.zeer.analytics

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UserActionsAnalyzer {
  val schema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = false),
    StructField("userId", StringType, nullable = false),
    StructField("event", StringType, nullable = false),
    StructField("productId", StringType, nullable = true),
    StructField("category", StringType, nullable = true),
    StructField("sessionId", StringType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Zeer User Actions Analyzer")
      .enableHiveSupport()
      .getOrCreate()

    val date = args.lift(0).getOrElse(java.time.LocalDate.now.toString)
    val inputPath = s"hdfs://zeer-master:8020/zeer/logs/zeer-user-events/$date"
    val outputPath = s"hdfs://zeer-master:8020/zeer/analytics/user-actions/$date"

    val logs = spark.read.schema(schema).json(inputPath)

    // Actions by category
    logs.groupBy("category", "event")
      .count()
      .write.mode(SaveMode.Overwrite).parquet(s"$outputPath/actions_by_category")

    // User activity
    logs.groupBy("userId")
      .agg(count("*").as("total_actions"),
        countDistinct("sessionId").as("sessions"),
        countDistinct("productId").as("products_viewed"))
      .write.mode(SaveMode.Overwrite).parquet(s"$outputPath/user_activity")

    // Hourly activity
    logs.withColumn("hour", hour(col("timestamp")))
      .groupBy("hour", "event")
      .count()
      .write.mode(SaveMode.Overwrite).parquet(s"$outputPath/hourly_activity")

    spark.stop()
  }
}

6.4 Build & Deploy

cd zeer-analytics
sbt clean assembly
scp target/scala-2.12/zeer-analytics-assembly-1.0.jar hadoop@zeer-master:/opt/zeer/jars/

Phase 7: Monitoring Setup (Prometheus + Grafana)

7.1 Prometheus (Master)

# /opt/prometheus/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'hadoop-namenode'
    static_configs:
      - targets: ['zeer-master:9870']
  - job_name: 'yarn-resourcemanager'
    static_configs:
      - targets: ['zeer-master:8088']
  - job_name: 'spark-master'
    static_configs:
      - targets: ['zeer-master:8080']
  - job_name: 'kafka-brokers'
    static_configs:
      - targets: ['zeer-worker1:9092', 'zeer-worker2:9092', 'zeer-worker3:9092']
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['zeer-master:9100', 'zeer-worker1:9100', 'zeer-worker2:9100', 'zeer-worker3:9100']

7.2 Node Exporter (All Nodes)

# On all nodes
wget https://github.com/prometheus/node_exporter/releases/download/v1.7.0/node_exporter-1.7.0.linux-amd64.tar.gz
tar xzf node_exporter-*.tar.gz
sudo cp node_exporter-*/node_exporter /usr/local/bin/
sudo useradd --no-create-home --shell /bin/false node_exporter
sudo systemctl enable --now node_exporter

7.3 Grafana (Master)

docker run -d --name grafana -p 3001:3000 \
  -v /opt/grafana:/var/lib/grafana \
  grafana/grafana:latest
# Access at http://192.168.1.5:3001 (admin/admin)
# Add Prometheus datasource: http://zeer-master:9090
# Import Hadoop/Spark/Kafka dashboards

Phase 8: Validation & Testing

8.1 End-to-End Test

# 1. Send test log via API
curl -X POST http://zeer-master:3000/api/logs/user-action \
  -H "Content-Type: application/json" \
  -d '{"userId":"test1","event":"product_view","productId":"prod1","category":"electronics"}'

# 2. Verify Kafka
kafka-console-consumer.sh --bootstrap-server zeer-worker1:9092 \
  --topic zeer-user-events --from-beginning --max-messages 1

# 3. Verify HDFS (wait ~10 min for Flume)
hdfs dfs -ls /zeer/logs/zeer-user-events/

# 4. Run Spark job
spark-submit --master yarn --class com.zeer.analytics.UserActionsAnalyzer \
  /opt/zeer/jars/zeer-analytics.jar \
  hdfs://zeer-master:8020/zeer/logs/zeer-user-events/$(date +%Y/%m/%d) \
  hdfs://zeer-master:8020/zeer/analytics/user-actions/$(date +%Y/%m/%d)

# 5. Check results
hdfs dfs -ls /zeer/analytics/user-actions/$(date +%Y/%m/%d)/

8.2 Cluster Health Check Script

#!/bin/bash
# /opt/health-check.sh
echo "=== HDFS ==="
hdfs dfsadmin -report | grep -E "Live|Dead|Under replicated"
echo "=== YARN ==="
yarn node -list | grep -E "RUNNING|UNHEALTHY"
echo "=== Kafka ==="
kafka-broker-api-versions.sh --bootstrap-server zeer-worker1:9092
echo "=== ZooKeeper ==="
for h in zeer-worker1 zeer-worker2 zeer-worker3; do
  ssh $h "zkServer.sh status"
done
echo "=== Disk ==="
df -h | grep -E "Filesystem|/opt"

Phase 9: Maintenance & Operations

9.1 Cron Jobs (All Nodes)

# HDFS log cleanup (30 days)
0 2 * * * find /opt/hadoop/logs -name "*.log*" -mtime +30 -delete

# Spark cleanup (7 days)
0 3 * * * find /opt/spark/work -mtime +7 -delete

# HDFS Balancer (weekly)
0 1 * * 0 hdfs balancer -threshold 10

9.2 HDFS Metadata Backup (Master)

#!/bin/bash
# /opt/backup-hdfs-metadata.sh
BACKUP_DIR="/backup/hdfs/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR
hdfs dfsadmin -fetchImage $BACKUP_DIR/fsimage
cp /opt/hadoop/hdfs/namenode/current/edits_* $BACKUP_DIR/

Verification Checklist

- [ ] All 4 nodes accessible via SSH from Master
- [ ] ADCM UI accessible at http://192.168.1.5:8000
- [ ] All ADCM services show GREEN status
- [ ] HDFS shows 3 Live DataNodes, 0 Dead
- [ ] YARN shows 3 RUNNING NodeManagers
- [ ] ZooKeeper: 1 Leader, 2 Followers
- [ ] Kafka: 3 brokers, 3 topics created
- [ ] Hive Metastore initialized
- [ ] Flume agents running on Worker1, Worker2
- [ ] Node.js API responding on port 3000
- [ ] Test log flows: API → Kafka → Flume → HDFS → Spark → Analytics
- [ ] Prometheus scraping all targets
- [ ] Grafana dashboards showing metrics
- [ ] Health check script passes
- [ ] Cron jobs configured
- [ ] Backup script tested

Rollback Plan

If any phase fails:
1. Check ADCM UI for service logs
2. Use journalctl -u <service> on affected nodes
3. For ADCM: docker-compose down && docker-compose up -d
4. For manual services: stop/start via systemd or init scripts
5. HDFS: stop-dfs.sh && start-dfs.sh
6. YARN: stop-yarn.sh && start-yarn.sh

Notes

- This is a home lab cluster - not production-grade HA
- All passwords default to admin/admin - change in production
- No Kerberos/TLS configured - add for security hardening
- Monitor disk space - 512GB fills quickly with logs
- Consider adding 4th DataNode for better fault tolerance