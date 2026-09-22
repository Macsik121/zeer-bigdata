# Zeer Marketplace Big Data Architecture — Diploma Thesis Documentation Structure

**Version:** 1.0  
**Project:** Big Data Infrastructure for Zeer Marketplace  
**Cluster:** 1 Master (Laptop) + 3 Worker Nodes (Desktop PCs)  
**Target Audience:** Thesis Committee, Technical Reviewers, Future Maintainers

---

## 📚 COMPLETE TABLE OF CONTENTS

---

### **PART I: INTRODUCTION & REQUIREMENTS**

#### **1. Introduction**
- 1.1 Problem Statement & Motivation
  - Why Big Data for Zeer Marketplace?
  - Business value: user behavior analytics, crash detection, security monitoring
- 1.2 Project Goals & Success Criteria
  - Functional: ingest → process → store → analyze 3 log types at scale
  - Non-functional: 24/7 uptime, horizontal scalability, fault tolerance
- 1.3 Scope & Limitations
  - Home lab cluster (not production-grade HA)
  - Hardware constraints: LGA 1155, 16GB RAM, 1Gb Ethernet
- 1.4 Thesis Structure Overview

#### **2. Requirements Analysis**
- 2.1 Functional Requirements
  - FR-1: Action Log ingestion from Node.js API (REST → Kafka)
  - FR-2: Crash Log ingestion from Loader API (game client)
  - FR-3: Inject Log ingestion from Loader API (security events)
  - FR-4: Real-time crash detection (Spark Streaming)
  - FR-5: Batch analytics: user behavior, product popularity, conversion
  - FR-6: SQL access via Hive for BI/reporting
  - FR-7: Cluster monitoring & alerting
- 2.2 Non-Functional Requirements
  - NFR-1: Latency < 5 min for streaming, < 1 hr for batch
  - NFR-2: 99.5% uptime (planned maintenance windows)
  - NFR-3: Data retention: 90 days raw, 365 days aggregated
  - NFR-4: Horizontal scalability (add 4th/5th node)
  - NFR-5: Security: no plaintext secrets, network isolation
- 2.3 Data Characteristics
  - Volume estimates: Action ~10K events/min, Crash ~100/min, Inject ~50/min
  - Velocity: Continuous streaming + hourly batch
  - Variety: JSON (structured), Parquet (columnar), Avro (schema evolution)
  - Veracity: Deduplication, schema validation, anomaly detection

---

### **PART II: INFRASTRUCTURE & HARDWARE DOCUMENTATION**

#### **3. Physical Infrastructure Design**
- 3.1 Cluster Topology & Network Architecture
  - 3.1.1 Logical topology diagram (Master ↔ Workers)
  - 3.1.2 IP addressing scheme (192.168.1.0/24, static assignments)
  - 3.1.3 Hostname resolution (/etc/hosts, DNS considerations)
  - 3.1.4 Network ports matrix (HDFS, YARN, Kafka, ZooKeeper, Spark, Hive, API, Monitoring)
  - 3.1.5 Bandwidth analysis: 1Gb Ethernet sufficiency for workload
  - 3.1.6 Network validation: iperf3, ping, latency measurements
- 3.2 Hardware Specification & Selection Rationale
  - 3.2.1 Master Node (Laptop): Role justification, specs, limitations
  - 3.2.2 Worker Nodes (3× Desktop): CPU (i7-2600/3770 vs Xeon E3-1275), RAM, Storage, Network
  - 3.2.3 Component selection criteria: TDP, core count, iGPU, cost/performance
  - 3.2.4 Alternative hardware considered & rejected
  - 3.2.5 Bill of Materials (BOM) with costs
- 3.3 Physical Assembly & Cable Management
  - 3.3.1 Chassis selection, motherboard standoffs, PSU mounting
  - 3.3.2 Cable routing: ATX 24-pin, EPS 4/8-pin, SATA power/data, fan headers
  - 3.3.3 Dust filter installation, positive pressure verification
  - 3.3.4 Labeling scheme: node ID, port mapping, cable tags
- 3.4 **Physical Safety & Cooling Engineering** ← *Critical Chapter*
  - 3.4.1 Thermal envelope analysis (140-160W/node sustained)
  - 3.4.2 CPU cooler selection: Sandy Bridge (95W) vs Ivy Bridge (77W) TDP matching
  - 3.4.3 Case airflow design: positive pressure, fan curves, filter maintenance
  - 3.4.4 Multi-layer overheating protection (CPU TM1/TM2 → BIOS → OS → App watchdog)
  - 3.4.5 Electrical safety: PSU specs, surge protection, UPS, grounding, ESD prevention
  - 3.4.6 **Winter/condensation protection**: dew point analysis, pre-warm protocol, conformal coating
  - 3.4.7 Fire safety: extinguisher placement, smoke detector, cable ratings
  - 3.4.8 Vibration/shock mitigation for HDDs
  - 3.4.9 Environmental monitoring IoT layer (temp/humidity/power/airflow sensors)
  - 3.4.10 Maintenance schedule: daily/weekly/monthly/quarterly/annual tasks
  - 3.4.11 Emergency procedures: thermal runaway, power loss, water leak, fire
  - 3.4.12 Validation checklist: 4h stress test, electrical, environmental, safety
  - *Reference: `zeer-bigdata/docs/PHYSICAL_SAFETY_AND_COOLING.md`*

#### **4. Operating System & Base Configuration**
- 4.1 Ubuntu Server 22.04 LTS Installation & Hardening
  - 4.1.1 Partitioning scheme: /boot, / (root), /var/log, /opt, swap
  - 4.1.2 User management: `hadoop` user, sudo, SSH keys
  - 4.1.3 Kernel tuning: swappiness, hugepages, file descriptors, network buffers
  - 4.1.4 Time synchronization: chrony (master as NTP server for LAN)
  - 4.1.5 Security: UFW rules, fail2ban, automatic security updates
- 4.2 Docker & Container Runtime
  - 4.2.1 Docker Engine installation, daemon config (log rotation, storage driver)
  - 4.2.2 Docker Compose for ADCM, Prometheus, Grafana
  - 4.2.3 Container resource limits & health checks

---

### **PART III: BIG DATA PLATFORM ARCHITECTURE**

#### **5. Platform Architecture & Design Decisions**
- 5.1 Technology Stack Selection & Justification
  - 5.1.1 Hadoop 3.3.x vs alternatives (CDP, HDP, EMR) — why vanilla Apache
  - 5.1.2 Spark on YARN vs Standalone vs Kubernetes — resource sharing rationale
  - 5.1.3 Kafka as ingestion buffer: durability, replay, backpressure
  - 5.1.4 ZooKeeper ensemble: 3-node quorum on workers
  - 5.1.5 Hive on Spark: SQL layer for BI, metastore on PostgreSQL
  - 5.1.6 Flume vs Kafka Connect vs custom: why Flume for Kafka→HDFS
  - 5.1.7 ADCM/ADH: cluster lifecycle management, automation
- 5.2 High-Level Architecture Diagrams
  - 5.2.1 Data flow: Zeer Marketplace → Node.js API → Kafka → Flume → HDFS → Spark → Hive → BI
  - 5.2.2 Control plane: ADCM → Service deployment → Configuration management
  - 5.2.3 Monitoring plane: Node Exporters → Prometheus → Grafana → Alerting
  - 5.2.4 Failure domains & recovery paths
- 5.3 Resource Allocation Model
  - 5.3.1 YARN capacity: 12GB/6vcores per worker (of 16GB/8T)
  - 5.3.2 Spark executor sizing: 6 executors × 3 cores × 6GB = 18 cores, 36GB total
  - 5.3.3 Kafka broker resources: 1-2GB RAM, log.dirs on dedicated SSD
  - 5.3.4 ZooKeeper: minimal (256MB heap), co-located with Kafka
  - 5.3.5 Reservation for OS, monitoring agents, buffer

#### **6. Component Configuration Deep-Dive**
- 6.1 HDFS Configuration
  - 6.1.1 `core-site.xml`: fs.defaultFS, io.file.buffer.size, hadoop.tmp.dir
  - 6.1.2 `hdfs-site.xml`: replication=2, blocksize=128MB, namenode/datanode dirs, handler counts
  - 6.1.3 NameNode HA: not implemented (single NN), checkpoint on Worker3
  - 6.1.4 Directory structure: `/zeer/logs/{user,crash,injection}/YYYY/MM/DD`
- 6.2 YARN Configuration
  - 6.2.1 `yarn-site.xml`: RM hostname, NM memory/vcores, scheduler limits, vmem/pmem disabled
  - 6.2.2 `mapred-site.xml`: MR framework, map/reduce memory, AM resources
  - 6.2.3 Queue configuration: default, spark, batch queues with capacities
- 6.3 Spark Configuration
  - 6.3.1 `spark-env.sh`: master host, worker cores/memory, history server
  - 6.3.2 `spark-defaults.conf`: event log, serializer, shuffle partitions, YARN archive
  - 6.3.3 Spark on YARN: cluster mode, dynamic allocation (disabled for predictability)
- 6.4 Kafka Configuration
  - 6.4.1 `server.properties`: broker.id, listeners, replication, retention, compression=snappy
  - 6.4.2 Topic design: partitions=6 (2×brokers), RF=3, min.insync.replicas=2
  - 6.4.3 Topics: `zeer-user-events`, `zeer-crash-events`, `zeer-injection-events`
- 6.5 ZooKeeper Configuration
  - 6.5.1 `zoo.cfg`: tickTime, initLimit, syncLimit, dataDir, ensemble servers
  - 6.5.2 myid files on each worker
- 6.6 Hive Configuration
  - 6.6.1 Metastore: PostgreSQL backend, schema versioning
  - 6.6.2 HiveServer2: Thrift/HTTP ports, authentication (none for lab)
  - 6.6.3 Warehouse directory: `/user/hive/warehouse`
  - 6.6.4 Execution engine: Spark
- 6.7 Flume Configuration
  - 6.7.1 Agent: `zeer-kafka-hdfs-agent` on Worker1 & Worker2 (HA)
  - 6.7.2 Source: KafkaSource, topics, consumer group, timestamp interceptor
  - 6.7.3 Channel: memory, capacity=100K, transactionCapacity=10K
  - 6.7.4 Sink: HDFS, path by topic/date, rollInterval=600s, rollSize=128MB, DataStream

---

### **PART IV: DATA PIPELINE & PROCESSING ENGINEERING**

#### **7. Data Ingestion Layer**
- 7.1 Node.js API Collector (Master Node)
  - 7.1.1 Architecture: Express + TypeScript, KafkaJS producer, Winston logging
  - 7.1.2 Endpoints: `POST /api/logs/user-action`, `/api/logs/crash`, `/api/logs/injection`
  - 7.1.3 Health check, metrics exposition (/metrics for Prometheus)
  - 7.1.4 Configuration: `.env` with Kafka brokers, topics, Spark/YARN endpoints
  - 7.1.5 Deployment: systemd service, log rotation, graceful shutdown
- 7.2 Loader API (Game Client Integration) — *External Specification*
  - 7.2.1 Interface contract: payload format, authentication, batching
  - 7.2.2 Crash Log schema: timestamp, userId, sessionId, crashType, stackTrace, appVersion, platform, deviceInfo
  - 7.2.3 Inject Log schema: timestamp, sourceIp, endpoint, payload, injectionType, blocked, severity
  - 7.2.4 Delivery guarantees: at-least-once, retry with exponential backoff

#### **8. Stream Processing Layer**
- 8.1 Spark Streaming Jobs
  - 8.1.1 Crash Streaming Analyzer: 5-min tumbling windows, watermark 10min
    - Aggregations: crash count, affected users by crashType/appVersion
    - Output: Parquet to `/zeer/analytics/crashes/realtime`
    - Checkpointing: HDFS `/zeer/checkpoints/crashes`
  - 8.1.2 Injection Streaming Detector (future): ML-based anomaly scoring
- 8.2 Kafka Consumer Groups & Offset Management
  - Flume consumer group: `flume-consumer`
  - Spark streaming: dedicated consumer groups per job
  - Offset reset policies: earliest (batch), latest (streaming)

#### **9. Batch Processing & Analytics Layer**
- 9.1 User Actions Analyzer (Daily Batch)
  - 9.1.1 Input: `/zeer/logs/zeer-user-events/YYYY/MM/DD/*.log`
  - 9.1.2 Schema enforcement: StructType with timestamp, userId, sessionId, action, productId, category, metadata
  - 9.1.3 Analytics:
    - Actions by category & action type (popularity)
    - User activity: total actions, sessions, products viewed
    - Hourly activity heatmap
  - 9.1.4 Output: Partitioned Parquet to `/zeer/analytics/user-actions/YYYY/MM/DD/`
- 9.2 Crash Analytics (Batch)
  - 9.2.1 Daily aggregation: crash trends, version regression detection
  - 9.2.2 Device/platform segmentation
- 9.3 Injection Analytics (Batch + ML)
  - 9.3.1 Feature engineering: injectionType encoding, severity scoring
  - 9.3.2 RandomForest classifier for blocked prediction
  - 9.3.3 Model persistence: HDFS `/zeer/models/injection-detector`
- 9.4 Job Orchestration & Deployment
  - 9.4.1 SBT build: assembly plugin, provided Spark dependencies
  - 9.4.2 Deploy script: SCP JAR → master → spark-submit
  - 9.4.3 CI/CD considerations: GitHub Actions / GitLab CI

#### **10. Storage & Query Layer**
- 10.1 HDFS Data Lake Organization
  - 10.1.1 Raw zone: `/zeer/logs/` (immutable, partitioned by topic/date)
  - 10.1.2 Processed zone: `/zeer/analytics/` (Parquet, partitioned)
  - 10.1.3 Model zone: `/zeer/models/` (ML artifacts)
  - 10.1.4 Spark zone: `/spark-logs/`, `/spark-jars/`
  - 10.1.5 YARN zone: `/app-logs/` (aggregated logs)
- 10.2 Hive External Tables
  - 10.2.1 `zeer_user_events` partitioned by (year, month, day)
  - 10.2.2 `zeer_crash_events` partitioned by (year, month, day)
  - 10.2.3 `zeer_injection_events` partitioned by (year, month, day)
  - 10.2.4 Analytics views: daily summaries, user cohorts, crash trends
- 10.3 Data Lifecycle & Retention
  - 10.3.1 Raw logs: 90 days (HDFS TTL + manual cleanup)
  - 10.3.2 Aggregated analytics: 365 days
  - 10.3.3 Models: versioned, indefinite
  - 10.3.4 Compaction strategy: small file merge via Spark

---

### **PART V: CLUSTER MANAGEMENT & OPERATIONS**

#### **11. ADCM/ADH Cluster Lifecycle Management**
- 11.1 ADCM Deployment Architecture
  - 11.1.1 Docker Compose: ADCM server + PostgreSQL
  - 11.1.2 UI access: http://zeer-master:8000, API: https://zeer-master:9443
  - 11.1.3 License management, backup/restore
- 11.2 Cluster Provisioning via ADCM
  - 11.2.1 Host registration: SSH key-based, host checks
  - 11.2.2 Cluster creation: `zeer-bigdata-cluster`
  - 11.2.3 Service addition order: ZooKeeper → HDFS → YARN → Spark → Kafka → Hive → Flume
  - 11.2.4 Configuration templates: reference to `configs/adcm-service-config.md`
- 11.3 Day-2 Operations via ADCM
  - 11.3.1 Rolling restart, configuration updates, version upgrades
  - 11.3.2 Decommissioning nodes, rebalancing HDFS
  - 11.3.3 Backup: HDFS metadata (fsimage + edits), Hive metastore, ADCM DB

#### **12. Monitoring, Alerting & Observability**
- 12.1 Prometheus Exporters & Metrics Collection
  - 12.1.1 Node Exporter (all nodes): CPU, memory, disk, network, thermal
  - 12.1.2 JMX Exporters: NameNode, DataNode, RM, NM, Kafka, Spark, Hive
  - 12.1.3 Custom exporters: Flume, Node.js API, thermal watchdog
  - 12.1.4 Scrape config: intervals, relabeling, target discovery
- 12.2 Grafana Dashboards
  - 12.2.1 Cluster Overview: node health, resource utilization
  - 12.2.2 HDFS: capacity, blocks, replication, DN status
  - 12.2.3 YARN: apps, containers, queue metrics, NM health
  - 12.2.4 Kafka: broker health, topic lag, throughput, ISR
  - 12.2.5 Spark: application metrics, executor utilization, shuffle
  - 12.2.6 Business: events/sec by type, crash rate, injection rate
  - 12.2.7 Environmental: ambient temp/humidity, power draw, fan speeds
- 12.3 Alerting Rules (Alertmanager)
  - 12.3.1 Critical: node down, NN/DN dead, RM down, Kafka broker down
  - 12.3.2 Warning: CPU > 85%, disk > 80%, replication < 2, consumer lag > 10K
  - 12.3.3 Thermal: CPU > 80°C, ambient > 28°C, humidity > 70%
  - 12.3.4 Notification channels: Telegram, email, webhook to Node.js API

#### **13. Operational Procedures & Runbooks**
- 13.1 Cluster Startup/Shutdown Sequence
  - 13.1.1 Cold boot: ZooKeeper → HDFS → YARN → Kafka → Spark → Flume → API
  - 13.1.2 Graceful shutdown: reverse order, drain NMs, flush Kafka
- 13.2 Common Operational Tasks
  - 13.2.1 Adding/removing DataNodes
  - 13.2.2 Rebalancing HDFS, repairing under-replicated blocks
  - 13.2.3 Kafka topic management: create, alter, delete, reassign partitions
  - 13.2.4 Spark job submission, monitoring, killing stuck apps
  - 13.2.5 Hive metastore backup/restore
- 13.3 Incident Response Runbooks
  - 13.3.1 DataNode offline: diagnosis, recovery, decommission
  - 13.3.2 Kafka consumer lag spike: cause analysis, remediation
  - 13.3.3 YARN NM unhealthy: log analysis, restart, capacity recovery
  - 13.3.4 Thermal emergency: watchdog auto-actions, manual intervention
- 13.4 Backup & Disaster Recovery
  - 13.4.1 HDFS metadata: daily fsimage fetch, edits retention
  - 13.4.2 Hive metastore: daily pg_dump
  - 13.4.3 ADCM DB: daily pg_dump
  - 13.4.4 Configuration: Git-backed (`zeer-bigdata/` repo)
  - 13.4.5 Recovery time objective (RTO) / recovery point objective (RPO) estimates

---

### **PART VI: TESTING, VALIDATION & PERFORMANCE**

#### **14. Testing Strategy & Results**
- 14.1 Unit Testing (Spark Jobs)
  - 14.1.1 ScalaTest + Spark local mode
  - 14.1.2 Test coverage: schema parsing, aggregations, edge cases
  - 14.1.3 Property-based testing for data transformations
- 14.2 Integration Testing
  - 14.2.1 End-to-end pipeline: API → Kafka → Flume → HDFS → Spark → Hive
  - 14.2.2 Test data generator: synthetic logs matching production schemas
  - 14.2.3 Contract testing: Avro/Protobuf schema compatibility
- 14.3 Performance & Load Testing
  - 14.3.1 Baseline: single-node Spark local mode benchmarks
  - 14.3.2 Cluster benchmarks: 3-node YARN, varying executor counts
  - 14.3.3 Kafka throughput: producer/consumer perf with replication
  - 14.3.4 HDFS I/O: `dfsio` benchmarks (read/write throughput, IOPS)
  - 14.3.5 Stress test: 4h `stress-ng` thermal validation
- 14.4 Fault Injection & Chaos Testing
  - 14.4.1 Single DataNode failure: HDFS/YARN recovery time
  - 14.4.2 Kafka broker failure: ISR reassignment, producer retry
  - 14.4.3 Network partition simulation: split-brain behavior
  - 14.4.4 Power loss: unclean shutdown, fsck duration, data integrity

#### **15. Validation Checklists & Sign-Off**
- 15.1 Pre-Production Checklist (from `PHYSICAL_SAFETY_AND_COOLING.md`)
- 15.2 Post-ADCM Verification Script Results (`05-post-adcm-verify.sh`)
- 15.3 End-to-End Pipeline Test Results
- 15.4 Performance Baseline Report
- 15.5 Security Audit: open ports, default passwords, encryption at rest/in transit

---

### **PART VII: FUTURE WORK & SCALABILITY**

#### **16. Evolution Roadmap**
- 16.1 Short-term (0-3 months)
  - Kerberos authentication, TLS encryption
  - Hive ACID transactions, materialized views
  - Airflow for job orchestration
  - Elasticsearch + Kibana for log search
- 16.2 Medium-term (3-12 months)
  - 4th/5th DataNode addition
  - HDFS Federation for NameNode scaling
  - Trino/Presto for interactive SQL
  - Kafka Streams for real-time enrichment
- 16.3 Long-term (12+ months)
  - Multi-datacenter replication (MirrorMaker)
  - Kubernetes migration (Spark Operator, Strimzi)
  - Delta Lake / Iceberg for ACID + time travel
  - Real-time ML serving (KServe, MLflow)

#### **17. Lessons Learned & Retrospective**
- 17.1 Hardware decisions: LGA 1155 viability, cooling challenges
- 17.2 Software choices: ADCM value vs complexity, vanilla Hadoop maintenance burden
- 17.3 Pipeline design: Flume vs Kafka Connect, batch vs streaming tradeoffs
- 17.4 Operational insights: monitoring gaps, alert fatigue, runbook effectiveness

---

### **PART VIII: APPENDICES**

#### **Appendix A: Complete Configuration Files**
- A.1 All XML configs: core-site, hdfs-site, yarn-site, mapred-site, spark-env, spark-defaults, zoo.cfg, server.properties, flume.conf
- A.2 systemd unit files: thermal-watchdog, zeer-api, zeer-flume, zeer-spark-worker
- A.3 Docker Compose: ADCM, Prometheus/Grafana
- A.4 Prometheus scrape configs, alerting rules
- A.5 Node.js `.env.example`, `tsconfig.json`, `package.json`

#### **Appendix B: Scripts & Automation**
- B.1 Deployment scripts: 01-base-setup.sh, 02-network-setup.sh, 03-ssh-setup.sh, 04-adcm-deploy.sh, 05-post-adcm-verify.sh
- B.2 Thermal watchdog: `thermal-watchdog.py`
- B.3 Maintenance: backup scripts, log rotation, cleanup cron jobs
- B.4 Health check: `cluster-health-check.sh`

#### **Appendix C: Schemas & Data Models**
- C.1 Action Log JSON Schema (Avro/JSON Schema)
- C.2 Crash Log JSON Schema
- C.3 Injection Log JSON Schema
- C.4 Hive DDL: CREATE EXTERNAL TABLE statements
- C.5 Spark job parameters & CLI reference

#### **Appendix D: Network & Hardware Diagrams**
- D.1 Physical rack layout (top-down view)
- D.2 Network topology (L2/L3, VLANs if any)
- D.3 Power distribution: PDU, UPS, surge protectors
- D.4 Sensor placement map (thermal, humidity, power, leak)
- D.5 Cable labeling scheme reference

#### **Appendix E: Bill of Materials & Cost Analysis**
- E.1 Hardware BOM: CPUs, RAM, SSDs, PSUs, coolers, fans, cases, network
- E.2 Safety & cooling BOM: thermal paste, filters, heaters, sensors, extinguisher
- E.3 Software licenses: ADCM (if commercial), OS, monitoring
- E.4 Total cost of ownership (TCO) estimate: CapEx + OpEx (electricity)

#### **Appendix F: Glossary & Abbreviations**
- F.1 Big Data terms: HDFS, YARN, RDD, DAG, shuffle, partition, replication
- F.2 Infrastructure terms: TDP, PWM, VRM, MOV, UPS, RTO/RPO
- F.3 Project-specific: Zeer, Loader API, Inject Log, ADCM, ADH

#### **Appendix G: References & Standards**
- G.1 Apache project documentation links
- G.2 IEC/ASHRAE/GOST standards referenced
- G.3 Academic papers: MapReduce, Spark, Kafka, Lambda/Kappa architecture
- G.4 Vendor datasheets: Intel ARK, cooler specs, PSU specs

---

## 📐 DIAGRAMS REQUIRED (Mermaid / Draw.io / PlantUML)

| # | Diagram | Type | Location |
|---|---------|------|----------|
| 1 | Cluster logical topology | Architecture | Ch 3.1, 5.2.1 |
| 2 | Data flow pipeline | Sequence/Flow | Ch 5.2.1, 7-10 |
| 3 | Control plane (ADCM) | Component | Ch 5.2.2, 11 |
| 4 | Monitoring plane | Deployment | Ch 5.2.3, 12 |
| 5 | Failure domains | Network | Ch 5.2.4 |
| 6 | Case airflow (per node) | Physical | Ch 3.4.3 |
| 7 | Thermal protection layers | State machine | Ch 3.4.4 |
| 8 | Condensation risk chart | Chart | Ch 3.4.6 |
| 9 | Rack layout (top-down) | Floor plan | Appendix D.1 |
| 10 | Power distribution | Single-line | Appendix D.3 |
| 11 | Sensor placement | Floor plan | Appendix D.4 |
| 12 | Spark job DAG | DAG | Ch 9.1 |
| 12 | Hive table partitioning | ER/Schema | Ch 10.2 |

---

## ✅ DOCUMENTATION QUALITY CHECKLIST

For each section, ensure:
- [ ] **Diagrams** are version-controlled (source: `.drawio`, `.mmd`, `.puml`)
- [ ] **Config files** are actual production configs (not templates), secrets redacted
- [ ] **Commands** are copy-pasteable, tested, with expected output samples
- [ ] **Tables** have units, sources, and "why this value" rationale
- [ ] **Code snippets** compile/run, with language tags
- [ ] **Cross-references** use section numbers (not page numbers)
- [ ] **Terminology** is consistent (glossary in Appendix F)
- [ ] **Metrics** have baseline + target + actual measured values
- [ ] **Decisions** document alternatives considered + tradeoffs (ADR-style)

---

## 📝 WRITING GUIDELINES FOR THESIS

| Aspect | Guideline |
|--------|-----------|
| **Voice** | Passive for methods ("was configured"), active for decisions ("we chose") |
| **Tense** | Past for completed work, present for architecture/design, future for roadmap |
| **Precision** | "CPU temperature reached 78°C" not "CPU got hot" |
| **Reproducibility** | Every config value must be traceable to a source (calculation, benchmark, vendor spec) |
| **Honesty** | Document failures, workarounds, limitations — committee values integrity |
| **Visuals** | One diagram per 2-3 pages of text; every diagram has caption + figure number |

---

## 🗂️ FILE ORGANIZATION FOR THESIS REPO

```
zeer-bigdata-thesis/
├── thesis/
│   ├── 01-introduction.md
│   ├── 02-requirements.md
│   ├── 03-infrastructure/
│   │   ├── 01-topology.md
│   │   ├── 02-hardware.md
│   │   ├── 03-assembly.md
│   │   ├── 04-physical-safety-cooling.md    ← From PHYSICAL_SAFETY_AND_COOLING.md
│   │   └── 04-os-base.md
│   ├── 04-architecture/
│   │   ├── 01-stack-selection.md
│   │   ├── 02-diagrams/
│   │   ├── 03-resource-model.md
│   │   └── 04-config-deep-dive/
│   ├── 05-pipeline/
│   │   ├── 01-ingestion.md
│   │   ├── 02-streaming.md
│   │   ├── 03-batch.md
│   │   └── 04-storage-query.md
│   ├── 06-operations/
│   │   ├── 01-adcm.md
│   │   ├── 02-monitoring.md
│   │   ├── 03-runbooks.md
│   │   └── 04-backup-dr.md
│   ├── 07-testing/
│   │   ├── 01-unit-integration.md
│   │   ├── 02-performance.md
│   │   ├── 03-chaos.md
│   │   └── 04-validation.md
│   ├── 08-future-work.md
│   ├── 09-retrospective.md
│   └── appendices/
│       ├── A-configs/
│       ├── B-scripts/
│       ├── C-schemas/
│       ├── D-diagrams/
│       ├── E-bom/
│       ├── F-glossary.md
│       └── G-references.md
├── diagrams/           # Source files (.drawio, .mmd)
├── configs/            # Production configs (secrets in .gitignore)
├── scripts/            # All .sh, .py files
└── README.md
```

---

## 🎯 PRIORITY SECTIONS FOR DEFENSE PREPARATION

**Must-have deep knowledge (expect detailed questions):**
1. **Ch 3.4** — Physical safety & cooling (your unique contribution)
2. **Ch 5.1** — Technology selection rationale (why not CDH/EMR/K8s)
3. **Ch 6** — Config deep-dive (shows you understand each knob)
4. **Ch 9** — Spark job logic (analytics, not just plumbing)
5. **Ch 12** — Monitoring design (observability maturity)
6. **Ch 14** — Performance numbers (prove it works)
7. **Ch 13.3** — Incident response (operational maturity)

**Prepare 5-min demo script:**
- Live Grafana dashboard walkthrough
- Submit Spark job → watch in Spark UI → check Hive result
- Simulate thermal alert → show watchdog action → Grafana alert

---

**End of Documentation Structure**

*Generated from analysis of: servers-infrastructure-software-implementation-plan.md, big-data-project-zeer-implementation.txt, PHYSICAL_SAFETY_AND_COOLING.md, and project context.*

📋 Structure Overview (8 Parts, 17 Chapters, 7 Appendices)

Part I: Introduction & Requirements (Ch 1-2)

- Problem statement, goals, success criteria
- Functional/non-functional requirements with IDs (FR-1..7, NFR-1..5)
- Data characteristics: volume, velocity, variety, veracity

Part II: Infrastructure & Hardware (Ch 3-4) ⭐ Your unique contribution

- Ch 3.1-3.3: Network topology, hardware specs, assembly, cable management
- Ch 3.4: Physical Safety & Cooling Engineering — 12 subsections covering:
  - Thermal envelope (140-160W/node), cooler selection per CPU generation
  - Positive pressure airflow design with fan curves
  - 4-layer overheating protection (CPU → BIOS → OS → Ap
  - Electrical safety, grounding, ESD, UPS
  - Winter condensation protection: dew point math, pre-ing
  - Fire safety, vibration mitigation, IoT environmental monitoring
  - Maintenance schedule + emergency runbooks
    -4h stress test validation checklist
- Ch 4: Ubuntu hardening, kernel tuning, Docker

Part III: Big Data Platform Architecture (Ch 5-6)

- Technology selection rationale (why vanilla Apache, no
- Architecture diagrams (data/control/monitoring planes)
- Resource allocation model (YARN 12GB/6vcores, Spark 6
- Deep-dive configs for every component (HDFS, YARN, Spark, Kafka, ZK, Hive, Flume)

Part IV: Data Pipeline Engineering (Ch 7-10)

- Node.js API collector (endpoints, deployment, health)
- Loader API contract (Crash/Inject log schemas)
- Spark Streaming (crash 5-min windows, watermark, checkpointing)
- Batch analytics (user actions, crash trends, injection
- HDFS data lake zones + Hive external tables + retention policy

Part V: Cluster Management & Operations (Ch 11-13)

- ADCM lifecycle: host registration → service addition order → day-2 ops
- Prometheus/Grafana: exporters, 7 dashboards, alerting
- Runbooks: startup/shutdown, common tasks, incident response, DR

Part VI: Testing & Validation (Ch 14-15)

- Unit/integration/performance/chaos testing
- Validation checklists with sign-off

Part VII: Future Work & Retrospective (Ch 16-17)

- 3-horizon roadmap (Kerberos/TLS → 4th node → K8s/Delta
- Lessons learned

Part VIII: Appendices (A-G)

- All configs, scripts, schemas, diagrams, BOM, glossary, standards

---

🎯 Defense-Ready Features

┌─────────────────────────────────────────────────────────┬──────────┐
│                         Feature
├─────────────────────────────────────────────────────────┼──────────┤
│ 23 Mermaid/Draw.io diagrams specified with locations
├─────────────────────────────────────────────────────────┼──────────┤
│ ADR-style decision logs (why this config value)
├─────────────────────────────────────────────────────────┼──────────┤
│ Actual measured metrics placeholders (not just targets
├─────────────────────────────────────────────────────────┼──────────┤
│ Cross-referenced sections (no page numbers)
├─────────────────────────────────────────────────────────┼──────────┤
│ File repo structure for thesis writing
├─────────────────────────────────────────────────────────┼──────────┤
│ Priority chapters for defense prep identified
├─────────────────────────────────────────────────────────┼──────────┤
│ 5-min live demo script outlined
└─────────────────────────────────────────────────────────┴──────────┘

---

🔗 Integration with Your Existing Docs

┌────────────────────────────────────────────────────────┬────────────────────────────────────────┐
│                        Your Doc                                    │
├────────────────────────────────────────────────────────┼────────────────────────────────────────┤
│ servers-infrastructure-software-implementation-plan.mdB            │
├────────────────────────────────────────────────────────┼────────────────────────────────────────┤
│ big-data-project-zeer-implementation.txt                           │
├────────────────────────────────────────────────────────┼────────────────────────────────────────┤
│ PHYSICAL_SAFETY_AND_COOLING.md                        Appendix D/E │
└────────────────────────────────────────────────────────┴────────────────────────────────────────┘                                     
The structure is thesis-committee ready — every section explains what, why, how, and with what evidence. Want me to expand any specific chapter into a detailed writing template?
