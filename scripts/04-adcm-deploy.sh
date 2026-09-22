#!/bin/bash
# Zeer Big Data - ADCM Deployment Script
# Run on MASTER node after SSH setup is complete

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $*${NC}"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $*${NC}"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*${NC}"; exit 1; }

# Verify running on master
if [[ "$(hostname)" != "zeer-master" ]]; then
    error "This script must run on MASTER node (zeer-master)"
fi

log "=== ADCM Deployment ==="

# 1. Create ADCM directory
ADCM_DIR="/opt/adcm"
sudo mkdir -p $ADCM_DIR
sudo chown hadoop:hadoop $ADCM_DIR
cd $ADCM_DIR

# 2. Copy config files
log "Copying ADCM configuration..."
cp ~/zeer-bigdata/adcm/docker-compose.yml $ADCM_DIR/
cp ~/zeer-bigdata/adcm/adcm.env $ADCM_DIR/.env

# 3. Start ADCM
log "Starting ADCM containers..."
docker-compose --env-file .env up -d

# 4. Wait for ADCM to be ready
log "Waiting for ADCM to start..."
sleep 30

for i in {1..30}; do
    if curl -s -f http://localhost:8000/health >/dev/null 2>&1; then
        log "ADCM is ready!"
        break
    fi
    echo -n "."
    sleep 5
done

# 5. Show access info
log "=== ADCM Access Information ==="
log "UI: http://192.168.1.5:8000"
log "API: https://192.168.1.5:9443"
log "Default credentials: admin / admin"
log ""
log "Next steps in ADCM UI:"
log "1. Change default password"
log "2. Upload license file"
log "3. Add hosts (zeer-master, zeer-worker1, zeer-worker2, zeer-worker3)"
log "4. Run 'Host Check' on each host"
log "5. Create cluster 'zeer-cluster'"
log "6. Add services in order: ZooKeeper, HDFS, YARN, Spark, Kafka, Hive, Flume"
log "7. Configure service parameters (see configs/adcm-service-config.md)"
log "8. Run 'Install' then 'Start' on cluster"

# 6. Save ADCM info
cat << INFO > ~/zeer-bigdata/ADCM_INFO.txt
ADCM Deployment Complete
========================
Date: $(date)
UI: http://192.168.1.5:8000
API: https://192.168.1.5:9443
Credentials: admin / admin (CHANGE IMMEDIATELY)

Cluster Name: zeer-bigdata-cluster
Hosts:
  - zeer-master (192.168.1.5) - Master/NameNode/ResourceManager
  - zeer-worker1 (192.168.1.10) - DataNode/NodeManager/Kafka/ZooKeeper/Flume
  - zeer-worker2 (192.168.1.11) - DataNode/NodeManager/Kafka/ZooKeeper/Flume
  - zeer-worker3 (192.168.1.12) - DataNode/NodeManager/Kafka/ZooKeeper

Services to Add:
  1. ZooKeeper (3 nodes)
  2. HDFS (1 NameNode + 3 DataNodes)
  3. YARN (1 RM + 3 NMs)
  4. Spark (History Server on Master)
  5. Kafka (3 Brokers)
  6. Hive (Metastore + HiveServer2 on Master)
  7. Flume (2 Agents on Worker1, Worker2)

Configuration files: ~/zeer-bigdata/configs/
INFO

log "ADCM deployment completed! Check ~/zeer-bigdata/ADCM_INFO.txt"