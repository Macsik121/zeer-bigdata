#!/bin/bash
# Zeer Big Data - Base OS Setup for All Nodes
# Run on ALL 4 nodes (Master + 3 Workers)

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $*${NC}"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $*${NC}"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*${NC}"; exit 1; }

# Check if running as hadoop user
if [[ "$USER" != "hadoop" ]]; then
    error "This script must be run as 'hadoop' user. Run: su - hadoop"
fi

NODE_TYPE="${1:-worker}"  # master or worker
NODE_NUM="${2:-1}"        # 1, 2, 3 for workers

log "=== Zeer Big Data Base Setup - Node Type: $NODE_TYPE$NODE_NUM ==="

# 1. System Update
log "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# 2. Install Base Packages
log "Installing base packages..."
sudo apt install -y \
    openssh-server vim net-tools curl wget git rsync \
    python3 python3-pip build-essential \
    openjdk-11-jdk ntp chrony \
    docker.io docker-compose-plugin

# 3. Configure Java
log "Configuring Java environment..."
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
java -version

# 4. Add hadoop to docker group
log "Adding hadoop to docker group..."
sudo usermod -aG docker hadoop
newgrp docker << 'EOF'
docker --version
EOF

# 5. Configure SSH
log "Configuring SSH..."
if [[ ! -f ~/.ssh/id_rsa ]]; then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    log "SSH key generated"
fi

# 6. Configure chrony for time sync
log "Configuring time synchronization..."
sudo systemctl enable chrony
sudo systemctl start chrony

if [[ "$NODE_TYPE" == "master" ]]; then
    # Master acts as NTP server for LAN
    sudo sed -i '/^pool /d' /etc/chrony/chrony.conf
    echo "allow 192.168.1.0/24" | sudo tee -a /etc/chrony/chrony.conf
    echo "local stratum 10" | sudo tee -a /etc/chrony/chrony.conf
    sudo systemctl restart chrony
else
    # Workers sync from master
    sudo sed -i 's/^pool .*/server zeer-master iburst/' /etc/chrony/chrony.conf
    sudo systemctl restart chrony
fi

# 7. Kernel Tuning
log "Applying kernel tuning..."
cat << 'KERNEL' | sudo tee /etc/sysctl.d/99-hadoop.conf
vm.swappiness=1
vm.overcommit_memory=1
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=65535
fs.file-max=1000000
KERNEL
sudo sysctl --system

# 8. Set ulimits
log "Setting ulimits..."
cat << 'ULIMIT' | sudo tee /etc/security/limits.d/hadoop.conf
hadoop soft nofile 1000000
hadoop hard nofile 1000000
hadoop soft nproc 65535
hadoop hard nproc 65535
ULIMIT

# 9. Disable Transparent Huge Pages
log "Disabling Transparent Huge Pages..."
echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
cat << 'THP' | sudo tee /etc/rc.local
#!/bin/bash
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
exit 0
THP
sudo chmod +x /etc/rc.local

# 10. Create Hadoop directories
log "Creating Hadoop directories..."
sudo mkdir -p /opt/hadoop/{hdfs,tmp,logs}
sudo mkdir -p /opt/kafka/kafka-logs
sudo mkdir -p /opt/zookeeper/data
sudo mkdir -p /opt/flume/conf
sudo mkdir -p /opt/spark
sudo mkdir -p /opt/zeer/{jars,logs}
sudo chown -R hadoop:hadoop /opt/hadoop /opt/kafka /opt/zookeeper /opt/flume /opt/spark /opt/zeer

# 11. Configure hostname
HOSTNAME=$(hostname)
if [[ "$NODE_TYPE" == "master" ]]; then
    sudo hostnamectl set-hostname zeer-master
else
    sudo hostnamectl set-hostname zeer-worker$NODE_NUM
fi

log "=== Base setup completed for $NODE_TYPE$NODE_NUM ==="
log "IMPORTANT: Reboot required for all changes to take effect"
log "Run: sudo reboot"