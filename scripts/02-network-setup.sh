#!/bin/bash
# Zeer Big Data - Network Configuration
# Run on ALL 4 nodes after base setup and reboot

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $*${NC}"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $*${NC}"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*${NC}"; exit 1; }

NODE_TYPE="${1:-worker}"
NODE_NUM="${2:-1}"

log "=== Network Setup for $NODE_TYPE$NODE_NUM ==="

# Determine IP based on node type
case "$NODE_TYPE$NODE_NUM" in
    "master")
        STATIC_IP="192.168.1.5"
        HOSTNAME="zeer-master"
        ;;
    "worker1")
        STATIC_IP="192.168.1.10"
        HOSTNAME="zeer-worker1"
        ;;
    "worker2")
        STATIC_IP="192.168.1.11"
        HOSTNAME="zeer-worker2"
        ;;
    "worker3")
        STATIC_IP="192.168.1.12"
        HOSTNAME="zeer-worker3"
        ;;
    *)
        error "Usage: $0 <master|worker> [1|2|3]"
        ;;
esac

# Get interface name (usually eth0 or enp3s0)
INTERFACE=$(ip route | grep default | awk '{print $5}' | head -1)
if [[ -z "$INTERFACE" ]]; then
    INTERFACE=$(ls /sys/class/net | grep -E '^e(n|th)' | head -1)
fi
log "Using network interface: $INTERFACE"

# 1. Configure Netplan for static IP
log "Configuring static IP: $STATIC_IP on $INTERFACE"
cat << NETPLAN | sudo tee /etc/netplan/01-static.yaml
network:
  version: 2
  renderer: networkd
  ethernets:
    $INTERFACE:
      dhcp4: false
      addresses:
        - $STATIC_IP/24
      routes:
        - to: default
          via: 192.168.1.1
      nameservers:
        addresses: [8.8.8.8, 8.8.4.4, 192.168.1.1]
NETPLAN

sudo netplan apply

# 2. Update /etc/hosts on all nodes
log "Updating /etc/hosts..."
cat << HOSTS | sudo tee /etc/hosts
127.0.0.1       localhost
127.0.1.1       $HOSTNAME

# Zeer Big Data Cluster
192.168.1.5     zeer-master
192.168.1.10    zeer-worker1
192.168.1.11    zeer-worker2
192.168.1.12    zeer-worker3

# The following lines are desirable for IPv6 capable hosts
::1     localhost ip6-localhost ip6-loopback
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
HOSTS

# 3. Verify hostname
sudo hostnamectl set-hostname $HOSTNAME

# 4. Test connectivity
log "Testing network connectivity..."
ping -c 3 192.168.1.1 || warn "Gateway ping failed"
ping -c 3 8.8.8.8 || warn "Internet ping failed"

# 5. Test internal cluster connectivity
for host in zeer-master zeer-worker1 zeer-worker2 zeer-worker3; do
    if [[ "$host" != "$HOSTNAME" ]]; then
        ping -c 2 $host && log "✓ $host reachable" || warn "✗ $host unreachable"
    fi
done

log "=== Network setup completed for $HOSTNAME ($STATIC_IP) ==="