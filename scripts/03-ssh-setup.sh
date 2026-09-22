#!/bin/bash
# Zeer Big Data - SSH Passwordless Setup
# Run ONLY on MASTER node after all nodes have base setup and network configured

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

log "=== SSH Passwordless Setup from Master ==="

# 1. Ensure SSH keys exist on master
if [[ ! -f ~/.ssh/id_rsa ]]; then
    log "Generating SSH keys on master..."
    ssh-keygen -t rsa -b 4096 -P '' -f ~/.ssh/id_rsa
fi

# 2. Copy keys to all nodes
NODES=("zeer-master" "zeer-worker1" "zeer-worker2" "zeer-worker3")

for node in "${NODES[@]}"; do
    log "Copying SSH key to $node..."
    # First, accept host key
    ssh-keyscan -H $node >> ~/.ssh/known_hosts 2>/dev/null
    ssh-keyscan -H $node >> ~/.ssh/known_hosts 2>/dev/null

    # Copy key (will prompt for password)
    ssh-copy-id -o StrictHostKeyChecking=no hadoop@$node || {
        warn "Failed to copy key to $node - trying with password authentication"
        ssh-copy-id hadoop@$node
    }
done

# 3. Verify passwordless SSH
log "Verifying passwordless SSH..."
for node in "${NODES[@]}"; do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 hadoop@$node "echo 'SSH OK'"; then
        log "✓ $node: Passwordless SSH working"
    else
        error "✗ $node: Passwordless SSH FAILED"
    fi
done

# 4. Test cluster-wide SSH
log "Testing cross-node SSH..."
for node in "${NODES[@]}"; do
    ssh hadoop@$node "hostname" && log "✓ $node responds correctly"
done

# 5. Configure SSH for faster connections
log "Optimizing SSH config..."
cat << 'SSHCONFIG' > ~/.ssh/config
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
    ConnectTimeout 10
    ServerAliveInterval 30
    ServerAliveCountMax 3
    ControlMaster auto
    ControlPath ~/.ssh/cm-%r@%h:%p
    ControlPersist 600
SSHCONFIG
chmod 600 ~/.ssh/config

log "=== SSH setup completed ==="
log "All nodes now accessible via passwordless SSH from master"