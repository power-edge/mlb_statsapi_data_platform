#!/usr/bin/env bash
#
# Repair K3s cluster - Fix bootstrap lock issue
#
# This script fixes the "Bootstrap key already locked" issue that occurs
# when K3s gets stuck waiting for cluster data that never arrives.
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() { echo -e "${BLUE}ℹ${NC} $*"; }
success() { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}⚠${NC} $*"; }
error() { echo -e "${RED}✗${NC} $*"; }

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    error "This script must be run as root (use sudo)"
    exit 1
fi

echo "========================================="
echo "K3s Cluster Repair Script"
echo "========================================="
echo ""

info "Current K3s status:"
systemctl status k3s --no-pager | head -10
echo ""

# Step 1: Stop K3s
info "Step 1: Stopping K3s service..."
systemctl stop k3s || true
sleep 5

# Kill any remaining K3s processes
info "Killing any remaining K3s processes..."
pkill -9 k3s || true
sleep 2

success "K3s stopped"
echo ""

# Step 2: Clean up the stuck bootstrap lock
info "Step 2: Cleaning up bootstrap lock..."

# Backup the current state
BACKUP_DIR="/var/lib/rancher/k3s-backup-$(date +%Y%m%d-%H%M%S)"
if [ -d "/var/lib/rancher/k3s/server" ]; then
    info "Creating backup at ${BACKUP_DIR}..."
    mkdir -p "${BACKUP_DIR}"
    cp -r /var/lib/rancher/k3s/server "${BACKUP_DIR}/" || true
    success "Backup created"
fi

# Remove the database lock files
info "Removing database lock files..."
rm -rf /var/lib/rancher/k3s/server/db/etcd/member/
rm -f /var/lib/rancher/k3s/server/db/state.db*
rm -f /var/lib/rancher/k3s/server/cred/bootstrap.lock

success "Lock files removed"
echo ""

# Step 3: Reset to single-server mode
info "Step 3: Ensuring single-server configuration..."

# Check if K3s config exists
K3S_CONFIG="/etc/rancher/k3s/config.yaml"
if [ -f "$K3S_CONFIG" ]; then
    info "Backing up K3s config..."
    cp "$K3S_CONFIG" "${K3S_CONFIG}.backup-$(date +%Y%m%d-%H%M%S)"
fi

# Create a clean single-server config
mkdir -p /etc/rancher/k3s
cat > "$K3S_CONFIG" <<'EOF'
# K3s single-server configuration
# No cluster-init, no server URL - standalone mode
write-kubeconfig-mode: "0644"
tls-san:
  - "127.0.0.1"
  - "localhost"
EOF

success "Configuration set to single-server mode"
echo ""

# Step 4: Start K3s
info "Step 4: Starting K3s service..."
systemctl start k3s

info "Waiting for K3s to start (30 seconds)..."
sleep 30

# Step 5: Check status
echo ""
info "Step 5: Checking K3s status..."
systemctl status k3s --no-pager | head -15
echo ""

# Test API server
info "Testing API server..."
sleep 10

if curl -k https://127.0.0.1:6443/healthz 2>/dev/null | grep -q "ok"; then
    success "API server is responding!"
else
    warn "API server not responding yet, may need more time..."
    info "You can check logs with: journalctl -u k3s -f"
fi

echo ""

# Step 6: Set up kubeconfig
info "Step 6: Setting up kubeconfig access..."

# Copy kubeconfig to user's home directory
if [ -f /etc/rancher/k3s/k3s.yaml ]; then
    KUBE_CONFIG="/home/$SUDO_USER/.kube/config"
    mkdir -p "/home/$SUDO_USER/.kube"

    cp /etc/rancher/k3s/k3s.yaml "$KUBE_CONFIG"
    chown -R "$SUDO_USER:$SUDO_USER" "/home/$SUDO_USER/.kube"
    chmod 600 "$KUBE_CONFIG"

    success "Kubeconfig copied to $KUBE_CONFIG"
    echo ""
    info "Add this to your ~/.bashrc or ~/.zshrc:"
    echo "  export KUBECONFIG=~/.kube/config"
else
    warn "K3s kubeconfig not found yet, may still be starting..."
fi

echo ""
echo "========================================="
success "K3s repair completed!"
echo "========================================="
echo ""
echo "Next steps:"
echo "  1. Run: export KUBECONFIG=~/.kube/config"
echo "  2. Test: kubectl get nodes"
echo "  3. If still not working, check logs: journalctl -u k3s -f"
echo ""
echo "Backup location: ${BACKUP_DIR}"
