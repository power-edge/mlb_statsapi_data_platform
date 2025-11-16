#!/usr/bin/env bash
#
# K3d cluster management for MLB Stats API Data Platform
#
# Creates an isolated local K3d cluster with:
# - Project-specific name: mlb-data-platform-local
# - Automatic kubeconfig merge to ~/.kube/config
# - Port mappings for PostgreSQL, MinIO, and web UIs
# - Persistent volumes for data
#

set -euo pipefail

# Configuration
CLUSTER_NAME="mlb-data-platform-local"
KUBECONFIG_FILE="${HOME}/.kube/config"
K3S_VERSION="${K3S_VERSION:-v1.28.5-k3s1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
info() {
    echo -e "${BLUE}ℹ ${NC}$*"
}

success() {
    echo -e "${GREEN}✓${NC} $*"
}

warn() {
    echo -e "${YELLOW}⚠${NC} $*"
}

error() {
    echo -e "${RED}✗${NC} $*"
}

check_dependencies() {
    info "Checking dependencies..."

    local missing_deps=()

    if ! command -v k3d &> /dev/null; then
        missing_deps+=("k3d")
    fi

    if ! command -v kubectl &> /dev/null; then
        missing_deps+=("kubectl")
    fi

    if ! command -v docker &> /dev/null; then
        missing_deps+=("docker")
    fi

    if [ ${#missing_deps[@]} -gt 0 ]; then
        error "Missing required dependencies: ${missing_deps[*]}"
        echo ""
        echo "Install instructions:"
        echo "  k3d:     brew install k3d"
        echo "  kubectl: brew install kubectl"
        echo "  docker:  Install Docker Desktop from https://www.docker.com/products/docker-desktop"
        exit 1
    fi

    success "All dependencies installed"
}

cluster_exists() {
    k3d cluster list | grep -q "^${CLUSTER_NAME} "
}

create_cluster() {
    info "Creating K3d cluster: ${CLUSTER_NAME}"

    if cluster_exists; then
        warn "Cluster ${CLUSTER_NAME} already exists"
        return 0
    fi

    # Create cluster with:
    # - 1 server node
    # - 2 agent nodes (workers)
    # - Port mappings for services (using high ports to avoid conflicts)
    # - Volume mounts for persistence

    # Create volume directory if it doesn't exist
    mkdir -p "/tmp/k3d-${CLUSTER_NAME}"

    k3d cluster create "${CLUSTER_NAME}" \
        --image "rancher/k3s:${K3S_VERSION}" \
        --servers 1 \
        --agents 2 \
        --port "65254:30432@server:0" \
        --port "65290:30900@server:0" \
        --port "65291:30901@server:0" \
        --port "65263:30379@server:0" \
        --port "65280:30080@server:0" \
        --volume "/tmp/k3d-${CLUSTER_NAME}:/var/lib/rancher/k3s/storage@all" \
        --k3s-arg "--disable=traefik@server:0" \
        --kubeconfig-update-default \
        --kubeconfig-switch-context

    success "Cluster ${CLUSTER_NAME} created"

    # Wait for cluster to be ready
    info "Waiting for cluster to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=120s

    success "Cluster is ready"
}

delete_cluster() {
    if ! cluster_exists; then
        warn "Cluster ${CLUSTER_NAME} does not exist"
        return 0
    fi

    info "Deleting K3d cluster: ${CLUSTER_NAME}"
    k3d cluster delete "${CLUSTER_NAME}"
    success "Cluster ${CLUSTER_NAME} deleted"

    # Clean up volume directory
    if [ -d "/tmp/k3d-${CLUSTER_NAME}" ]; then
        info "Cleaning up volume directory..."
        rm -rf "/tmp/k3d-${CLUSTER_NAME}"
        success "Volume directory cleaned"
    fi
}

start_cluster() {
    if ! cluster_exists; then
        error "Cluster ${CLUSTER_NAME} does not exist. Create it first with: $0 create"
        exit 1
    fi

    info "Starting K3d cluster: ${CLUSTER_NAME}"
    k3d cluster start "${CLUSTER_NAME}"
    success "Cluster ${CLUSTER_NAME} started"

    # Switch context
    kubectl config use-context "k3d-${CLUSTER_NAME}"
    success "Switched to context: k3d-${CLUSTER_NAME}"
}

stop_cluster() {
    if ! cluster_exists; then
        warn "Cluster ${CLUSTER_NAME} does not exist"
        return 0
    fi

    info "Stopping K3d cluster: ${CLUSTER_NAME}"
    k3d cluster stop "${CLUSTER_NAME}"
    success "Cluster ${CLUSTER_NAME} stopped"
}

status_cluster() {
    info "K3d clusters:"
    k3d cluster list

    echo ""
    info "Kubernetes contexts:"
    kubectl config get-contexts | grep -E "NAME|k3d-${CLUSTER_NAME}" || true

    if cluster_exists; then
        echo ""
        info "Nodes in ${CLUSTER_NAME}:"
        kubectl get nodes --context "k3d-${CLUSTER_NAME}" 2>/dev/null || warn "Cluster not running"

        echo ""
        info "Namespaces:"
        kubectl get namespaces --context "k3d-${CLUSTER_NAME}" 2>/dev/null || warn "Cluster not running"
    fi
}

deploy_infrastructure() {
    if ! cluster_exists; then
        error "Cluster ${CLUSTER_NAME} does not exist. Create it first with: $0 create"
        exit 1
    fi

    info "Deploying infrastructure to ${CLUSTER_NAME}..."

    # Switch to cluster context
    kubectl config use-context "k3d-${CLUSTER_NAME}"

    # Create namespace
    kubectl create namespace mlb-data-platform --dry-run=client -o yaml | kubectl apply -f -
    success "Namespace created"

    # Deploy PostgreSQL
    info "Deploying PostgreSQL..."
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-config
  namespace: mlb-data-platform
data:
  POSTGRES_DB: mlb_games
  POSTGRES_USER: mlb_admin
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-secret
  namespace: mlb-data-platform
type: Opaque
stringData:
  POSTGRES_PASSWORD: mlb_admin_password
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc
  namespace: mlb-data-platform
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: mlb-data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15-alpine
        ports:
        - containerPort: 5432
        envFrom:
        - configMapRef:
            name: postgres-config
        - secretRef:
            name: postgres-secret
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
          subPath: postgres
      volumes:
      - name: postgres-storage
        persistentVolumeClaim:
          claimName: postgres-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: mlb-data-platform
spec:
  type: NodePort
  ports:
  - port: 5432
    targetPort: 5432
    nodePort: 30432
  selector:
    app: postgres
EOF
    success "PostgreSQL deployed"

    # Deploy Redis
    info "Deploying Redis..."
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: redis-config
  namespace: mlb-data-platform
data:
  redis.conf: |
    maxmemory 256mb
    maxmemory-policy allkeys-lru
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: mlb-data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        ports:
        - containerPort: 6379
        command:
        - redis-server
        - /etc/redis/redis.conf
        volumeMounts:
        - name: redis-config
          mountPath: /etc/redis
      volumes:
      - name: redis-config
        configMap:
          name: redis-config
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: mlb-data-platform
spec:
  type: NodePort
  ports:
  - port: 6379
    targetPort: 6379
    nodePort: 30379
  selector:
    app: redis
EOF
    success "Redis deployed"

    # Deploy MinIO
    info "Deploying MinIO..."
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: minio-secret
  namespace: mlb-data-platform
type: Opaque
stringData:
  MINIO_ROOT_USER: minioadmin
  MINIO_ROOT_PASSWORD: minioadmin
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-pvc
  namespace: mlb-data-platform
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: mlb-data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args:
        - server
        - /data
        - --console-address
        - ":9001"
        ports:
        - containerPort: 9000
        - containerPort: 9001
        envFrom:
        - secretRef:
            name: minio-secret
        volumeMounts:
        - name: minio-storage
          mountPath: /data
      volumes:
      - name: minio-storage
        persistentVolumeClaim:
          claimName: minio-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: mlb-data-platform
spec:
  type: NodePort
  ports:
  - name: api
    port: 9000
    targetPort: 9000
    nodePort: 30900
  - name: console
    port: 9001
    targetPort: 9001
    nodePort: 30901
  selector:
    app: minio
EOF
    success "MinIO deployed"

    info "Waiting for pods to be ready..."
    kubectl wait --for=condition=Ready pods --all -n mlb-data-platform --timeout=180s

    success "All infrastructure deployed and ready!"
    echo ""
    echo "Access services (MLB-specific 652XX ports):"
    echo "  PostgreSQL: localhost:65254 (user: mlb_admin, password: mlb_admin_password, db: mlb_games)"
    echo "  Redis:      localhost:65263"
    echo "  MinIO API:  http://localhost:65290 (user: minioadmin, password: minioadmin)"
    echo "  MinIO UI:   http://localhost:65291"
}

show_help() {
    cat <<EOF
K3d Cluster Management for MLB Stats API Data Platform

Usage: $0 <command>

Commands:
  create      Create a new K3d cluster (${CLUSTER_NAME})
  delete      Delete the K3d cluster
  start       Start the K3d cluster
  stop        Stop the K3d cluster
  status      Show cluster status
  deploy      Deploy infrastructure (PostgreSQL, Redis, MinIO)
  help        Show this help message

Context:
  The cluster context 'k3d-${CLUSTER_NAME}' will be automatically
  merged into ~/.kube/config and set as the current context.

Port Mappings (MLB-specific 652XX range):
  65254 -> PostgreSQL (652 + 54 for pg port 5432)
  65263 -> Redis      (652 + 63 for redis port 6379)
  65290 -> MinIO API  (652 + 90 for minio port 9000)
  65291 -> MinIO Console
  65280 -> Ingress    (652 + 80 for http port 8080)

Examples:
  # Create and deploy full stack
  $0 create
  $0 deploy

  # Check status
  $0 status

  # Stop when not in use
  $0 stop

  # Start again
  $0 start

  # Clean up completely
  $0 delete
EOF
}

# Main command handler
main() {
    case "${1:-help}" in
        create)
            check_dependencies
            create_cluster
            ;;
        delete)
            delete_cluster
            ;;
        start)
            start_cluster
            ;;
        stop)
            stop_cluster
            ;;
        status)
            status_cluster
            ;;
        deploy)
            deploy_infrastructure
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            error "Unknown command: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"
