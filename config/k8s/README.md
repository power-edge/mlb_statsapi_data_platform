# Kubernetes Deployment Guide

This directory contains Kubernetes manifests for deploying the MLB Data Platform CronWorkflows.

## Prerequisites

1. **Kubernetes cluster** with Argo Workflows installed
2. **PostgreSQL database** accessible from the cluster
3. **Container registry** with `mlb-data-platform/ingestion:latest` image

## Quick Start

```bash
# 1. Create namespace and RBAC
kubectl apply -f config/k8s/namespace.yaml
kubectl apply -f config/k8s/serviceaccount.yaml

# 2. Create secrets (edit the template first!)
# IMPORTANT: Replace REPLACE_WITH_ACTUAL_PASSWORD in secrets-template.yaml
kubectl apply -f config/k8s/secrets-template.yaml

# 3. Deploy CronWorkflows
kubectl apply -f config/workflows/cronworkflow-pipeline-daily.yaml
kubectl apply -f config/workflows/cronworkflow-pipeline-live.yaml

# 4. Verify deployment
kubectl get cronworkflows -n mlb-data-platform
```

## CronWorkflows

### Daily Pipeline (`mlb-pipeline-daily`)
- **Schedule**: 6 AM UTC daily (after games finish)
- **Purpose**: Fetch all games from the previous day
- **Duration**: ~5-10 minutes depending on game count

### Live Pipeline (`mlb-pipeline-live`)
- **Schedule**: Every 30 minutes during game hours (March-November)
- **Purpose**: Capture live game updates
- **Duration**: ~2-5 minutes

## Monitoring

```bash
# List all CronWorkflows
kubectl get cronworkflows -n mlb-data-platform

# View recent workflow runs
argo list -n mlb-data-platform --since 24h

# Check specific workflow logs
argo logs <workflow-name> -n mlb-data-platform

# Suspend a CronWorkflow (off-season)
kubectl patch cronworkflow mlb-pipeline-live -n mlb-data-platform \
  --type=merge -p '{"spec":{"suspend":true}}'

# Resume a CronWorkflow
kubectl patch cronworkflow mlb-pipeline-live -n mlb-data-platform \
  --type=merge -p '{"spec":{"suspend":false}}'
```

## Manual Triggers

```bash
# Manually trigger daily pipeline
argo submit config/workflows/workflow-pipeline-daily.yaml \
  -n mlb-data-platform \
  --watch

# Trigger backfill for specific date range
argo submit config/workflows/workflow-pipeline-backfill.yaml \
  -n mlb-data-platform \
  -p start=2024-07-01 \
  -p end=2024-07-31 \
  --watch
```

## Troubleshooting

### Workflow Failures
```bash
# Check workflow status
argo get <workflow-name> -n mlb-data-platform

# View detailed logs
argo logs <workflow-name> -n mlb-data-platform --follow

# Resubmit failed workflow
argo resubmit <workflow-name> -n mlb-data-platform
```

### Database Connection Issues
```bash
# Test database connectivity from within cluster
kubectl run -it --rm postgres-test \
  --image=postgres:15-alpine \
  -n mlb-data-platform \
  -- psql -h postgresql -U mlb_admin -d mlb_games -c "SELECT 1"
```

### Image Pull Issues
```bash
# Check if image exists
kubectl describe pod <pod-name> -n mlb-data-platform

# Verify image pull secret (if using private registry)
kubectl get secrets -n mlb-data-platform
```

## Season Schedule

During the MLB season (March-November):
- **Daily pipeline**: Runs every day at 6 AM UTC
- **Live pipeline**: Runs every 30 minutes from 4 PM - 6 AM UTC

During off-season (December-February):
- Consider suspending the live pipeline to save resources
- Daily pipeline can remain active for any off-season games
