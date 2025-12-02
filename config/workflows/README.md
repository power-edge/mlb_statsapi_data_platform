# MLB Data Platform - Argo Workflows

This directory contains Argo Workflows for orchestrating the MLB data platform's ingestion and transformation pipelines.

## Overview

The workflows implement the daily data pipeline:

```
Season Ingestion → Season Transform → Schedule Ingestion → Schedule Transform → Game Ingestion (parallel)
```

## Workflow Files

### Individual Workflows

#### `workflow-season-transform.yaml`
Transforms raw season data from PostgreSQL to normalized tables.

**Inputs:**
- `sport-ids`: JSON array of sport IDs (default: `[1]` for MLB)
- `export-to-delta`: Export to Delta Lake (default: `false`)
- `delta-path`: S3/MinIO path for Delta export

**Outputs:**
- Normalized season data in PostgreSQL
- Optional Delta Lake export

**Usage:**
```bash
argo submit config/workflows/workflow-season-transform.yaml \
  --namespace mlb-data-platform \
  --parameter sport-ids='[1]' \
  --watch
```

---

#### `workflow-schedule-transform.yaml`
Transforms raw schedule data, extracting games from nested arrays.

**Inputs:**
- `start-date`: Filter start date (YYYY-MM-DD, default: today)
- `end-date`: Filter end date (YYYY-MM-DD, default: today)
- `sport-ids`: JSON array of sport IDs (default: `[1]`)
- `export-to-delta`: Export to Delta Lake (default: `false`)
- `delta-path`: S3/MinIO path for Delta export

**Outputs:**
- `schedule.schedule_metadata` - Top-level schedule info
- `schedule.games` - Individual games extracted from nested arrays

**Usage:**
```bash
argo submit config/workflows/workflow-schedule-transform.yaml \
  --namespace mlb-data-platform \
  --parameter start-date='2024-07-01' \
  --parameter end-date='2024-07-31' \
  --watch
```

---

#### `workflow-daily-pipeline.yaml`
Orchestrates the full daily pipeline:
1. Ingest season data
2. Transform season data
3. Ingest schedule data
4. Transform schedule data
5. For each game in schedule, ingest game data (parallel)

**Inputs:**
- `date`: Date to process (YYYY-MM-DD, default: today)
- `sport-ids`: JSON array of sport IDs (default: `[1]`)
- `export-to-delta`: Export to Delta Lake (default: `false`)

**Usage:**
```bash
argo submit config/workflows/workflow-daily-pipeline.yaml \
  --namespace mlb-data-platform \
  --parameter date='2024-07-04' \
  --watch
```

---

#### `cronworkflow-daily.yaml`
Schedules the daily pipeline to run automatically at midnight UTC.

**Schedule:** `0 0 * * *` (daily at 00:00 UTC)

**Features:**
- Concurrency policy: `Forbid` (skip new run if previous still running)
- History limits: 7 successful, 3 failed
- TTL: 24 hours for successful runs, 48 hours for failures

**Usage:**
```bash
# Create the CronWorkflow
kubectl apply -f config/workflows/cronworkflow-daily.yaml -n mlb-data-platform

# List CronWorkflows
argo cron list -n mlb-data-platform

# Suspend/resume CronWorkflow
kubectl patch cronworkflow mlb-daily-pipeline -n mlb-data-platform \
  -p '{"spec":{"suspend":true}}'  # suspend
kubectl patch cronworkflow mlb-daily-pipeline -n mlb-data-platform \
  -p '{"spec":{"suspend":false}}' # resume

# Delete CronWorkflow
kubectl delete cronworkflow mlb-daily-pipeline -n mlb-data-platform
```

---

## Prerequisites

### 1. Kubernetes Cluster
Ensure you have a Kubernetes cluster running with:
- Argo Workflows installed
- Spark Operator installed (for PySpark jobs)
- PostgreSQL accessible
- MinIO/S3 accessible

### 2. Namespace and ServiceAccount
```bash
kubectl create namespace mlb-data-platform

kubectl create serviceaccount mlb-data-platform-sa -n mlb-data-platform

# Grant necessary RBAC permissions
kubectl create rolebinding mlb-data-platform-sa-admin \
  --clusterrole=admin \
  --serviceaccount=mlb-data-platform:mlb-data-platform-sa \
  -n mlb-data-platform
```

### 3. Secrets
Create PostgreSQL credentials secret:
```bash
kubectl create secret generic postgres-credentials \
  --from-literal=host=postgresql.mlb-data-platform.svc.cluster.local \
  --from-literal=username=mlb_admin \
  --from-literal=password=your-password-here \
  -n mlb-data-platform
```

### 4. ConfigMaps
Create job configs ConfigMap:
```bash
kubectl create configmap mlb-job-configs \
  --from-file=config/jobs/ \
  -n mlb-data-platform
```

### 5. Container Images
Build and push the required images:

**Ingestion image:**
```bash
docker build -f docker/Dockerfile.ingestion -t mlb-data-platform/ingestion:latest .
docker push mlb-data-platform/ingestion:latest
```

**Spark image:**
```bash
docker build -f docker/Dockerfile.spark -t mlb-data-platform/spark:latest .
docker push mlb-data-platform/spark:latest
```

---

## Local Testing

### Test Season Transformation
```bash
# Ensure PostgreSQL is running with raw data
docker compose up -d postgres

# Run Spark job locally
POSTGRES_PASSWORD=mlb_dev_password \
SPORT_IDS='[1]' \
EXPORT_TO_DELTA=false \
spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  examples/spark_jobs/transform_season.py
```

### Test Schedule Transformation
```bash
# Run Spark job locally
POSTGRES_PASSWORD=mlb_dev_password \
START_DATE=2024-07-01 \
END_DATE=2024-07-31 \
SPORT_IDS='[1]' \
EXPORT_TO_DELTA=false \
spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  examples/spark_jobs/transform_schedule.py
```

---

## Monitoring Workflows

### List all workflows
```bash
argo list -n mlb-data-platform
```

### Watch a workflow
```bash
argo watch <workflow-name> -n mlb-data-platform
```

### Get workflow logs
```bash
argo logs <workflow-name> -n mlb-data-platform -f
```

### Get workflow status
```bash
argo get <workflow-name> -n mlb-data-platform
```

### Delete a workflow
```bash
argo delete <workflow-name> -n mlb-data-platform
```

---

## Troubleshooting

### Workflow stuck in "Pending"
Check pod events:
```bash
kubectl get pods -n mlb-data-platform | grep <workflow-name>
kubectl describe pod <pod-name> -n mlb-data-platform
```

Common issues:
- Missing secrets
- Image pull errors
- Insufficient resources
- RBAC permissions

### Spark job failures
Check Spark driver logs:
```bash
kubectl logs -l spark-role=driver -n mlb-data-platform
```

Common issues:
- PostgreSQL connection errors (check secrets)
- Out of memory (increase driver/executor memory)
- Missing JDBC driver (ensure postgresql JAR is available)

### Database connection errors
Test PostgreSQL connectivity:
```bash
kubectl run -it --rm --restart=Never postgres-client \
  --image=postgres:15-alpine \
  --namespace=mlb-data-platform \
  -- psql -h postgresql -U mlb_admin -d mlb_games
```

---

## Production Deployment

### 1. Update workflow parameters
Edit `cronworkflow-daily.yaml` to set production values:
```yaml
- name: export-to-delta
  value: "true"  # Enable Delta Lake export
- name: delta-path
  value: "s3://your-production-bucket/mlb-data/..."
```

### 2. Configure resource limits
Add resource requests/limits to workflow templates:
```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2"
  limits:
    memory: "8Gi"
    cpu: "4"
```

### 3. Enable monitoring
Add Prometheus metrics and alerting:
```yaml
# Example: Alert on workflow failures
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: argo-workflow-alerts
  namespace: mlb-data-platform
spec:
  groups:
    - name: workflows
      rules:
        - alert: WorkflowFailed
          expr: argo_workflow_status{status="Failed"} > 0
          for: 5m
          annotations:
            summary: "Workflow {{ $labels.name }} failed"
```

### 4. Set up notifications
Configure Argo to send notifications on workflow completion:
```bash
kubectl create secret generic slack-webhook \
  --from-literal=url=https://hooks.slack.com/services/YOUR/WEBHOOK/URL \
  -n mlb-data-platform
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     CronWorkflow (Daily)                    │
│                      (midnight UTC)                         │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Daily Pipeline Workflow                   │
└─────────────────────────────────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
    ┌─────────┐         ┌─────────┐        ┌─────────┐
    │ Season  │         │Schedule │        │  Game   │
    │Pipeline │         │Pipeline │        │Pipeline │
    └─────────┘         └─────────┘        └─────────┘
         │                   │                   │
    ┌────┴────┐         ┌────┴────┐         ┌────┴────┐
    ▼         ▼         ▼         ▼         ▼         ▼
 Ingest   Transform  Ingest   Transform  Ingest   Transform
    │         │         │         │         │         │
    ▼         ▼         ▼         ▼         ▼         ▼
┌──────────────────────────────────────────────────────────┐
│                      PostgreSQL                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │   RAW    │  │NORMALIZED│  │  CACHE   │              │
│  │  JSONB   │  │  TABLES  │  │  (Redis) │              │
│  └──────────┘  └──────────┘  └──────────┘              │
└──────────────────────────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Delta Lake /   │
                    │     Parquet     │
                    │   (S3/MinIO)    │
                    └─────────────────┘
```

---

## Next Steps

1. **Deploy workflows**: Submit workflows to your Kubernetes cluster
2. **Monitor execution**: Use Argo UI to track workflow progress
3. **Scale resources**: Adjust Spark executor counts and memory based on data volume
4. **Optimize performance**: Tune Spark configurations for your workload
5. **Add game transformation**: Implement game data transformation (currently placeholder)
6. **Enable Delta Lake**: Configure S3/MinIO and enable Delta Lake exports
7. **Set up alerts**: Configure Prometheus alerting for failures

---

## References

- [Argo Workflows Documentation](https://argoproj.github.io/argo-workflows/)
- [Spark Operator Documentation](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Delta Lake Documentation](https://docs.delta.io/)
