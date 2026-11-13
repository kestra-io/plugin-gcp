# How to use the Google Cloud BigQuery plugin

Google Cloud BigQuery is a fully-managed, serverless enterprise data warehouse that enables high-speed SQL analytics across large datasets.

This sub-package provides tasks to run queries, load data, export data, manage datasets and tables, take zero-cost table snapshots, stream rows with the Storage Write API, manage transfer configurations, and poll query results via triggers.

## Authentication

All tasks inherit GCP authentication properties from the common GCP plugin structure (`io.kestra.plugin.gcp.shared.AbstractTask`). You can authenticate using:
- A service account JSON key via the `serviceAccount` property (e.g. `serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"`).
- Service account impersonation via `impersonatedServiceAccount`.
- Environment-provided credentials (e.g. `GOOGLE_APPLICATION_CREDENTIALS`).
- Workload Identity or Compute Engine metadata IAM credentials when running inside Google Cloud (GKE, GCE).

Scopes default to `https://www.googleapis.com/auth/cloud-platform`.

## Tasks

### CreateSnapshot
Creates an instantaneous, zero-cost point-in-time table snapshot. Table snapshots charge $0 for unmodified bytes and support automatic TTL expiration.

- `sourceDataset`: Dataset ID containing the base table to snapshot.
- `sourceTable`: Base table name to snapshot.
- `destinationDataset`: Target dataset where the snapshot will be created; defaults to `sourceDataset`.
- `snapshotTable`: Name of the snapshot table to create.
- `expirationDuration`: TTL duration from now until the snapshot table automatically expires (e.g., `P7D`, `P30D`). Expired snapshots are automatically purged by BigQuery with no manual cleanup required.
- `snapshotTimestamp`: Point-in-time timestamp (BigQuery Time Travel) to capture historical table state (within the 7-day time travel window).
- `description`: Friendly description for the snapshot table.
- **Outputs**: `projectId`, `datasetId`, `tableId`, `snapshotTime`, `expirationTime`, `totalRows`, `totalBytes`, `jobId`.

```yaml
id: bigquery_snapshot_backup
namespace: company.data

tasks:
  - id: create_snapshot
    type: io.kestra.plugin.gcp.bigquery.CreateSnapshot
    projectId: my-project
    sourceDataset: analytics_prod
    sourceTable: customer_orders
    destinationDataset: analytics_snapshots
    snapshotTable: "customer_orders_snap_{{ execution.startDate | date('yyyyMMdd') }}"
    expirationDuration: P7D
    description: "Daily automated pre-ETL snapshot"
```

### Query
Executes a SQL query against BigQuery datasets. Supports fetching rows (`FETCH_ONE`, `FETCH`, `STORE`), writing results to destination tables, parameterization, and cost estimation.

### Load / LoadFromGcs
Loads data from local files or Google Cloud Storage buckets into BigQuery tables with automatic schema detection and format handling (CSV, JSON, Avro, Parquet, ORC).

### ExtractToGcs
Exports BigQuery table data to Google Cloud Storage files.

### Copy
Copies tables, clones tables, or creates snapshots between datasets or tables.

### StorageWrite
Streams rows with high throughput using the BigQuery Storage Write API.

### TableMetadata
Retrieves metadata, schema, and statistics for a BigQuery table or partition.

### CreateTable / DeleteTable / UpdateTable
Manages BigQuery table lifecycles, schemas, partitions, clustering, and CMEK encryption.

### CreateDataset / DeleteDataset / UpdateDataset
Manages BigQuery dataset lifecycles, access control lists (ACLs), and default expiration policies.

### RunTransferConfig
Triggers and monitors BigQuery Data Transfer Service (DTS) scheduled runs.

## Triggers

### Trigger
Polls BigQuery with a SQL query at a regular interval and triggers workflow executions when rows are returned.
