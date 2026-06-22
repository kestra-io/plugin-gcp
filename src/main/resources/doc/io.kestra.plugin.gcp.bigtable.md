# Google Cloud Bigtable

Google Cloud Bigtable is a fully managed, petabyte-scale, NoSQL database service for large analytical and operational workloads.

This sub-package provides tasks and a trigger to manage tables, write mutations, query rows, and trigger workflow executions when new rows are detected.

## Authentication

All tasks inherit GCP authentication properties from the common GCP plugin structure. You can authenticate using:
- A service account JSON key via the `serviceAccount` property.
- Environment-provided credentials (e.g. `GOOGLE_APPLICATION_CREDENTIALS` environment variable).
- Metadata-service-provided IAM credentials when running inside Google Cloud (GKE, GCE, etc.).

## Local Testing with Emulator

To facilitate local development and unit testing without cloud cost, set the `emulatorHost` property (e.g., `localhost:8086`) to route task actions directly to a local Bigtable emulator in plaintext and with no credentials.

## Tasks

### CreateTable
Creates a new Bigtable table with the given ID and pre-defined column families.
- `tableId`: ID of the table to create.
- `columnFamilies`: List of column family names to initialize on the table.

### WriteRows
Applies mutation batches (set cell, delete cell) to a table.
- `tableId`: Destination table ID.
- `columnFamily`: Default column family name for the mutations.
- `rows`: List of row mutation inputs, each specifying `rowKey` and a map of cell qualifier-value pairs.

### ReadRows
Reads rows from a table.
- `tableId`: Table ID to scan.
- `rowKeyPrefix`, `rowKeyStart`/`rowKeyEnd`: Scopes the row keys scanned.
- `fetchType`: Exposes rows downstream. Use `FETCH_ONE` for single row, `FETCH` for list in output, or `STORE` (recommended) to dump matching rows directly to ion-formatted storage to prevent memory pressure.

### DeleteRows
Deletes rows by key prefix, range, or a manual list of exact row keys. To prevent memory issues, deletions are automatically batched and submitted in groups of 1000.

### DeleteTable
Deletes a table and all of its cells permanently.

## Triggers

### Trigger
A polling trigger that queries a Bigtable table at a configured `interval` (e.g., `PT5M`) and executes the flow when new rows matching the query filters are detected.
- `maxRows`: Caps the number of rows pulled into memory (default `1000`) to prevent OOM errors.
- Exposes query results under the `trigger.rows` and `trigger.rowCount` variables.
