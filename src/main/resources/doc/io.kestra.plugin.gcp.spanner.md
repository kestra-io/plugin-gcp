# How to use the Google Cloud Spanner plugin

Google Cloud Spanner is a fully managed, mission-critical relational database service that offers transactional consistency at global scale, schemas, SQL querying, and automatic synchronous replication.

This sub-package provides tasks and a trigger to manage databases, run SQL queries, execute DML and DDL operations, perform atomic batch modifications, and trigger workflows on database change streams.

## Authentication

All tasks inherit GCP authentication properties from the common GCP plugin structure. You can authenticate using:
- A service account JSON key via the `serviceAccount` property.
- Environment-provided credentials (e.g. `GOOGLE_APPLICATION_CREDENTIALS` environment variable).
- Metadata-service-provided IAM credentials when running inside Google Cloud (GKE, GCE, etc.).

## Local Testing with Emulator

To facilitate local development and unit testing without cloud cost, set the `emulatorHost` property (e.g., `localhost:9010`) to route task actions directly to a local Spanner emulator in plaintext and with no credentials.

## Tasks

### CreateDatabase
Creates a new Spanner database under the specified instance.
- `databaseId`: ID of the database to create.
- `extraDdl`: Optional list of DDL statements to run immediately upon database creation (e.g. creating tables, indexes, or change streams).

### DeleteDatabase
Drops a Spanner database and all of its schema, tables, and data permanently.

### Query
Executes a SQL `SELECT` query against the Spanner database.
- `sql`: The SQL query string to run.
- `parameters`: Query parameters mapped dynamically to Spanner parameter types.
- `fetchType`: Exposes rows downstream. Use `FETCH_ONE` for single row, `FETCH` for list in output, or `STORE` (recommended) to dump matching rows directly to ion-formatted storage to prevent memory pressure.

### Execute
Executes a single DML statement (INSERT, UPDATE, DELETE) or DDL statement.
- `sql`: The SQL statement to run.
- `parameters`: Parameters mapping to Spanner types (only applicable to DML).
- DML statements are wrapped in read-write transactions; DDL statements are executed asynchronously via Spanner database admin client operations.

### BatchDml
Executes multiple DML statements atomically in a single read-write transaction.
- `statements`: A list of DML statements to run.
- Returns a list of individual affected row counts and the sum of affected rows.

## Triggers

### Trigger
A polling trigger that queries a Spanner Change Stream at the configured `interval` and triggers workflow executions when modifications (inserts, updates, deletes) are detected in the stream.
- `changeStreamName`: The name of the Change Stream to poll.
- `lookback`: The lookback window duration applied to poll stream records. Defaults to the trigger polling interval.
- **Two-Step Querying**: Automatically handles Spanner Change Stream partition token discovery and query iteration across partition boundaries to retrieve data change records.
- Exposes query results under the `trigger.rows`, `trigger.rowCount`, and `trigger.changeCount` variables.
