# How to use the Google Cloud plugin

Set the `serviceAccount` property once using plugin defaults, or let the environment provide credentials via `GOOGLE_APPLICATION_CREDENTIALS` or the default service account.

## Authentication

All tasks must be authenticated for the Google Cloud Platform. You can do it in multiple ways:

- By setting the task `serviceAccount` property that must contain the service account JSON content. It can be handy to set this property globally by using [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) if your cluster accesses only one GCP project.
- By setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable on the nodes running Kestra. It must point to an application credentials file. Warning: it must be the same on all worker nodes and can cause some security concerns.
- If none is set, the default service account will be used.

You can also set authentication scopes. By default only one scope is used: `https://www.googleapis.com/auth/cloud-platform`.

## Common properties

Each task allows you to configure the GCP project identifier in the `projectId` property. If not set, the default project identifier will be used (the one returned by `ServiceOptions.getDefaultProjectId()`). Set this property globally by using [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) if your cluster accesses only one GCP project.

## Tasks

Tasks span the most commonly used GCP services. BigQuery covers queries, data loads from GCS or direct ingestion, table and dataset management, and a `Trigger` for polling query results. GCS handles uploads, downloads, copies, deletions, and file-arrival triggers. For messaging, Pub/Sub offers `Publish`, `Consume`, a polling `Trigger`, and a `RealtimeTrigger` — use `Trigger` for batch processing on a schedule and `RealtimeTrigger` for per-message executions.

For compute and data transformation, `dataproc` runs Spark workloads and `dataform` invokes transformation workflows. `firestore` covers document reads and writes, `vertexai` provides LLM completions and custom training jobs, and `function.HttpFunction` invokes Cloud Functions. Use `cli.GCloudCLI` for operations not covered by a dedicated task.
