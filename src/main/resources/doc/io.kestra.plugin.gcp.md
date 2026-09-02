# How to use the Google Cloud plugin

Set the `serviceAccount` property on each task, or let the environment provide credentials via `GOOGLE_APPLICATION_CREDENTIALS` or the default service account.

## Authentication

All tasks must be authenticated for the Google Cloud Platform. You can do it in multiple ways:

- By setting the task `serviceAccount` property that must contain the service account JSON content.
- By setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable on the nodes running Kestra. It must point to an application credentials file. Warning: it must be the same on all worker nodes and can cause some security concerns.
- If none is set, the default service account will be used.

You can also set authentication scopes. By default only one scope is used: `https://www.googleapis.com/auth/cloud-platform`.

## Common properties

Each task allows you to configure the GCP project identifier in the `projectId` property. If not set, the default project identifier will be used (the one returned by `ServiceOptions.getDefaultProjectId()`).

## Tasks

Tasks span the most commonly used GCP services. BigQuery covers queries, data loads from GCS or direct ingestion, table and dataset management, a `Trigger` for polling query results, and `RunTransferConfig` to start (or re-attach to) a BigQuery Data Transfer Service run by its `transferConfigName` resource name. GCS handles uploads, downloads, copies, deletions, and file-arrival triggers. For messaging, Pub/Sub offers `Publish`, `Consume`, a polling `Trigger`, and a `RealtimeTrigger` — use `Trigger` for batch processing on a schedule and `RealtimeTrigger` for per-message executions.

For compute and data transformation, `dataproc` runs Spark workloads, `dataform` invokes transformation workflows, and `compute` manages Compute Engine VM lifecycle (`Create`, `Start`, `Stop`, `Delete`). `firestore` covers document reads and writes, `vertexai` provides LLM completions and custom training jobs, and `function.HttpFunction` invokes Cloud Functions. Use `cli.GCloudCLI` for operations not covered by a dedicated task.

## Run a BigQuery Data Transfer Service config

`RunTransferConfig` starts a manual run of an existing BigQuery Data Transfer Service transfer config by its resource name and waits for it to reach a terminal state. A single task covers both scheduled queries and other DTS transfer types.

The only required property is `transferConfigName`:

```yaml
id: bigquery_data_transfer
namespace: company.team

tasks:
  - id: trigger_transfer
    type: io.kestra.plugin.gcp.bigquery.RunTransferConfig
    projectId: "{{ secret('GCP_PROJECT_ID') }}"
    transferConfigName: projects/my-project/locations/us/transferConfigs/615123456789012345
```

Before starting a new run, the task lists any `PENDING` or `RUNNING` runs for the config and adopts the most recent one, so a retried execution never fires a duplicate transfer. Set reattach: false to always start a new run. Use reattachMaxAge to treat in-flight runs older than a given duration as stale and start a fresh one instead.

maxDuration (default `PT1H`) is a wall-clock deadline, not an iteration count. A run that stays `PENDING` past the deadline times out with a message naming the run and its last observed state. pollInterval defaults to PT15S.

On Enterprise Edition, when the run completes successfully, the task automatically emits the destination as a data-lineage asset — a Table when the config targets a non-templated destination table, a Dataset otherwise. No additional configuration is required.

Killing or stopping this task cancels only the local wait loop. The transfer continues on the Google side until it reaches a terminal state, since the Data Transfer Service API has no run-cancellation endpoint.