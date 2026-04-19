# Kestra GCP Plugin

## What

- Provides plugin components under `io.kestra.plugin.gcp`.
- Includes classes such as `CredentialService`, `StoreFetchDestinationValidator`, `LoadCsvValidator`, `StoreFetchValidator`.

## Why

- What user problem does this solve? Teams need to execute tasks and orchestrate workflows across Google Cloud Platform services. Centralize authentication and resource management for seamless GCP integration from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Google Cloud steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Google Cloud.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `gcp`

### Key Plugin Classes

- `io.kestra.plugin.gcp.bigquery.Copy`
- `io.kestra.plugin.gcp.bigquery.CopyPartitions`
- `io.kestra.plugin.gcp.bigquery.CreateDataset`
- `io.kestra.plugin.gcp.bigquery.CreateTable`
- `io.kestra.plugin.gcp.bigquery.DeleteDataset`
- `io.kestra.plugin.gcp.bigquery.DeletePartitions`
- `io.kestra.plugin.gcp.bigquery.DeleteTable`
- `io.kestra.plugin.gcp.bigquery.ExtractToGcs`
- `io.kestra.plugin.gcp.bigquery.Load`
- `io.kestra.plugin.gcp.bigquery.LoadFromGcs`
- `io.kestra.plugin.gcp.bigquery.Query`
- `io.kestra.plugin.gcp.bigquery.StorageWrite`
- `io.kestra.plugin.gcp.bigquery.TableMetadata`
- `io.kestra.plugin.gcp.bigquery.Trigger`
- `io.kestra.plugin.gcp.bigquery.UpdateDataset`
- `io.kestra.plugin.gcp.bigquery.UpdateTable`
- `io.kestra.plugin.gcp.cli.GCloudCLI`
- `io.kestra.plugin.gcp.dataform.InvokeWorkflow`
- `io.kestra.plugin.gcp.dataproc.batches.PySparkSubmit`
- `io.kestra.plugin.gcp.dataproc.batches.RSparkSubmit`
- `io.kestra.plugin.gcp.dataproc.batches.SparkSqlSubmit`
- `io.kestra.plugin.gcp.dataproc.batches.SparkSubmit`
- `io.kestra.plugin.gcp.dataproc.clusters.Create`
- `io.kestra.plugin.gcp.dataproc.clusters.Delete`
- `io.kestra.plugin.gcp.firestore.Delete`
- `io.kestra.plugin.gcp.firestore.Get`
- `io.kestra.plugin.gcp.firestore.Query`
- `io.kestra.plugin.gcp.firestore.Set`
- `io.kestra.plugin.gcp.function.HttpFunction`
- `io.kestra.plugin.gcp.gcs.Compose`
- `io.kestra.plugin.gcp.gcs.Copy`
- `io.kestra.plugin.gcp.gcs.CreateBucket`
- `io.kestra.plugin.gcp.gcs.CreateBucketIamPolicy`
- `io.kestra.plugin.gcp.gcs.Delete`
- `io.kestra.plugin.gcp.gcs.DeleteBucket`
- `io.kestra.plugin.gcp.gcs.DeleteList`
- `io.kestra.plugin.gcp.gcs.Download`
- `io.kestra.plugin.gcp.gcs.Downloads`
- `io.kestra.plugin.gcp.gcs.List`
- `io.kestra.plugin.gcp.gcs.Trigger`
- `io.kestra.plugin.gcp.gcs.UpdateBucket`
- `io.kestra.plugin.gcp.gcs.Upload`
- `io.kestra.plugin.gcp.gke.ClusterMetadata`
- `io.kestra.plugin.gcp.monitoring.Push`
- `io.kestra.plugin.gcp.monitoring.Query`
- `io.kestra.plugin.gcp.monitoring.Trigger`
- `io.kestra.plugin.gcp.pubsub.Consume`
- `io.kestra.plugin.gcp.pubsub.Publish`
- `io.kestra.plugin.gcp.pubsub.RealtimeTrigger`
- `io.kestra.plugin.gcp.pubsub.Trigger`
- `io.kestra.plugin.gcp.vertexai.ChatCompletion`
- `io.kestra.plugin.gcp.vertexai.CustomJob`
- `io.kestra.plugin.gcp.vertexai.MultimodalCompletion`
- `io.kestra.plugin.gcp.vertexai.TextCompletion`

### Project Structure

```
plugin-gcp/
├── src/main/java/io/kestra/plugin/gcp/vertexai/
├── src/test/java/io/kestra/plugin/gcp/vertexai/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
