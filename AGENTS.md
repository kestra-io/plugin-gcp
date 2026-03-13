# Kestra GCP Plugin

## What

Integrate Google Cloud Platform services with Kestra data workflows. Exposes 54 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Google Cloud, allowing orchestration of Google Cloud-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
