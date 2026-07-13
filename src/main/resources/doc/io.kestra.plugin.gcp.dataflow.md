# How to use the Google Cloud Dataflow plugin

Google Cloud Dataflow is a fully managed service for executing Apache Beam pipelines within the Google Cloud Platform ecosystem.

This sub-package provides tasks and a trigger to launch Classic and Flex templates, query status, cancel running jobs, and trigger workflows on job completions.

## Authentication

All tasks inherit GCP authentication properties from the common GCP plugin structure. You can authenticate using:
- A service account JSON key via the `serviceAccount` property.
- Environment-provided credentials (e.g. `GOOGLE_APPLICATION_CREDENTIALS` environment variable).
- Metadata-service-provided IAM credentials when running inside Google Cloud.

## Tasks

### LaunchTemplate
Launches a Classic Template Dataflow job from a GCS path.
- `jobName`: Unique name for the job.
- `gcsPath`: Cloud Storage path to the template.
- `parameters`: Key-value map of runtime parameters.
- `environment`: Runtime environment parameters like `tempLocation`, `zone`, `maxWorkers`.

### LaunchFlexTemplate
Launches a Flex Template Dataflow job from a container spec path.
- `jobName`: Unique name for the job.
- `containerSpecGcsPath`: GCS path to the Flex Template container spec JSON file.
- `parameters`: Key-value map of template parameters.
- `environment`: Runtime environment options.

### GetJob
Retrieves the execution state and metrics of a Dataflow job by job ID.

### CancelJob
Sends a drain or cancel request to a running Dataflow job.
- Set `drain` to `true` to allow streaming pipelines to process buffered data before stopping.

### WaitForJob
Polls job execution state until it enters a terminal state (JOB_STATE_DONE, JOB_STATE_FAILED, etc.). Fails the task if the job terminates unsuccessfully.

## Triggers

### Trigger
A polling trigger that evaluates Dataflow job states in a region and fires when a job matching the name prefix transitions to the target state.
