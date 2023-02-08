### Authentication

All tasks must be authenticated for the Google Cloud Platform. You can do it in multiple ways:

- By setting the task `serviceAccount` property that must contain the service account JSON content. It can be handy to set this property globally by using [task defaults](../../docs/administrator-guide/configuration/others/#kestra-tasks-defaults) if your cluster access only one GCP project.
- By setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable on the nodes running Kestra. It must point to an application credentials file. **Warning:** it must be the same on all worker nodes and can cause some security concerns.
- If none is set, the default service account will be used.

You can also set authentication scopes. By default only one scope is used: `https://www.googleapis.com/auth/cloud-platform`.

### Common property

Each task allows configuring the GCP project identifier in the `projectId` property. If not set, the default project identifier will be used (the one returned by `ServiceOptions.getDefaultProjectId()`). It can be handy to set this property globally by using [task defaults](../../docs/administrator-guide/configuration/others/#kestra-tasks-defaults) if your cluster access only one GCP project. 