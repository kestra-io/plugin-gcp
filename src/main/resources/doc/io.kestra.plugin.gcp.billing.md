# How to use the Google Cloud Billing plugin

Google Cloud Billing tracks cloud usage and costs, manages billing accounts, and sets up spend budgets and alerts across Google Cloud Platform projects.

This sub-package provides tasks and triggers to automate FinOps processes, monitor budgets, query BigQuery billing exports, and react in real time to Pub/Sub budget alert notifications.

## Authentication

All tasks inherit GCP authentication properties from the common GCP plugin structure (`io.kestra.plugin.gcp.shared.AbstractTask`). You can authenticate using:
- A service account JSON key via the `serviceAccount` property.
- Service account impersonation via `impersonatedServiceAccount`.
- Environment-provided credentials (e.g. `GOOGLE_APPLICATION_CREDENTIALS`).
- Workload Identity or Compute Engine metadata IAM credentials when running inside GCP (GKE, GCE).

Scopes default to `https://www.googleapis.com/auth/cloud-platform`.

## Tasks

### ListBillingAccounts
Lists all Google Cloud Billing accounts accessible to the caller's credentials.
- `projectId`: Optional project ID used for quota attribution.
- Outputs a list of `BillingAccount` records containing `name`, `displayName`, `open` status, and `masterBillingAccount`.

### GetBudget
Retrieves metadata, monetary spend limits, and threshold alert rules for a specific Google Cloud budget.
- `budgetName`: Resource name in the format `billingAccounts/{billingAccountId}/budgets/{budgetId}`.
- Outputs `Budget` containing `specifiedAmount`, `currencyCode`, and `thresholdPercentRules`.

### BillingExportQuery
Generates and executes parameterized BigQuery SQL queries over standard Google Cloud Billing BigQuery export tables.
- `exportTable`: Fully qualified BigQuery table identifier (e.g. `my-project.billing_export.gcp_billing_export_v1_XXXXXX`).
- `dateFrom`: Start usage date or ISO-8601 timestamp (inclusive).
- `dateTo`: Optional end usage date or timestamp (exclusive).
- `groupBy`: List of column expressions (e.g. `service.description`, `project.id`). Expressions are automatically aliased to prevent duplicate or struct column naming issues in BigQuery.
- `includeCredits`: Whether to compute `net_cost` accounting for promotional, sustained-use, or negotiated credits.
- `fetchType`: BigQuery row output format (`FETCH`, `FETCH_ONE`, or `STORE`).

## Triggers

### BudgetAlertTrigger
An event-driven polling trigger that consumes Google Cloud Billing budget notifications from a Google Cloud Pub/Sub topic.
- `topic`: Pub/Sub topic to which Google Cloud Billing publishes budget alerts.
- `subscription`: Pub/Sub subscription name (auto-created if `autoCreateSubscription` is `true`).
- `thresholdRatio`: Minimum budget usage ratio required to trigger (e.g., `0.9` for 90%).
- `thresholdAmount`: Minimum absolute cost amount in budget currency units required to trigger.
- **Deduplication**: Automatically deduplicates incoming snapshot notifications using the namespace KV store to ensure the workflow fires only when a snapshot crosses a newly exceeded threshold, preventing repeated executions on periodic status snapshots.
- Outputs `trigger.budgetAlerts` (list of all alerts matching in the poll) and `trigger.budgetAlert` (the primary alert).
