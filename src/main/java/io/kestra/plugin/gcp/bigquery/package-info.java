@PluginSubGroup(
    title = "BigQuery",
    description = "This sub-group of plugins contains tasks for accessing Google Cloud BigQuery.\n" +
        "BigQuery is a completely serverless and cost-effective enterprise data warehouse. ",
    categories = { PluginSubGroup.PluginCategory.DATABASE, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.gcp.bigquery;

import io.kestra.core.models.annotations.PluginSubGroup;