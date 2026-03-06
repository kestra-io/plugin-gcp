@PluginSubGroup(
    title = "BigQuery",
    description = "This sub-group of plugins contains tasks for accessing Google Cloud BigQuery.\n" +
        "BigQuery is a completely serverless and cost-effective enterprise data warehouse. ",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA }
)
package io.kestra.plugin.gcp.bigquery;

import io.kestra.core.models.annotations.PluginSubGroup;