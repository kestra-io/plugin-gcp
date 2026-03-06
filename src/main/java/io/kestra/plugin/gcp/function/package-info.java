@PluginSubGroup(
    title = "Google Cloud Function",
    description = "This sub-group of plugins contains tasks for triggering Google Cloud Function.\n" +
        "Cloud Run functions automatically manages and scales underlying infrastructure with the size of workload. Deploy your code and let Google run and scale it for you. ",
        categories = { PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.gcp.function;

import io.kestra.core.models.annotations.PluginSubGroup;