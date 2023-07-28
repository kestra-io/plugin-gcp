@PluginSubGroup(
    title = "GCP Secret Manager",
    description = "This sub-group of plugins contains tasks for accessing GCP Secret Manager.\n" +
        "Secret Manager is a secure and convenient storage system for API keys, passwords, certificates, and other sensitive data.",
    categories = { PluginSubGroup.PluginCategory.MISC, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.gcp.secrets;

import io.kestra.core.models.annotations.PluginSubGroup;