@PluginSubGroup(
    title = "Pub/Sub",
    description = "This sub-group of plugins contains tasks for accessing Google Cloud Pub/Sub.\n" +
        "Pub/Sub is an asynchronous and scalable messaging service that decouples services producing messages from services processing those messages.",
    categories = { PluginSubGroup.PluginCategory.MESSAGING, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.annotations.PluginSubGroup;