package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.gcp.GcpInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface PubSubConnectionInterface extends GcpInterface {
    @Schema(
        title = "The Pub/Sub topic",
        description = "The Pub/Sub topic. It must be created before executing the task."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<String> getTopic();
}
