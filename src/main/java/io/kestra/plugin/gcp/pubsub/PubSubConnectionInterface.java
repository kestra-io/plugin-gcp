package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.gcp.GcpInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface PubSubConnectionInterface extends GcpInterface {
    @Schema(
        title = "The Pub/Sub topic",
        description = "The Pub/Sub topic. It must be created before executing the task."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    Property<String> getTopic();
}
