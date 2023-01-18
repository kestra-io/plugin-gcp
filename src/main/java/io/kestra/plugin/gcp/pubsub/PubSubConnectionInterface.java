package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.gcp.GcpInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface PubSubConnectionInterface extends GcpInterface {
    @Schema(
        title = "The PubSub topic",
        description = "The PubSub topic. It must be create before executing the task."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getTopic();
}
