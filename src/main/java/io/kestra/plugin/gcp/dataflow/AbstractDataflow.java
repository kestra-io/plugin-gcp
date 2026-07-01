package io.kestra.plugin.gcp.dataflow;

import com.google.api.services.dataflow.Dataflow;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
public abstract class AbstractDataflow extends AbstractTask implements DataflowConnectionInterface {

    @NotNull
    @Schema(title = "The regional endpoint (e.g. us-central1)")
    @PluginProperty(group = "connection")
    protected Property<String> location;

    protected Dataflow dataflowClient(RunContext runContext) throws Exception {
        return DataflowService.dataflowClient(runContext, this);
    }
}
