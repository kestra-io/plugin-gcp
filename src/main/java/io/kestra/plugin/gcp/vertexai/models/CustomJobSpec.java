package io.kestra.plugin.gcp.vertexai.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Getter
@Builder
@Jacksonized
public class CustomJobSpec {
    @Schema(
        title = "Worker pool specs",
        description = "At least one worker pool; define machine type, replica count, and container/package."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private List<WorkerPoolSpec> workerPoolSpecs;


    @Schema(
        title = "Service account",
        description = "Run-as service account; submitters need act-as on this account. Defaults to Vertex AI Custom Code Service Agent."
    )
    private Property<String> serviceAccount;

    @Schema(
        title = "VPC network",
        description = "Full network name (projects/{project}/global/networks/{network}); requires Vertex AI VPC peering"
    )
    private Property<String> network;

    @Schema(
        title = "Tensorboard",
        description = "Tensorboard resource for logs, format projects/{project}/locations/{location}/tensorboards/{tensorboard}"
    )
    private Property<String> tensorboard;

    @Schema(
        title = "Enable web access",
        description = "Enable interactive shell access to training containers"
    )
    private Property<Boolean> enableWebAccess;

    @Schema(
        title = "Scheduling",
        description = "Optional scheduling options (timeout, restart, etc.)"
    )
    @PluginProperty(dynamic = false)
    private Scheduling scheduling;

    @Schema(
        title = "Base output directory",
        description = "GCS location for job outputs"
    )
    @PluginProperty(dynamic = false)
    private GcsDestination baseOutputDirectory;

    public  com.google.cloud.aiplatform.v1.CustomJobSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.aiplatform.v1.CustomJobSpec.Builder builder = com.google.cloud.aiplatform.v1.CustomJobSpec.newBuilder();

        this.workerPoolSpecs
            .stream()
            .map(throwFunction(workerPoolSpec -> workerPoolSpec.to(runContext)))
            .forEach(builder::addWorkerPoolSpecs);

        if (this.getServiceAccount() != null) {
            builder.setServiceAccount(runContext.render(this.getServiceAccount()).as(String.class).orElseThrow());
        }

        if (this.getNetwork() != null) {
            builder.setNetwork(runContext.render(this.getNetwork()).as(String.class).orElseThrow());
        }

        if (this.getTensorboard() != null) {
            builder.setTensorboard(runContext.render(this.getTensorboard()).as(String.class).orElseThrow());
        }

        if (this.getEnableWebAccess() != null) {
            builder.setEnableWebAccess(runContext.render(this.getEnableWebAccess()).as(Boolean.class).orElseThrow());
        }

        if (this.getScheduling() != null) {
            builder.setScheduling(this.getScheduling().to(runContext));
        }

        if (this.getBaseOutputDirectory() != null) {
            builder.setBaseOutputDirectory(this.getBaseOutputDirectory().to(runContext));
        }

        return builder.build();
    }
}
