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
        title = "The spec of the worker pools including machine type and Docker image.",
        description = " All worker pools except the first one are optional and can be skipped"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private List<WorkerPoolSpec> workerPoolSpecs;


    @Schema(
        title = "Specifies the service account for workload run-as account.",
        description = "       Users submitting jobs must have act-as permission on this run-as account.\n" +
            "       If unspecified, the [Vertex AI Custom Code Service\n" +
            "       Agent](https://cloud.google.com/vertex-ai/docs/general/access-control#service-agents)\n" +
            "       for the CustomJob's project is used."
    )
    private Property<String> serviceAccount;

    @Schema(
        title = "The full name of the Compute Engine [network](https://cloud.google.com/compute/docs/networks-and-firewalls#networks) to which the Job should be peered.",
        description = "For example, `projects/12345/global/networks/myVPC`.\n" +
            "Format is of the form `projects/{project}/global/networks/{network}`. " +
            "Where {project} is a project number, as in `12345`, and {network} is a network name.\n" +
            "To specify this field, you must have already [configured VPC Network Peering for Vertex AI](https://cloud.google.com/vertex-ai/docs/general/vpc-peering).\n" +
            "If this field is left unspecified, the job is not peered with any network."
    )
    private Property<String> network;

    @Schema(
        title = "The name of a Vertex AI Tensorboard resource to which this CustomJob",
        description = "will upload Tensorboard logs. Format:`projects/{project}/locations/{location}/tensorboards/{tensorboard}`"
    )
    @PluginProperty(dynamic = true)
    private String tensorboard;

    @Schema(
        title = "Whether you want Vertex AI to enable [interactive shell access](https://cloud.google.com/vertex-ai/docs/training/monitor-debug-interactive-shell) to training containers."
    )
    private Property<Boolean> enableWebAccess;

    @Schema(
        title = "Scheduling options for a CustomJob."
    )
    @PluginProperty(dynamic = false)
    private Scheduling scheduling;

    @Schema(
        title = "The Cloud Storage location to store the output of this job."
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
            builder.setTensorboard(runContext.render(this.getTensorboard()));
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
