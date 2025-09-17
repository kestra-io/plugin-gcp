package io.kestra.plugin.gcp.dataform;

import com.google.cloud.dataform.v1.CreateWorkflowInvocationRequest;
import com.google.cloud.dataform.v1.DataformClient;
import com.google.cloud.dataform.v1.WorkflowInvocation;

import io.kestra.core.runners.RunContext;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.Builder;

@SuperBuilder
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode(callSuper = true)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Trigger a Dataform workflow in GCP",
            code = """
                id: invoke-dataform
                namespace: my.company

                tasks:
                  - id: transform
                    type: io.kestra.plugin.gcp.dataform.InvokeWorkflow
                    wait: true
                    projectId: "my-gcp-project"
                    location: "europe-west1"
                    repositoryId: "my-repo"
                    workflowConfigId: "my-config"
                """
        )
    }
)
@Schema(
    title = "Invoke a Dataform workflow in GCP."
)
public class InvokeWorkflow extends AbstractDataForm implements RunnableTask<InvokeWorkflow.Output> {

    @NotNull
    @Schema(title = "The workflow config ID to invoke.")
    @PluginProperty
    protected Property<String> workflowConfigId;

    @Builder.Default
    @Schema(title = "Whether to wait for the workflow to finish, default value is true.")
    @PluginProperty
    protected Boolean wait = true;;

   @Override
public Output run(RunContext runContext) throws Exception {
    try (DataformClient client = this.dataformClient(runContext)) {
        String parent = buildRepositoryPath(runContext);

        // Render the config ID
        String configPath = String.format("%s/workflowConfigs/%s",
            parent,
            runContext.render(this.workflowConfigId).as(String.class).orElseThrow()
        );

        // Build the workflow invocation with the proper field name
        WorkflowInvocation requestBody = WorkflowInvocation.newBuilder()
            .setWorkflowConfig(configPath)
            .build();

        CreateWorkflowInvocationRequest request = CreateWorkflowInvocationRequest.newBuilder()
            .setParent(parent)
            .setWorkflowInvocation(requestBody)
            .build();

        WorkflowInvocation response = client.createWorkflowInvocation(request);
        String invocationName = response.getName();

        if (wait) {
                WorkflowInvocation current;
                do {
                    Thread.sleep(1000);
                    current = client.getWorkflowInvocation(invocationName);
                } while (current.getState() == WorkflowInvocation.State.RUNNING);

                response = current; // Optional: return latest status
        }

        return Output.builder()
            .workflowInvocationName(response.getName())
            .workflowInvocationState(response.getState().name())
            .build();
    }
}

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The full name of the workflow invocation")
        private String workflowInvocationName;

        @Schema(title = "The final state of the workflow invocation")
        private String workflowInvocationState;
    }
}