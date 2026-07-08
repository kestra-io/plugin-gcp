package io.kestra.plugin.gcp.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.google.api.services.dataflow.model.FlexTemplateRuntimeEnvironment;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
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
@Schema(
    title = "Launch a Flex Template Dataflow job",
    description = "Starts a Dataflow pipeline defined as a Flex Template packaged in a Docker container image."
)
@Plugin(
    examples = {
        @Example(
            title = "Launch a Flex Template job.",
            full = true,
            code = """
                id: dataflow_flex_template
                namespace: company.team

                tasks:
                  - id: launch
                    type: io.kestra.plugin.gcp.dataflow.LaunchFlexTemplate
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    location: us-central1
                    jobName: my-flex-job
                    containerSpecGcsPath: gs://my-bucket/flex-templates/spec.json
                    parameters:
                      inputSubscription: projects/my-project/subscriptions/my-sub
                      outputTable: my-project:my_dataset.my_table
                    environment:
                      maxWorkers: 10
                """
        )
    }
)
public class LaunchFlexTemplate extends AbstractDataflow implements RunnableTask<LaunchFlexTemplate.Output> {

    @NotNull
    @Schema(title = "The unique job name")
    @PluginProperty(group = "main")
    private Property<String> jobName;

    @NotNull
    @Schema(title = "The Cloud Storage path to the Flex Template container spec JSON file")
    @PluginProperty(group = "main")
    private Property<String> containerSpecGcsPath;

    @Schema(title = "Template parameters")
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> parameters;

    @Schema(title = "Template runtime environment options")
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> environment;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rLocation = runContext.render(this.location).as(String.class).orElseThrow();
        var rJobName = runContext.render(this.jobName).as(String.class).orElseThrow();
        var rContainerSpecGcsPath = runContext.render(this.containerSpecGcsPath).as(String.class).orElseThrow();

        var launchParameter = new LaunchFlexTemplateParameter()
            .setJobName(rJobName)
            .setContainerSpecGcsPath(rContainerSpecGcsPath);

        if (this.parameters != null) {
            var rParams = runContext.render(this.parameters).asMap(String.class, Object.class);
            var stringParams = new HashMap<String, String>();
            for (var entry : rParams.entrySet()) {
                if (entry.getValue() != null) {
                    stringParams.put(entry.getKey(), entry.getValue().toString());
                }
            }
            launchParameter.setParameters(stringParams);
        }

        if (this.environment != null) {
            var rEnv = runContext.render(this.environment).asMap(String.class, Object.class);
            var envString = JacksonMapper.ofJson().writeValueAsString(rEnv);
            var runtimeEnv = JacksonMapper.ofJson().readValue(envString, FlexTemplateRuntimeEnvironment.class);
            launchParameter.setEnvironment(runtimeEnv);
        }

        var request = new LaunchFlexTemplateRequest()
            .setLaunchParameter(launchParameter);

        var dataflow = this.dataflowClient(runContext);
        var launchRequest = dataflow.projects().locations().flexTemplates()
            .launch(rProjectId, rLocation, request);

        var response = launchRequest.execute();
        var job = response.getJob();
        if (job == null) {
            throw new IllegalStateException("Launch Flex Template response does not contain a job object. Response: " + response.toString());
        }

        return Output.builder()
            .jobId(job.getId())
            .jobState(job.getCurrentState())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The launched job ID")
        private final String jobId;

        @Schema(title = "The job state")
        private final String jobState;
    }
}
