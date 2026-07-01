package io.kestra.plugin.gcp.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.RuntimeEnvironment;

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
    title = "Launch a Classic Template Dataflow job from a GCS path.",
    description = "Starts a Dataflow pipeline defined as a Classic Template stored in Google Cloud Storage."
)
@Plugin(
    examples = {
        @Example(
            title = "Launch a Classic Template job.",
            full = true,
            code = """
                id: dataflow_classic_template
                namespace: company.team

                inputs:
                  - id: input_path
                    type: STRING

                tasks:
                  - id: launch
                    type: io.kestra.plugin.gcp.dataflow.LaunchTemplate
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    location: us-central1
                    jobName: my-etl-job
                    gcsPath: gs://my-bucket/templates/my-template
                    parameters:
                      inputFile: "{{ inputs.input_path }}"
                      outputTable: my-project:my_dataset.my_table
                """
        )
    }
)
public class LaunchTemplate extends AbstractDataflow implements RunnableTask<LaunchTemplate.Output> {

    @NotNull
    @Schema(title = "The unique job name")
    @PluginProperty(group = "main")
    private Property<String> jobName;

    @NotNull
    @Schema(title = "The Cloud Storage path to the Classic Template")
    @PluginProperty(group = "main")
    private Property<String> gcsPath;

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
        var rGcsPath = runContext.render(this.gcsPath).as(String.class).orElseThrow();

        var params = new LaunchTemplateParameters()
            .setJobName(rJobName);

        if (this.parameters != null) {
            var rParams = runContext.render(this.parameters).asMap(String.class, Object.class);
            var stringParams = new HashMap<String, String>();
            for (var entry : rParams.entrySet()) {
                if (entry.getValue() != null) {
                    stringParams.put(entry.getKey(), entry.getValue().toString());
                }
            }
            params.setParameters(stringParams);
        }

        if (this.environment != null) {
            var rEnv = runContext.render(this.environment).asMap(String.class, Object.class);
            var envString = JacksonMapper.ofJson().writeValueAsString(rEnv);
            var runtimeEnv = JacksonMapper.ofJson().readValue(envString, RuntimeEnvironment.class);
            params.setEnvironment(runtimeEnv);
        }

        var dataflow = this.dataflowClient(runContext);
        var launchRequest = dataflow.projects().locations().templates()
            .launch(rProjectId, rLocation, params)
            .setGcsPath(rGcsPath);

        var response = launchRequest.execute();
        var job = response.getJob();
        if (job == null) {
            throw new IllegalStateException("Launch Classic Template response does not contain a job object. Response: " + response.toString());
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
