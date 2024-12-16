package io.kestra.plugin.gcp.vertexai;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.JobServiceClient;
import com.google.cloud.aiplatform.v1.JobServiceSettings;
import com.google.cloud.aiplatform.v1.JobState;
import com.google.cloud.aiplatform.v1.LocationName;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.plugin.gcp.AbstractTask;
import io.kestra.plugin.gcp.vertexai.models.CustomJobSpec;
import io.kestra.plugin.gcp.services.LogTailService;
import io.kestra.plugin.gcp.services.TimestampService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a Vertex AI [custom job](https://cloud.google.com/vertex-ai/docs/training/create-custom-job)."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_vertexai_custom_job
                namespace: company.team

                tasks:
                  - id: custom_job
                    type: io.kestra.plugin.gcp.vertexai.CustomJob
                    projectId: my-gcp-project
                    region: europe-west1
                    displayName: Start Custom Job
                    spec:
                      workerPoolSpecs:
                      - containerSpec:
                          imageUri: gcr.io/my-gcp-project/my-dir/my-image:latest
                        machineSpec:
                          machineType: n1-standard-4
                        replicaCount: 1
                """
        )
    }
)
public class CustomJob extends AbstractTask implements RunnableTask<CustomJob.Output> {
    @Schema(
        title = "The GCP region."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String region;

    @Schema(
        title = "The job display name."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String displayName;

    @Schema(
        title = "The job specification."
    )
    @PluginProperty
    @NotNull
    private CustomJobSpec spec;

    @Schema(
        title = "Wait for the end of the job.",
        description = "Allowing to capture job status & logs."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    private Boolean wait = true;

    @Schema(
        title = "Delete the job at the end."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    private Boolean delete = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        GoogleCredentials credentials = this.credentials(runContext);
        FixedCredentialsProvider fixedCredentialsProvider = FixedCredentialsProvider.create(credentials);
        AtomicBoolean stopLog = new AtomicBoolean(false);

        JobServiceSettings pipelineServiceSettings = JobServiceSettings.newBuilder()
            .setEndpoint(runContext.render(this.region) + "-aiplatform.googleapis.com:443")
            .setCredentialsProvider(fixedCredentialsProvider)
            .build();

        String jobName = runContext.render(this.displayName);

        try (JobServiceClient client = JobServiceClient.create(pipelineServiceSettings)) {
            com.google.cloud.aiplatform.v1.CustomJob.Builder builder = com.google.cloud.aiplatform.v1.CustomJob.newBuilder()
                .setJobSpec(this.getSpec().to(runContext))
                .setDisplayName(jobName);

            LocationName parent = LocationName.of(
                runContext.render(this.projectId).as(String.class).orElse(null),
                runContext.render(this.region)
            );

            com.google.cloud.aiplatform.v1.CustomJob response = client.createCustomJob(parent, builder.build());

            if (response.hasError()) {
                throw new Exception(response.getError().getMessage());
            }

            logger.info("Job created with name {}", response.getName());

            if (response.getWebAccessUrisCount() > 0) {
                logger.info("Web access: {}", response.getWebAccessUrisMap());
            }

            Output.OutputBuilder outputBuilder = Output.builder()
                .name(response.getName())
                .createDate(TimestampService.of(response.getCreateTime()))
                .updateDate(TimestampService.of(response.getUpdateTime()))
                .state(response.getState());

            Thread tailThread = null;
            try {
                tailThread = LogTailService.tail(
                    logger,
                    runContext.render(this.projectId).as(String.class).orElse(null),
                    credentials,
                    "resource.labels.job_id=\"" + response.getName()
                        .substring(response.getName().lastIndexOf("/") + 1) + "\" AND " +
                        "resource.type=\"ml_job\"",
                    stopLog
                );

                if (this.wait) {
                    com.google.cloud.aiplatform.v1.CustomJob result = Await.until(
                        () -> {
                            com.google.cloud.aiplatform.v1.CustomJob customJob = client.getCustomJob(response.getName());

                            if (!customJob.hasEndTime()) {
                                return null;
                            }

                            return customJob;
                        },
                        Duration.ofSeconds(30)
                    );

                    stopLog.set(true);

                    outputBuilder
                        .endDate(TimestampService.of(result.getEndTime()))
                        .state(result.getState());

                    logger.info("Job {} ended in {} with status {}",
                        result.getName(),
                        Duration.between(TimestampService.of(result.getCreateTime()), TimestampService.of(result.getEndTime())),
                        result.getState()
                    );

                    // wait for logs
                    tailThread.join();

                    if (this.delete) {
                        client.deleteCustomJobAsync(response.getName()).get();
                        logger.info("Job {} is deleted", response.getName());
                    }

                    if (result.hasError()) {
                        throw new Exception(result.getError().getMessage());
                    }
                }

                return outputBuilder.build();
            } finally {
                if (tailThread != null) {
                    tailThread.join();
                }
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @NotNull
        @Schema(
            title = "Resource name of a CustomJob."
        )
        private final String name;

        @NotNull
        @Schema(
            title = "Time when the CustomJob was created."
        )
        private final Instant createDate;


        @NotNull
        @Schema(
            title = "Time when the CustomJob was updated."
        )
        private final Instant updateDate;

        @NotNull
        @Schema(
            title = "Time when the CustomJob was ended."
        )
        private final Instant endDate;

        @NotNull
        @Schema(
            title = "The detailed state of the CustomJob."
        )
        private final JobState state;
    }
}
