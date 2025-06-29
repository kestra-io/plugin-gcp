package io.kestra.plugin.gcp.vertexai;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.JobServiceClient;
import com.google.cloud.aiplatform.v1.JobServiceSettings;
import com.google.cloud.aiplatform.v1.JobState;
import com.google.cloud.aiplatform.v1.LocationName;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a custom job in Google Vertex AI.",
    description = "For more details, check out the [custom job documentation](https://cloud.google.com/vertex-ai/docs/training/create-custom-job)."
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
    @NotNull
    private Property<String> region;

    @Schema(
        title = "The job display name."
    )
    @NotNull
    private Property<String> displayName;

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
    @NotNull
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(true);

    @Schema(
        title = "Delete the job at the end."
    )
    @NotNull
    @Builder.Default
    private Property<Boolean> delete = Property.ofValue(true);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final AtomicReference<Runnable> killable = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        GoogleCredentials credentials = this.credentials(runContext);
        FixedCredentialsProvider fixedCredentialsProvider = FixedCredentialsProvider.create(credentials);
        AtomicBoolean stopLog = new AtomicBoolean(false);

        JobServiceSettings pipelineServiceSettings = JobServiceSettings.newBuilder()
            .setEndpoint(runContext.render(this.region).as(String.class).orElseThrow() + "-aiplatform.googleapis.com:443")
            .setCredentialsProvider(fixedCredentialsProvider)
            .build();

        String jobName = runContext.render(this.displayName).as(String.class).orElseThrow();

        try (JobServiceClient client = JobServiceClient.create(pipelineServiceSettings)) {
            com.google.cloud.aiplatform.v1.CustomJob.Builder builder = com.google.cloud.aiplatform.v1.CustomJob.newBuilder()
                .setJobSpec(this.getSpec().to(runContext))
                .setDisplayName(jobName);

            LocationName parent = LocationName.of(
                runContext.render(this.projectId).as(String.class).orElse(null),
                runContext.render(this.region).as(String.class).orElseThrow()
            );

            com.google.cloud.aiplatform.v1.CustomJob response = client.createCustomJob(parent, builder.build());

            //Set killable in case kill method is called
            killable.set(() -> safelyKillJob(runContext, pipelineServiceSettings, response.getName()));

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

                if (runContext.render(this.wait).as(Boolean.class).orElseThrow()) {
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

                    if (runContext.render(this.delete).as(Boolean.class).orElseThrow()) {
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

    private static void safelyKillJob(RunContext runContext, JobServiceSettings settings, String jobName) {
        try (final JobServiceClient client = JobServiceClient.create(settings)) {
            client.cancelCustomJob(jobName);
        } catch (ApiException e) {
            runContext.logger().warn("API issue when cancelling job: {}", jobName);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            runContext.logger().warn("Failed to cancel job: {}", jobName, e);
        }
    }

    @Override
    public void kill() {
        if (isKilled.compareAndSet(false, true)) {
            Optional.ofNullable(killable.get()).ifPresent(Runnable::run);
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
