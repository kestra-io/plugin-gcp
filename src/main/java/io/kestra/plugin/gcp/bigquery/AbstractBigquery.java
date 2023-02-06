package io.kestra.plugin.gcp.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import io.kestra.core.utils.VersionProvider;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import net.jodah.failsafe.Failsafe;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.retrys.AbstractRetry;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBigquery extends AbstractTask {
    @Schema(
        title = "The geographic location where the dataset should reside",
        description = "This property is experimental" +
            " and might be subject to change or removed.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset Location</a>"
    )
    @PluginProperty(dynamic = true)
    protected String location;

    @Schema(
        title = "Automatic retry for retryable bigquery exceptions",
        description = "Some exceptions (especially rate limit) are not retried by default by BigQuery client, we use by " +
            "default a transparent retry (not the kestra one) to handle this case.\n" +
            "The default values are Exponential of 5 seconds for max 15 minutes and 10 attempts"
    )
    @PluginProperty
    protected AbstractRetry retryAuto;

    @Builder.Default
    @Schema(
        title = "The reason that are valid for a automatic retry."
    )
    @PluginProperty(dynamic = true)
    protected List<String> retryReasons = Arrays.asList(
        "rateLimitExceeded",
        "jobBackendError",
        "internalError",
        "jobInternalError"
    );

    @Builder.Default
    @Schema(
        title = "The message that are valid for a automatic retry.",
        description = "Message is tested as a substring of the full message and case insensitive"
    )
    @PluginProperty(dynamic = true)
    protected List<String> retryMessages = Arrays.asList(
        "due to concurrent update",
        "Retrying the job may solve the problem"
    );

    BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return connection(
            runContext,
            this.credentials(runContext),
            this.projectId,
            this.location
        );
    }

    static BigQuery connection(RunContext runContext, GoogleCredentials googleCredentials, String projectId, String location) throws IllegalVariableEvaluationException {
        VersionProvider versionProvider = runContext.getApplicationContext().getBean(VersionProvider.class);

        return BigQueryOptions
            .newBuilder()
            .setCredentials(googleCredentials)
            .setProjectId(runContext.render(projectId))
            .setLocation(runContext.render(location))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + versionProvider.getVersion()))
            .build()
            .getService();
    }

    protected Job waitForJob(Logger logger, Callable<Job> createJob) {
        return this.waitForJob(logger, createJob, false);
    }

    protected Job waitForJob(Logger logger, Callable<Job> createJob, Boolean dryRun) {
        return Failsafe
            .with(AbstractRetry.<Job>retryPolicy(this.getRetryAuto() != null ? this.getRetry() : Exponential.builder()
                    .type("exponential")
                    .interval(Duration.ofSeconds(5))
                    .maxInterval(Duration.ofMinutes(60))
                    .maxDuration(Duration.ofMinutes(15))
                    .maxAttempt(10)
                    .build()
                )
                .handleIf(throwable -> this.shouldRetry(throwable, logger))
                .onFailure(event -> logger.error(
                    "Stop retry, attempts {} elapsed {} seconds",
                    event.getAttemptCount(),
                    event.getElapsedTime().getSeconds(),
                    event.getFailure()
                ))
                .onRetry(event -> {
                    logger.warn(
                        "Retrying, attempts {} elapsed {} seconds",
                        event.getAttemptCount(),
                        event.getElapsedTime().getSeconds()
                    );
                })
            )
            .get(() -> {
                Job job = null;
                try {
                    job = createJob.call();

                    BigQueryService.handleErrors(job, logger);

                    logger.debug("Starting job '{}'", job.getJobId());

                    if (!dryRun) {
                        job = job.waitFor();
                    }

                    BigQueryService.handleErrors(job, logger);

                    return job;
                } catch (Exception exception) {
                    if (exception instanceof com.google.cloud.bigquery.BigQueryException) {
                        com.google.cloud.bigquery.BigQueryException bqException = (com.google.cloud.bigquery.BigQueryException) exception;

                        logger.warn(
                            "Error query on {} with errors:\n[\n - {}\n]",
                            job != null ? "job '" + job.getJobId().getJob() + "'" : "create job",
                            String.join("\n - ", bqException.getErrors().stream().map(BigQueryError::toString).toArray(String[]::new))
                        );

                        throw new BigQueryException(bqException.getErrors());
                    } else if (exception instanceof JobException) {
                        JobException bqException = (JobException) exception;

                        logger.warn(
                            "Error query on job '{}' with errors:\n[\n - {}\n]",
                            job != null ? "job '" + job.getJobId().getJob() + "'" : "create job",
                            String.join("\n - ", bqException.getErrors().stream().map(BigQueryError::toString).toArray(String[]::new))
                        );

                        throw new BigQueryException(bqException.getErrors());
                    }

                    throw exception;
                }
            });
    }

    private boolean shouldRetry(Throwable failure, Logger logger) {
        if (!(failure instanceof BigQueryException)) {
            logger.warn("Cancelled retrying, unknown exception type {}", failure.getClass(), failure);
            return false;
        }

        for (BigQueryError error : ((BigQueryException) failure).getErrors()) {
            if (this.retryReasons != null && this.retryReasons.contains(error.getReason())) {
                return true;
            }

            if (this.retryMessages != null) {
                for (String message: this.retryMessages) {
                    if (error.getMessage().toLowerCase().contains(message.toLowerCase())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
