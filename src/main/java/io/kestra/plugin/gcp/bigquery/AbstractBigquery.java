package io.kestra.plugin.gcp.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import dev.failsafe.Failsafe;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
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
        title = "The geographic location where the dataset should reside.",
        description = "This property is experimental" +
            " and might be subject to change or removed.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset Location</a>"
    )
    protected Property<String> location;

    @Schema(
        title = "Automatic retry for retryable BigQuery exceptions.",
        description = "Some exceptions (especially rate limit) are not retried by default by BigQuery client, we use by " +
            "default a transparent retry (not the kestra one) to handle this case.\n" +
            "The default values are exponential of 5 seconds for a maximum of 15 minutes and ten attempts"
    )
    @PluginProperty
    protected AbstractRetry retryAuto;

    @Builder.Default
    @Schema(
        title = "The reasons which would trigger an automatic retry."
    )
    protected Property<List<String>> retryReasons = Property.of(Arrays.asList(
        "rateLimitExceeded",
        "jobBackendError",
        "backendError",
        "internalError",
        "jobInternalError"
    ));

    @Builder.Default
    @Schema(
        title = "The messages which would trigger an automatic retry.",
        description = "Message is tested as a substring of the full message, and is case insensitive."
    )
    protected Property<List<String>> retryMessages = Property.of(Arrays.asList(
        "due to concurrent update",
        "Retrying the job may solve the problem",
        "Retrying may solve the problem"
    ));

    BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return connection(
            runContext,
            this.credentials(runContext),
            runContext.render(this.projectId).as(String.class).orElse(null),
            runContext.render(this.location).as(String.class).orElse(null)
        );
    }

    static BigQuery connection(RunContext runContext, GoogleCredentials googleCredentials, String projectId, String location) throws IllegalVariableEvaluationException {
        return BigQueryOptions
            .newBuilder()
            .setCredentials(googleCredentials)
            .setProjectId(runContext.render(projectId))
            .setLocation(runContext.render(location))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }

    protected Job waitForJob(Logger logger, Callable<Job> createJob, RunContext runContext) {
        return this.waitForJob(logger, createJob, false, runContext);
    }

    protected Job waitForJob(Logger logger, Callable<Job> createJob, Boolean dryRun, RunContext runContext) {
        return Failsafe
            .with(AbstractRetry.<Job>retryPolicy(this.getRetryAuto() != null ? this.getRetry() : Exponential.builder()
                    .type("exponential")
                    .interval(Duration.ofSeconds(5))
                    .maxInterval(Duration.ofMinutes(60))
                    .maxDuration(Duration.ofMinutes(15))
                    .maxAttempt(10)
                    .build()
                )
                .handleIf(throwable -> this.shouldRetry(throwable, logger, runContext))
                .onFailure(event -> logger.error(
                    "Stop retry, attempts {} elapsed {} seconds",
                    event.getAttemptCount(),
                    event.getElapsedTime().getSeconds(),
                    event.getException()
                ))
                .onRetry(event -> {
                    logger.warn(
                        "Retrying, attempts {} elapsed {} seconds",
                        event.getAttemptCount(),
                        event.getElapsedTime().getSeconds()
                    );
                }).build()
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

    private boolean shouldRetry(Throwable failure, Logger logger, RunContext runContext) throws IllegalVariableEvaluationException {
        if (!(failure instanceof BigQueryException)) {
            logger.warn("Cancelled retrying, unknown exception type {}", failure.getClass(), failure);
            return false;
        }

        for (BigQueryError error : ((BigQueryException) failure).getErrors()) {
            if (runContext.render(this.retryReasons).asList(String.class).contains(error.getReason())) {
                return true;
            }

            if (this.retryMessages != null) {
                for (String message: runContext.render(this.retryMessages).asList(String.class)) {
                    if (error.getMessage().toLowerCase().contains(message.toLowerCase())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
