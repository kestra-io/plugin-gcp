package io.kestra.plugin.gcp.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import dev.failsafe.Failsafe;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.retrys.AbstractRetry;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import com.fasterxml.jackson.annotation.JsonIgnore;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBigquery extends AbstractTask {
    @Schema(
        title = "Dataset location",
        description = "Optional BigQuery location for created or targeted resources. Experimental and may change; see BigQuery dataset location documentation."
    )
    protected Property<String> location;

    @Schema(
        title = "Automatic BigQuery retry policy",
        description = "Optional custom retry policy for retryable BigQuery errors. If unset, uses an exponential backoff starting at 5s, up to 15m, max 10 attempts."
    )
    @PluginProperty
    protected AbstractRetry retryAuto;

    @Builder.Default
    @Schema(
        title = "Retry reasons",
        description = "BigQuery error reasons that trigger an automatic retry; evaluated against error reason strings"
    )
    protected Property<List<String>> retryReasons = Property.ofValue(Arrays.asList(
        "rateLimitExceeded",
        "jobBackendError",
        "backendError",
        "internalError",
        "jobInternalError"
    ));

    @Builder.Default
    @Schema(
        title = "Retry message substrings",
        description = "Case-insensitive substrings that, if found in the error message, trigger an automatic retry"
    )
    protected Property<List<String>> retryMessages = Property.ofValue(Arrays.asList(
        "due to concurrent update",
        "Retrying the job may solve the problem",
        "Retrying may solve the problem"
    ));

    @FunctionalInterface
    public interface BigQueryFactory {
        BigQuery create(
            RunContext runContext,
            GoogleCredentials credentials,
            String projectId,
            String location
        ) throws IllegalVariableEvaluationException;
    }

    @Builder.Default
    @JsonIgnore
    protected BigQueryFactory bigQueryFactory = AbstractBigquery::connection;


//    BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
//        return connection(
//            runContext,
//            this.credentials(runContext),
//            runContext.render(this.projectId).as(String.class).orElse(null),
//            runContext.render(this.location).as(String.class).orElse(null)
//        );
//    }

    BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials = this.credentials(runContext);
        String projectId = runContext.render(this.projectId).as(String.class).orElse(null);
        String location = runContext.render(this.location).as(String.class).orElse(null);

        return bigQueryFactory.create(runContext, credentials, projectId, location);
    }



    static BigQuery connection(RunContext runContext, GoogleCredentials googleCredentials, String projectId, String location) throws IllegalVariableEvaluationException {
        return BigQueryOptions
            .newBuilder()
            .setCredentials(googleCredentials)
            .setProjectId(projectId)
            .setLocation(location)
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
                        .maxAttempts(10)
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
                            bqException.getErrors() == null ? "" : String.join("\n - ", bqException.getErrors().stream().map(BigQueryError::toString).toArray(String[]::new))
                        );

                        throw new BigQueryException(bqException.getErrors());
                    } else if (exception instanceof JobException) {
                        JobException bqException = (JobException) exception;

                        logger.warn(
                            "Error query on job '{}' with errors:\n[\n - {}\n]",
                            job != null ? "job '" + job.getJobId().getJob() + "'" : "create job",
                            bqException.getErrors() == null ? "" : String.join("\n - ", bqException.getErrors().stream().map(BigQueryError::toString).toArray(String[]::new))
                        );

                        throw new BigQueryException(bqException.getErrors());
                    }

                    throw exception;
                }
            });
    }

    boolean shouldRetry(Throwable failure, Logger logger, RunContext runContext) throws IllegalVariableEvaluationException {
        if (!(failure instanceof BigQueryException)) {
            logger.warn("Cancelled retrying, unknown exception type {}", failure.getClass(), failure);
            return false;
        }

        for (BigQueryError error : ((BigQueryException) failure).getErrors()) {
            if (runContext.render(this.retryReasons).asList(String.class).contains(error.getReason())) {
                return true;
            }

            if (this.retryMessages != null) {
                for (String message : runContext.render(this.retryMessages).asList(String.class)) {
                    if (error.getMessage().toLowerCase().contains(message.toLowerCase())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
