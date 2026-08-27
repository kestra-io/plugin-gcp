package io.kestra.plugin.gcp.bigquery;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.retrys.AbstractRetry;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import dev.failsafe.Failsafe;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBigquery extends AbstractTask implements WorkerJobLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBigquery.class);

    @Schema(
        title = "Dataset location",
        description = "Optional BigQuery location for created or targeted resources. Experimental and may change; see BigQuery dataset location documentation."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> location;

    @Schema(
        title = "Automatic BigQuery retry policy",
        description = "Optional custom retry policy for retryable BigQuery errors. If unset, uses an exponential backoff starting at 5s (per-attempt interval capped at 60m), with a total duration of up to 15m and a maximum of 10 attempts."
    )
    @PluginProperty(group = "advanced")
    protected AbstractRetry retryAuto;

    @Builder.Default
    @Schema(
        title = "Retry reasons",
        description = "BigQuery error reasons that trigger an automatic retry; evaluated against error reason strings"
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> retryReasons = Property.ofValue(
        Arrays.asList(
            "rateLimitExceeded",
            "jobBackendError",
            "backendError",
            "internalError",
            "jobInternalError"
        )
    );

    @Builder.Default
    @Schema(
        title = "Retry message substrings",
        description = "Case-insensitive substrings that, if found in the error message, trigger an automatic retry"
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> retryMessages = Property.ofValue(
        Arrays.asList(
            "due to concurrent update",
            "Retrying the job may solve the problem",
            "Retrying may solve the problem"
        )
    );

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<BigQuery> trackedConnection = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<JobId> trackedJobId = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<Logger> trackedLogger = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);

    /**
     * Records the job currently submitted, so that {@link #kill()} or {@link #stop()} can cancel the
     * live BigQuery job instead of a stale one from a previous retry attempt.
     */
    protected void trackJob(BigQuery connection, JobId jobId, Logger logger) {
        this.trackedConnection.set(connection);
        this.trackedJobId.set(jobId);
        this.trackedLogger.set(logger);
    }

    @Override
    public void kill() {
        cancelTrackedJob();
    }

    @Override
    public void stop() {
        cancelTrackedJob();
    }

    private void cancelTrackedJob() {
        if (isCancelled.compareAndSet(false, true)) {
            BigQuery connection = this.trackedConnection.get();
            JobId jobId = this.trackedJobId.get();

            if (connection != null && jobId != null) {
                try {
                    connection.cancel(jobId);
                } catch (Exception e) {
                    Logger logger = this.trackedLogger.get();
                    if (logger != null) {
                        logger.warn("Failed to cancel BigQuery job '{}'", jobId, e);
                    } else {
                        LOG.warn("Failed to cancel BigQuery job '{}'", jobId, e);
                    }
                }
            }
        }
    }

    BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials = this.credentials(runContext);
        String projectId = runContext.render(this.projectId).as(String.class).orElse(null);
        String location = runContext.render(this.location).as(String.class).orElse(null);

        return connection(runContext, credentials, projectId, location);
    }

    protected static BigQuery connection(RunContext runContext, GoogleCredentials googleCredentials, String projectId, String location) throws IllegalVariableEvaluationException {
        return BigQueryOptions
            .newBuilder()
            .setCredentials(googleCredentials)
            .setProjectId(projectId)
            .setLocation(location)
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }

    protected Job waitForJob(Logger logger, Callable<Job> createJob, RunContext runContext, BigQuery connection) {
        return this.waitForJob(logger, createJob, false, runContext, connection);
    }

    protected Job waitForJob(Logger logger, Callable<Job> createJob, Boolean dryRun, RunContext runContext, BigQuery connection) {
        var lastJobId = new AtomicReference<JobId>();

        return Failsafe
            .with(
                AbstractRetry.<Job> retryPolicy(
                    this.getRetryAuto() != null ? this.getRetryAuto()
                        : Exponential.builder()
                            .type("exponential")
                            .interval(Duration.ofSeconds(5))
                            .maxInterval(Duration.ofMinutes(60))
                            .maxDuration(Duration.ofMinutes(15))
                            .maxAttempts(10)
                            .build()
                )
                    .handleIf(throwable -> this.shouldRetry(throwable, logger, runContext))
                    .onFailure(
                        event -> logger.error(
                            "Stop retry, attempts {} elapsed {} seconds",
                            event.getAttemptCount(),
                            event.getElapsedTime().getSeconds(),
                            event.getException()
                        )
                    )
                    .onRetry(event ->
                    {
                        logger.warn(
                            "Retrying, attempts {} elapsed {} seconds",
                            event.getAttemptCount(),
                            event.getElapsedTime().getSeconds()
                        );
                    }).build()
            )
            .get(() ->
            {
                Job job = null;
                try {
                    // Dry-run jobs have no side effects and aren't reliably pollable, so always create a fresh one.
                    if (!dryRun) {
                        var previousJobId = lastJobId.get();
                        if (previousJobId != null) {
                            var previousJob = connection.getJob(previousJobId);

                            if (previousJob != null) {
                                if (!previousJob.isDone()) {
                                    previousJob = previousJob.waitFor();
                                }

                                if (previousJob.getStatus().getError() == null) {
                                    logger.warn(
                                        "Job '{}' already completed successfully despite a transient error, skipping duplicate retry",
                                        previousJob.getJobId()
                                    );

                                    return previousJob;
                                }

                                lastJobId.set(null);
                            }
                        }
                    }

                    job = createJob.call();
                    lastJobId.set(job.getJobId());
                    this.trackJob(connection, job.getJobId(), logger);

                    BigQueryService.handleErrors(job, logger);

                    logger.debug("Starting job '{}'", job.getJobId());

                    if (!dryRun) {
                        job = job.waitFor();
                    }

                    BigQueryService.handleErrors(job, logger);

                    return job;
                } catch (Exception exception) {
                    JobId jobId = job != null ? job.getJobId() : lastJobId.get();

                    if (isInterrupted(exception)) {
                        throw this.interrupted(logger, jobId, exception);
                    }

                    if (exception instanceof com.google.cloud.bigquery.BigQueryException bqException) {
                        List<BigQueryError> errors = BigQueryService.errorsOf(bqException);

                        logger.warn(
                            "Error query on {} with errors:\n[\n - {}\n]",
                            jobId != null ? "job '" + jobId.getJob() + "'" : "create job",
                            String.join("\n - ", errors.stream().map(BigQueryError::toString).toArray(String[]::new))
                        );

                        throw new BigQueryException(errors, bqException);
                    } else if (exception instanceof JobException bqException) {
                        List<BigQueryError> errors = BigQueryService.errorsOf(bqException);

                        logger.warn(
                            "Error query on {} with errors:\n[\n - {}\n]",
                            jobId != null ? "job '" + jobId.getJob() + "'" : "create job",
                            String.join("\n - ", errors.stream().map(BigQueryError::toString).toArray(String[]::new))
                        );

                        throw new BigQueryException(errors, bqException);
                    }

                    throw exception;
                }
            });
    }

    /**
     * The worker interrupts the task thread on a kill, on a task timeout, and on {@code shutdownNow()}
     * when a worker terminates. The BigQuery client surfaces that as a BigQueryException wrapping an
     * InterruptedException, with no error list and no HTTP code, so it has to be recognised before the
     * generic handling below turns it into an errorless failure.
     */
    private static boolean isInterrupted(Throwable throwable) {
        // Bounded walk: a malformed cause chain must not spin here.
        for (int depth = 0; throwable != null && depth < 20; throwable = throwable.getCause(), depth++) {
            if (throwable instanceof InterruptedException) {
                return true;
            }
        }

        return false;
    }

    /**
     * Reports an interrupted wait against the job it was waiting on. Retrying is pointless once the
     * thread carries the interrupt flag, so this is deliberately not a retryable reason, but the job id
     * has to reach the user: unless the task was killed, the job keeps running on BigQuery and will
     * complete there while Kestra reports the task as failed.
     */
    private BigQueryException interrupted(Logger logger, JobId jobId, Throwable cause) {
        Thread.currentThread().interrupt();

        String message = "Interrupted while waiting for BigQuery job"
            + (jobId != null ? " '" + jobId.getJob() + "'" : "")
            + (this.isCancelled.get()
                ? ", the job was cancelled."
                : ". The job was not cancelled and may still be running on BigQuery.");

        logger.warn(message, cause);

        return new BigQueryException(List.of(new BigQueryError("interrupted", null, message)), cause);
    }

    boolean shouldRetry(Throwable failure, Logger logger, RunContext runContext) throws IllegalVariableEvaluationException {
        if (!(failure instanceof BigQueryException)) {
            logger.warn("Cancelled retrying, unknown exception type {}", failure.getClass(), failure);
            return false;
        }

        for (BigQueryError error : ((BigQueryException) failure).getErrors()) {
            if (error.getReason() != null && runContext.render(this.retryReasons).asList(String.class).contains(error.getReason())) {
                return true;
            }

            if (this.retryMessages != null && error.getMessage() != null) {
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
