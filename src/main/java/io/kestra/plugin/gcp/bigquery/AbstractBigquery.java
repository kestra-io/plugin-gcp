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
import io.kestra.plugin.gcp.shared.AbstractTask;

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

    /** Guards the cause-chain walk against a cyclic chain. Real chains are an order of magnitude shorter. */
    private static final int MAX_CAUSE_DEPTH = 20;

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
                                previousJob = pollUntilDone(connection, previousJob, logger, MAX_CONSECUTIVE_JOB_MISSES);
                            }

                            if (previousJob != null && previousJob.getStatus().getError() == null) {
                                logger.warn(
                                    "Job '{}' already completed successfully despite a transient error, skipping duplicate retry",
                                    previousJob.getJobId()
                                );

                                return previousJob;
                            }

                            // Gone or failed, so there is nothing left to deduplicate against: drop the
                            // id so a later retry resubmits instead of waiting on it again.
                            lastJobId.set(null);
                        }
                    }

                    job = createJob.call();
                    lastJobId.set(job.getJobId());
                    this.trackJob(connection, job.getJobId(), logger);

                    // A submission can come back as a bare job reference. There is nothing to report yet,
                    // and the check below runs once the state is known, so nothing is skipped for good.
                    if (job.getStatus() != null) {
                        BigQueryService.handleErrors(job, logger);
                    }

                    logger.debug("Starting job '{}'", job.getJobId());

                    if (!dryRun) {
                        job = pollUntilDone(connection, job, logger);
                    }

                    BigQueryService.handleErrors(job, logger);

                    return job;
                } catch (Exception exception) {
                    var jobId = job != null ? job.getJobId() : lastJobId.get();

                    if (isInterrupted(exception)) {
                        throw this.interruptedFailure(logger, jobId, exception);
                    }

                    List<BigQueryError> errors = null;
                    var retryable = false;

                    if (exception instanceof com.google.cloud.bigquery.BigQueryException bqException) {
                        errors = BigQueryService.errorsOf(bqException);

                        // Trust the client's verdict only once the job id is known, because the lookback
                        // above can then re-attach instead of resubmitting. A failed submission cannot
                        // tell an accepted job from a lost one, and BigQuery assigns the id itself
                        // (#674), so retrying there would run the statement twice.
                        retryable = jobId != null && bqException.isRetryable();
                    } else if (exception instanceof JobException jobException) {
                        errors = BigQueryService.errorsOf(jobException);
                    }

                    if (errors == null) {
                        throw exception;
                    }

                    logger.warn(
                        "Error query on {} with errors:\n[\n - {}\n]",
                        jobId != null ? "job '" + jobId.getJob() + "'" : "create job",
                        String.join("\n - ", errors.stream().map(BigQueryError::toString).toArray(String[]::new))
                    );

                    throw new BigQueryException(errors, exception, retryable);
                }
            });
    }

    /** Poll backoff: short first so a fast job is not held behind a floor, then widened. */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    Duration jobPollInitialInterval = Duration.ofMillis(500);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    Duration jobPollMaxInterval = Duration.ofSeconds(5);

    /** Ceiling on the whole wait, matching the client's own DEFAULT_JOB_WAIT_SETTINGS totalTimeout. */
    private static final Duration JOB_WAIT_TIMEOUT = Duration.ofHours(12);

    /**
     * Consecutive jobs.get misses tolerated before concluding a job we did not just submit is gone.
     * Three rather than one because a lone miss is transient. Three in a row on a job that does still
     * exist would clear lastJobId and resubmit, which is the duplicate execution #688 guards against;
     * that is the accepted risk, bounded by this job having answered a jobs.get already, and the
     * alternative -- never giving up -- holds the lookback on a vanished job for the full deadline.
     */
    private static final int MAX_CONSECUTIVE_JOB_MISSES = 3;

    /**
     * Await completion by polling jobs.get rather than Job#waitFor().
     *
     * waitFor() delegates to waitForQueryResults(), whose retry config treats rateLimitExceeded as
     * retryable. It cannot tell a throttled API call from a job that has already FAILED with a quota
     * error, so it retries a terminal state for up to DEFAULT_JOB_WAIT_SETTINGS' 12h totalTimeout.
     * jobs.get returns the terminal job as data, so handleErrors() reports it and retryReasons -- which
     * lists rateLimitExceeded first -- applies as documented.
     *
     * The loop reads the state off the job it just fetched rather than calling Job#isDone(), which
     * issues its own jobs.get and discards the result, doubling the request rate and leaving this
     * handle stale.
     */
    private Job pollUntilDone(BigQuery connection, Job job, Logger logger) throws InterruptedException, BigQueryException {
        return pollUntilDone(connection, job, logger, Integer.MAX_VALUE);
    }

    /**
     * {@code maxConsecutiveMisses} distinguishes the two callers. A job we JUST submitted is only
     * invisible because jobs.get has not caught up, so the main wait tolerates misses until the
     * deadline -- capping it would surface a null, and handleErrors(null) raises an
     * IllegalArgumentException that shouldRetry does not retry, failing the task hard. The lookback
     * asks about a job from a previous attempt, which really can be gone, so it caps and gets back
     * null -- matching Job#waitFor()'s contract -- which tells it to resubmit.
     */
    private Job pollUntilDone(BigQuery connection, Job job, Logger logger, int maxConsecutiveMisses) throws InterruptedException, BigQueryException {
        var deadline = System.nanoTime() + JOB_WAIT_TIMEOUT.toNanos();
        var interval = this.jobPollInitialInterval;
        var consecutiveMisses = 0;

        while (job != null && !isDone(job)) {
            if (System.nanoTime() - deadline >= 0) {
                throw timedOut(connection, job, logger);
            }

            Thread.sleep(interval.toMillis());

            interval = interval.multipliedBy(2);
            if (interval.compareTo(this.jobPollMaxInterval) > 0) {
                interval = this.jobPollMaxInterval;
            }

            // A single null read is TRANSIENT -- jobs.get can briefly fail to see a job that was
            // just submitted -- so absorb a few. Persistent absence is a real answer though: the
            // job is gone, and reporting that (null, as Job#waitFor() does) lets the caller
            // resubmit instead of waiting out the whole deadline.
            var refreshed = connection.getJob(job.getJobId());
            if (refreshed == null) {
                if (++consecutiveMisses >= maxConsecutiveMisses) {
                    return null;
                }
            } else {
                consecutiveMisses = 0;
                job = refreshed;
            }
        }

        return job;
    }

    private static boolean isDone(Job job) {
        return job.getStatus() != null && JobStatus.State.DONE.equals(job.getStatus().getState());
    }

    /**
     * The remote job outlives a client-side give-up, so cancel it rather than leave it running and
     * billing, and report in the same shape as every other failure here so shouldRetry() and the
     * task's error output stay consistent.
     */
    private static BigQueryException timedOut(BigQuery connection, Job job, Logger logger) {
        try {
            connection.cancel(job.getJobId());
        } catch (Exception e) {
            logger.warn("Failed to cancel BigQuery job '{}' after the wait timed out", job.getJobId(), e);
        }

        return new BigQueryException(List.of(new BigQueryError(
            "timeout",
            null,
            "Timed out after " + JOB_WAIT_TIMEOUT + " waiting for job '" + job.getJobId() + "' to complete; the job was cancelled"
        )));
    }

    /** The client wraps an interrupt in a BigQueryException carrying no error list, so match on the cause chain. */
    private static boolean isInterrupted(Throwable throwable) {
        for (int depth = 0; throwable != null && depth < MAX_CAUSE_DEPTH; throwable = throwable.getCause(), depth++) {
            if (throwable instanceof InterruptedException) {
                return true;
            }
        }

        return false;
    }

    /**
     * Deliberately not a retryable reason: an interrupted thread cannot make progress. Names the job,
     * which outlives the task unless the kill already cancelled it.
     */
    private BigQueryException interruptedFailure(Logger logger, JobId jobId, Throwable cause) {
        Thread.currentThread().interrupt();

        var message = "Interrupted while waiting for BigQuery job"
            + (jobId != null ? " '" + jobId.getJob() + "'" : "")
            + (this.isCancelled.get()
                ? ", the job was cancelled."
                : ". The job was not cancelled and may still be running on BigQuery.");

        logger.warn(message, cause);

        return new BigQueryException(List.of(new BigQueryError("interrupted", null, message)), cause, false);
    }

    boolean shouldRetry(Throwable failure, Logger logger, RunContext runContext) throws IllegalVariableEvaluationException {
        // Structural, not a matter of which reasons are configured: an interrupted thread cannot back off.
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }

        if (!(failure instanceof BigQueryException bigQueryException)) {
            logger.warn("Cancelled retrying, unknown exception type {}", failure.getClass(), failure);
            return false;
        }

        // A transport failure carries no BigQuery reason to match on, so defer to the client.
        if (bigQueryException.isRetryable()) {
            return true;
        }

        for (BigQueryError error : bigQueryException.getErrors()) {
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
