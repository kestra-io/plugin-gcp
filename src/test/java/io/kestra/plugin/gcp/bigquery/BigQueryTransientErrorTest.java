package io.kestra.plugin.gcp.bigquery;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import dev.failsafe.FailsafeException;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Transport failures carry no error list, and used to be rethrown as an empty "Bigquery Errors [ - ]".
 *
 * @see <a href="https://github.com/kestra-io/plugin-gcp/issues/675">#675</a>
 */
@KestraTest
class BigQueryTransientErrorTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static JobStatus runningStatus() {
        var status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getState()).thenReturn(JobStatus.State.RUNNING);

        return status;
    }

    @AfterEach
    void clearInterruptFlag() {
        // waitForJob restores the interrupt flag, and JUnit reuses this thread.
        Thread.interrupted();
    }

    @Test
    void shouldCarryTheMessageWhenTheErrorListIsMissing() {
        var exception = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");

        List<BigQueryError> errors = BigQueryService.errorsOf(exception);

        assertThat(errors, hasSize(1));
        assertThat(errors.getFirst().getReason(), is("unknown"));
        assertThat(errors.getFirst().getMessage(), is("The service is currently unavailable."));
    }

    @Test
    void shouldFallBackToTheCauseWhenTheExceptionCarriesNoMessage() {
        var cause = new IOException("Connection reset");
        var exception = new com.google.cloud.bigquery.BigQueryException(0, null, cause);

        List<BigQueryError> errors = BigQueryService.errorsOf(exception);

        assertThat(errors, hasSize(1));
        assertThat(errors.getFirst().getMessage(), containsString("Connection reset"));
    }

    @Test
    void shouldKeepTheReportedErrorListUntouched() {
        var reported = new BigQueryError("invalidQuery", null, "Syntax error");
        var exception = new com.google.cloud.bigquery.BigQueryException(400, "Syntax error", reported);

        assertThat(BigQueryService.errorsOf(exception), is(List.of(reported)));
    }

    @Test
    void shouldTrustTheClientVerdictOnceTheJobIdIsKnown() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var unavailable = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");
        var job = runningJob("job_poll_failed");
        var connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.getJob(job.getJobId())).thenThrow(unavailable);

        var failure = failureOf(task, runContext, () -> job, connection);

        assertThat(failure.isRetryable(), is(true));
        assertThat(failure.getCause(), instanceOf(com.google.cloud.bigquery.BigQueryException.class));
        assertThat(failure.getMessage(), containsString("The service is currently unavailable."));
    }

    @Test
    void shouldFollowTheClientVerdictRatherThanAnHttpStatus() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        // A socket timeout has no HTTP status at all, yet the client reports it as worth retrying.
        var job = runningJob("job_socket_timeout");
        var connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.getJob(job.getJobId()))
            .thenThrow(new com.google.cloud.bigquery.BigQueryException(new SocketTimeoutException("read timed out")));

        var failure = failureOf(task, runContext, () -> job, connection);

        assertThat(failure.isRetryable(), is(true));
        assertThat(failure.getMessage(), containsString("read timed out"));
    }

    @Test
    void shouldNotRetryASubmissionThatFailedWithoutAnErrorList() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var submissions = new AtomicInteger();

        // Nothing here can tell an accepted job from a lost one, so a retry could run the statement twice.
        var unavailable = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");

        var failure = failureOf(task, runContext, () ->
        {
            submissions.incrementAndGet();
            throw unavailable;
        });

        assertThat(failure.isRetryable(), is(false));
        assertThat(failure.getMessage(), containsString("The service is currently unavailable."));
        assertThat(failure.getCause(), is(unavailable));
        assertThat(submissions.get(), is(1));
    }

    @Test
    void shouldNameTheJobAndStopRetryingWhenThePollIsInterrupted() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var submissions = new AtomicInteger();

        // The poll sleeps between attempts, so an interrupt lands as a raw InterruptedException
        // out of Thread.sleep -- the same shape Job#waitFor used to declare.
        var job = runningJob("job_interrupted");
        var connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.getJob(job.getJobId())).thenAnswer(invocation ->
        {
            Thread.currentThread().interrupt();
            throw new InterruptedException();
        });

        var failure = failureOf(task, runContext, () ->
        {
            submissions.incrementAndGet();
            return job;
        }, connection);

        assertThat(failure.isRetryable(), is(false));
        assertThat(failure.getErrors(), hasSize(1));
        assertThat(failure.getErrors().getFirst().getReason(), is("interrupted"));
        assertThat(failure.getErrors().getFirst().getMessage(), containsString("'job_interrupted'"));
        assertThat(failure.getErrors().getFirst().getMessage(), containsString("may still be running on BigQuery"));

        // An interrupted thread cannot make progress: no replay.
        assertThat(submissions.get(), is(1));
        assertThat(Thread.currentThread().isInterrupted(), is(true));
    }

    @Test
    void shouldSayTheJobWasCancelledWhenTheTaskWasKilledFirst() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        task.kill();

        var failure = failureOf(task, runContext, () ->
        {
            throw new com.google.cloud.bigquery.BigQueryException(0, "java.lang.InterruptedException", new InterruptedException());
        });

        assertThat(failure.getErrors().getFirst().getMessage(), containsString("the job was cancelled"));
        assertThat(failure.getErrors().getFirst().getMessage(), not(containsString("may still be running")));
    }

    /**
     * A quota error on a job that has ALREADY FINISHED must be reported, not retried.
     *
     * Job#waitFor() delegates to waitForQueryResults(), whose BigQueryRetryHelper config treats
     * `rateLimitExceeded` as retryable. That is right when the API call was throttled, but when
     * the JOB ITSELF died of a quota error the same message keeps coming back, so the client
     * retried a terminal state under a 12-hour totalTimeout and the task never finished.
     *
     * Note this is also the FIRST entry in the task's own `retryReasons` default: the plugin
     * already intends to handle rateLimitExceeded via its own retry policy, and could not,
     * because the client swallowed it first.
     */
    @Test
    void shouldReportATerminalQuotaErrorInsteadOfRetryingIt() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var jobId = JobId.of("project", "job_rate_limited");

        // Submitted clean, so the pre-wait error check passes and the wait is actually entered.
        // Built before the when(...) chain: creating a mock inside thenReturn() trips
        // Mockito's UnfinishedStubbingException.
        var submittedStatus = runningStatus();

        var submitted = Mockito.mock(Job.class);
        Mockito.when(submitted.getJobId()).thenReturn(jobId);
        Mockito.when(submitted.getStatus()).thenReturn(submittedStatus);

        // It then finishes having hit the quota.
        var quota = terminalJob(
            "job_rate_limited",
            new BigQueryError(
                "rateLimitExceeded", null,
                "Exceeded rate limits: too many table dml insert operations for this table."
            )
        );

        var connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.getJob(jobId)).thenReturn(quota);

        var failure = failureOf(task, runContext, () -> submitted, connection);

        assertThat(failure.getErrors(), hasSize(1));
        assertThat(failure.getErrors().getFirst().getReason(), is("rateLimitExceeded"));
        assertThat(failure.getMessage(), containsString("too many table dml insert operations"));
    }

    /**
     * A submission-time rejection leaves a bare job reference whose getStatus() is null, because
     * Job#isDone() reloads internally and reports terminal without populating the handle we hold.
     * Without a re-fetch, handleErrors() throws "has no status to report" -- an IllegalStateException
     * that is not retryable -- so the real BigQuery reason is lost and the task fails uninformatively.
     */
    @Test
    void shouldRefetchAJobThatIsTerminalWithNoStatus() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var outstanding = new BigQueryError(
            "resourcesExceeded", null,
            "Resources exceeded during query execution: Too many DML statements outstanding against table."
        );
        var jobId = JobId.of("project", "job_no_status");

        // What createJob() hands back on a rejected submission: terminal, but status not populated.
        var bare = Mockito.mock(Job.class);
        Mockito.when(bare.getJobId()).thenReturn(jobId);
        Mockito.when(bare.getStatus()).thenReturn(null);

        // Built BEFORE the when(...) chain: creating a mock inside thenReturn() trips
        // Mockito's UnfinishedStubbingException.
        var refetched = terminalJob("job_no_status", outstanding);

        var connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.getJob(jobId)).thenReturn(refetched);

        var failure = failureOf(task, runContext, () -> bare, connection);

        assertThat(failure.getErrors(), hasSize(1));
        assertThat(failure.getErrors().getFirst().getReason(), is("resourcesExceeded"));
        assertThat(failure.getMessage(), containsString("Too many DML statements outstanding"));
    }

    /**
     * A job submitted as RUNNING that later FAILS must never be reported as a success.
     *
     * Job#isDone() reloads internally but does not refresh the handle the caller holds, so after
     * the wait that handle can still carry the RUNNING status it was created with. Reporting from
     * it would find no error and pass -- a silent false success on a job BigQuery actually failed.
     */
    @Test
    void shouldNotReportSuccessWhenTheHeldHandleStillSaysRunning() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var jobId = JobId.of("project", "job_stale_handle");

        // The handle from createJob(): reports terminal, but its cached status shows no error.
        var runningStatus = runningStatus();

        var stale = Mockito.mock(Job.class);
        Mockito.when(stale.getJobId()).thenReturn(jobId);
        Mockito.when(stale.getStatus()).thenReturn(runningStatus);

        // The authoritative job says it failed.
        var authoritative = terminalJob("job_stale_handle", new BigQueryError("invalidQuery", null, "Division by zero"));

        var connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.getJob(jobId)).thenReturn(authoritative);

        var failure = failureOf(task, runContext, () -> stale, connection);

        assertThat(failure.getErrors(), hasSize(1));
        assertThat(failure.getErrors().getFirst().getReason(), is("invalidQuery"));
        assertThat(failure.getMessage(), containsString("Division by zero"));
    }

    /**
     * jobs.get can briefly fail to see a job that was just submitted. Treating that null as
     * "the job is gone" reaches handleErrors(null), which throws IllegalArgumentException --
     * not a retryable reason -- turning a momentary blip into a hard task failure.
     */
    @Test
    void shouldKeepPollingWhenGetJobTransientlyReturnsNull() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var jobId = JobId.of("project", "job_transient_null");
        var polled = new AtomicInteger();

        var pollingStatus = runningStatus();

        var job = Mockito.mock(Job.class);
        Mockito.when(job.getJobId()).thenReturn(jobId);
        Mockito.when(job.getStatus()).thenReturn(pollingStatus);

        var failed = terminalJob("job_transient_null", new BigQueryError("invalidQuery", null, "Syntax error"));

        var connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.getJob(jobId)).thenAnswer(invocation ->
            polled.incrementAndGet() == 1 ? null : failed
        );

        var failure = failureOf(task, runContext, () -> job, connection);

        // The null was absorbed: polling continued and the real error still surfaced.
        // Two reads: the absorbed null, then the terminal job.
        assertThat(polled.get(), is(2));
        assertThat(failure.getErrors().getFirst().getReason(), is("invalidQuery"));
        assertThat(failure.getCause(), not(instanceOf(IllegalArgumentException.class)));
    }

    /**
     * The lookback re-attach has the same terminal-vs-retryable problem as the main wait: a previous
     * job found still running that then fails with a quota error must be reported, not retried by the
     * client for up to 12h.
     */
    @Test
    void shouldReportAQuotaErrorThatLandsDuringTheLookbackWait() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var jobId = JobId.of("project", "job_lookback_quota");
        var submissions = new AtomicInteger();
        var runningStatus = runningStatus();

        // First attempt fails transiently, so the retry takes the lookback path.
        var submitted = Mockito.mock(Job.class);
        Mockito.when(submitted.getJobId()).thenReturn(jobId);
        Mockito.when(submitted.getStatus()).thenReturn(runningStatus);

        // Still running when looked up, then terminal with a quota error.
        var stillRunning = Mockito.mock(Job.class);
        Mockito.when(stillRunning.getJobId()).thenReturn(jobId);
        Mockito.when(stillRunning.getStatus()).thenReturn(runningStatus);

        var quota = terminalJob("job_lookback_quota", new BigQueryError(
            "rateLimitExceeded", null, "Exceeded rate limits: too many table dml insert operations for this table."
        ));

        var connection = Mockito.mock(BigQuery.class);
        var reads = new AtomicInteger();
        Mockito.when(connection.getJob(jobId)).thenAnswer(invocation ->
            switch (reads.incrementAndGet()) {
                case 1 -> throw new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");
                case 2 -> stillRunning;
                default -> quota;
            }
        );

        var failure = failureOf(task, runContext, () ->
        {
            submissions.incrementAndGet();
            return submitted;
        }, connection);

        assertThat(failure.getErrors(), hasSize(1));
        assertThat(failure.getErrors().getFirst().getReason(), is("rateLimitExceeded"));
        assertThat(failure.getMessage(), containsString("too many table dml insert operations"));

        // The quota error was read as data and acted on -- the lookback found the previous job
        // failed and resubmitted -- rather than being retried inside the client until the deadline.
        assertThat(submissions.get(), greaterThan(1));
        Mockito.verify(stillRunning, Mockito.never()).waitFor();
    }

    /**
     * Job#waitFor() returns null when the job no longer exists, and the lookback used to dereference
     * that directly -- turning a recoverable "job is gone, resubmit" state into a non-retryable NPE.
     */
    @Test
    void shouldResubmitWhenThePreviousJobDisappearsDuringTheLookback() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var jobId = JobId.of("project", "job_lookback_vanished");
        var submissions = new AtomicInteger();
        var runningStatus = runningStatus();

        var submitted = Mockito.mock(Job.class);
        Mockito.when(submitted.getJobId()).thenReturn(jobId);
        Mockito.when(submitted.getStatus()).thenReturn(runningStatus);

        var stillRunning = Mockito.mock(Job.class);
        Mockito.when(stillRunning.getJobId()).thenReturn(jobId);
        Mockito.when(stillRunning.getStatus()).thenReturn(runningStatus);

        // Reads 3-5 are misses (the job vanished), after which the lookback gives up on it and the
        // task resubmits; the fresh job is then found and fails on its own terms.
        var afterResubmit = terminalJob("job_lookback_vanished", new BigQueryError("invalidQuery", null, "Syntax error"));

        var connection = Mockito.mock(BigQuery.class);
        var reads = new AtomicInteger();
        Mockito.when(connection.getJob(jobId)).thenAnswer(invocation ->
            switch (reads.incrementAndGet()) {
                case 1 -> throw new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");
                case 2 -> stillRunning;
                case 3, 4, 5 -> null;
                default -> afterResubmit;
            }
        );

        var failure = failureOf(task, runContext, () ->
        {
            submissions.incrementAndGet();
            return submitted;
        }, connection);

        // A vanished job is a recoverable state: resubmit, never a NullPointerException.
        assertThat(failure.getCause(), not(instanceOf(NullPointerException.class)));
        assertThat(failure.getErrors().getFirst().getReason(), is("invalidQuery"));
        assertThat(submissions.get(), greaterThan(1));
    }

    /**
     * A job we just submitted is only invisible because jobs.get has not caught up, so the main wait
     * must tolerate a run of misses. Capping it there would surface a null, and handleErrors(null)
     * raises an IllegalArgumentException that shouldRetry does not retry -- failing the task hard on
     * a purely transient condition, under exactly the concurrent load this class is about.
     */
    @Test
    void shouldKeepPollingTheMainWaitThroughSeveralConsecutiveMisses() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var jobId = JobId.of("project", "job_slow_to_appear");
        var reads = new AtomicInteger();

        var submittedStatus = runningStatus();

        var submitted = Mockito.mock(Job.class);
        Mockito.when(submitted.getJobId()).thenReturn(jobId);
        Mockito.when(submitted.getStatus()).thenReturn(submittedStatus);

        var finished = terminalJob("job_slow_to_appear", new BigQueryError("invalidQuery", null, "Syntax error"));

        var connection = Mockito.mock(BigQuery.class);
        // Five misses -- comfortably past MAX_CONSECUTIVE_JOB_MISSES -- then the job appears.
        Mockito.when(connection.getJob(jobId)).thenAnswer(invocation ->
            reads.incrementAndGet() <= 5 ? null : finished
        );

        var failure = failureOf(task, runContext, () -> submitted, connection);

        // It waited the job out rather than declaring it gone: the real error surfaced.
        assertThat(failure.getErrors().getFirst().getReason(), is("invalidQuery"));
        assertThat(failure.getCause(), not(instanceOf(IllegalArgumentException.class)));
        assertThat(reads.get(), greaterThan(5));
    }

    private BigQueryException failureOf(Query task, io.kestra.core.runners.RunContext runContext, java.util.concurrent.Callable<Job> createJob) {
        return failureOf(task, runContext, createJob, Mockito.mock(BigQuery.class));
    }

    /**
     * Completion is awaited by POLLING jobs.get, so a transient poll failure has to be injected
     * on the connection rather than on Job#waitFor(). Tests that exercise the poll therefore
     * need to hand in the connection they stubbed.
     */
    private BigQueryException failureOf(
        Query task,
        io.kestra.core.runners.RunContext runContext,
        java.util.concurrent.Callable<Job> createJob,
        BigQuery connection
    ) {
        var thrown = assertThrows(
            FailsafeException.class, () -> task.waitForJob(
                runContext.logger(),
                createJob,
                runContext,
                connection
            )
        );

        return (BigQueryException) thrown.getCause();
    }

    private Job runningJob(String id) {
        var status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getError()).thenReturn(null);
        Mockito.when(status.getState()).thenReturn(JobStatus.State.RUNNING);

        var job = Mockito.mock(Job.class);
        Mockito.when(job.getJobId()).thenReturn(JobId.of("project", id));
        Mockito.when(job.getStatus()).thenReturn(status);

        return job;
    }

    /** A job already in a terminal state, carrying {@code error} (null for a clean success). */
    private Job terminalJob(String id, BigQueryError error) {
        var status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getError()).thenReturn(error);
        // Left null deliberately: handleErrors() collects getError() AND getExecutionErrors(),
        // so echoing the same error in both would report it twice.
        Mockito.when(status.getExecutionErrors()).thenReturn(null);
        Mockito.when(status.getState()).thenReturn(JobStatus.State.DONE);

        var job = Mockito.mock(Job.class);
        Mockito.when(job.getJobId()).thenReturn(JobId.of("project", id));
        Mockito.when(job.getStatus()).thenReturn(status);

        return job;
    }

    private Query task() {
        return Query.builder()
            .id(BigQueryTransientErrorTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("SELECT 1"))
            // Per-instance so the poll loop does not sleep on real wall clock; no shared state.
            .jobPollInitialInterval(Duration.ofMillis(1))
            .jobPollMaxInterval(Duration.ofMillis(2))
            .retryAuto(
                Exponential.builder()
                    .type("exponential")
                    .interval(Duration.ofMillis(10))
                    .maxInterval(Duration.ofMillis(50))
                    .maxDuration(Duration.ofSeconds(5))
                    .maxAttempts(3)
                    .build()
            )
            .build();
    }
}
