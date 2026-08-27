package io.kestra.plugin.gcp.bigquery;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

    @AfterEach
    void clearInterruptFlag() {
        // waitForJob restores the interrupt flag, and JUnit reuses this thread.
        Thread.interrupted();
    }

    @ParameterizedTest
    @CsvSource(
        {
            "429, rateLimitExceeded",
            "500, internalError",
            "502, backendError",
            "503, backendError",
            "504, backendError",
            "403, unknown",
            "400, unknown"
        }
    )
    void shouldDeriveTheReasonFromTheHttpStatusWhenTheErrorListIsMissing(int code, String expectedReason) {
        var exception = new com.google.cloud.bigquery.BigQueryException(code, "boom");

        List<BigQueryError> errors = BigQueryService.errorsOf(exception, true);

        assertThat(errors, hasSize(1));
        assertThat(errors.getFirst().getReason(), is(expectedReason));
        assertThat(errors.getFirst().getMessage(), is("boom"));
    }

    @Test
    void shouldNotInferAReasonWhenARetryCannotBeDeduplicated() {
        var exception = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");

        List<BigQueryError> errors = BigQueryService.errorsOf(exception, false);

        assertThat(errors.getFirst().getReason(), is("unknown"));
        assertThat(errors.getFirst().getMessage(), is("The service is currently unavailable."));
    }

    @Test
    void shouldFallBackToTheCauseWhenTheExceptionCarriesNoMessage() {
        var cause = new IOException("Connection reset");
        var exception = new com.google.cloud.bigquery.BigQueryException(0, null, cause);

        List<BigQueryError> errors = BigQueryService.errorsOf(exception, true);

        assertThat(errors, hasSize(1));
        assertThat(errors.getFirst().getReason(), is("unknown"));
        assertThat(errors.getFirst().getMessage(), containsString("Connection reset"));
    }

    @Test
    void shouldKeepTheReportedErrorListUntouched() {
        var reported = new BigQueryError("invalidQuery", null, "Syntax error");
        var exception = new com.google.cloud.bigquery.BigQueryException(400, "Syntax error", reported);

        assertThat(BigQueryService.errorsOf(exception, true), is(List.of(reported)));
    }

    @Test
    void shouldNotRetryASubmissionThatFailedWithoutAnErrorList() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var submissions = new AtomicInteger();

        // Nothing here can tell an accepted job from a lost one, so a retry could run the statement twice.
        var unavailable = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");

        var thrown = assertThrows(
            FailsafeException.class, () -> task.waitForJob(
                runContext.logger(),
                () ->
                {
                    submissions.incrementAndGet();
                    throw unavailable;
                },
                runContext,
                Mockito.mock(BigQuery.class)
            )
        );

        var failure = (BigQueryException) thrown.getCause();

        assertThat(failure.getErrors().getFirst().getReason(), is("unknown"));
        assertThat(failure.getMessage(), containsString("The service is currently unavailable."));
        assertThat(failure.getCause(), is(unavailable));
        assertThat(submissions.get(), is(1));
    }

    @Test
    void shouldNameTheJobAndStopRetryingWhenThePollIsInterrupted() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var submissions = new AtomicInteger();

        // Job#waitFor declares InterruptedException raw, on top of the wrapped shape the client also uses.
        var job = runningJob("job_interrupted");
        Mockito.when(job.waitFor()).thenThrow(new InterruptedException());

        var thrown = assertThrows(
            FailsafeException.class, () -> task.waitForJob(
                runContext.logger(),
                () ->
                {
                    submissions.incrementAndGet();
                    return job;
                },
                runContext,
                Mockito.mock(BigQuery.class)
            )
        );

        var failure = (BigQueryException) thrown.getCause();

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

        var thrown = assertThrows(
            FailsafeException.class, () -> task.waitForJob(
                runContext.logger(),
                () ->
                {
                    throw new com.google.cloud.bigquery.BigQueryException(0, "java.lang.InterruptedException", new InterruptedException());
                },
                runContext,
                Mockito.mock(BigQuery.class)
            )
        );

        var failure = (BigQueryException) thrown.getCause();

        assertThat(failure.getErrors().getFirst().getMessage(), containsString("the job was cancelled"));
        assertThat(failure.getErrors().getFirst().getMessage(), not(containsString("may still be running")));
    }

    @Test
    void shouldKeepTheOriginalExceptionAsTheCauseOfAnErrorlessPollFailure() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var unavailable = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");
        var job = runningJob("job_poll_failed");
        Mockito.when(job.waitFor()).thenThrow(unavailable);

        var thrown = assertThrows(
            FailsafeException.class, () -> task.waitForJob(
                runContext.logger(),
                () -> job,
                runContext,
                Mockito.mock(BigQuery.class)
            )
        );

        var failure = (BigQueryException) thrown.getCause();

        assertThat(failure.getCause(), instanceOf(com.google.cloud.bigquery.BigQueryException.class));
        assertThat(failure.getMessage(), containsString("The service is currently unavailable."));
        // The job id is known here, so the reason is inferred and the retry policy can act on it.
        assertThat(failure.getErrors().getFirst().getReason(), is("backendError"));
    }

    private Job runningJob(String id) {
        var status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getError()).thenReturn(null);

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
