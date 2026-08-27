package io.kestra.plugin.gcp.bigquery;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import dev.failsafe.FailsafeException;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * BigQuery reports job-level problems in an error list, but transport problems (a 5xx on the REST call,
 * a socket error, an interrupted poll) come back with that list null. Those used to be rethrown as an
 * empty "Bigquery Errors [ - ]" with no cause and no reason to retry on.
 *
 * @see <a href="https://github.com/kestra-io/plugin-gcp/issues/675">#675</a>
 */
@KestraTest
class BigQueryTransientErrorTest {
    @Inject
    private RunContextFactory runContextFactory;

    @AfterEach
    void clearInterruptFlag() {
        // waitForJob restores the interrupt flag on the calling thread, and JUnit reuses it for the next test.
        Thread.interrupted();
    }

    @Test
    void shouldDeriveRetryableReasonFromHttpStatusWhenErrorListIsMissing() {
        var exception = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");

        List<BigQueryError> errors = BigQueryService.errorsOf(exception);

        assertThat(errors, hasSize(1));
        assertThat(errors.getFirst().getReason(), is("backendError"));
        assertThat(errors.getFirst().getMessage(), is("The service is currently unavailable."));
    }

    @Test
    void shouldFallBackToTheCauseWhenTheExceptionCarriesNoMessage() {
        var cause = new IOException("Connection reset");
        var exception = new com.google.cloud.bigquery.BigQueryException(0, null, cause);

        List<BigQueryError> errors = BigQueryService.errorsOf(exception);

        assertThat(errors, hasSize(1));
        assertThat(errors.getFirst().getReason(), is("unknown"));
        assertThat(errors.getFirst().getMessage(), containsString("Connection reset"));
    }

    @Test
    void shouldKeepTheReportedErrorListUntouched() {
        var reported = new BigQueryError("invalidQuery", null, "Syntax error");
        var exception = new com.google.cloud.bigquery.BigQueryException(400, "Syntax error", reported);

        assertThat(BigQueryService.errorsOf(exception), is(List.of(reported)));
    }

    @Test
    void shouldReportTheJobIdAndStopRetryingWhenTheWaitIsInterrupted() throws Exception {
        Query task = task();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        BigQuery connection = Mockito.mock(BigQuery.class);
        AtomicInteger submissions = new AtomicInteger();

        // How the client surfaces an interrupt raised while polling: no error list, no HTTP code.
        var interrupted = new com.google.cloud.bigquery.BigQueryException(
            0,
            "java.lang.InterruptedException",
            new InterruptedException()
        );

        FailsafeException thrown = assertThrows(
            FailsafeException.class, () -> task.waitForJob(
                runContext.logger(),
                () ->
                {
                    submissions.incrementAndGet();
                    throw interrupted;
                },
                runContext,
                connection
            )
        );

        assertThat(thrown.getCause(), instanceOf(BigQueryException.class));
        BigQueryException failure = (BigQueryException) thrown.getCause();

        assertThat(failure.getErrors(), hasSize(1));
        assertThat(failure.getErrors().getFirst().getReason(), is("interrupted"));
        assertThat(failure.getErrors().getFirst().getMessage(), containsString("may still be running on BigQuery"));
        assertThat(failure.getCause(), is(interrupted));

        // An interrupted thread cannot make progress, so the submission must not be replayed.
        assertThat(submissions.get(), is(1));
        assertThat(Thread.currentThread().isInterrupted(), is(true));
    }

    @Test
    void shouldKeepTheOriginalExceptionAsTheCauseOfAnErrorlessFailure() throws Exception {
        Query task = task();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        BigQuery connection = Mockito.mock(BigQuery.class);

        var unavailable = new com.google.cloud.bigquery.BigQueryException(503, "The service is currently unavailable.");

        FailsafeException thrown = assertThrows(
            FailsafeException.class, () -> task.waitForJob(
                runContext.logger(),
                () ->
                {
                    throw unavailable;
                },
                runContext,
                connection
            )
        );

        BigQueryException failure = (BigQueryException) thrown.getCause();

        assertThat(failure.getMessage(), containsString("The service is currently unavailable."));
        assertThat(failure.getCause(), is(unavailable));
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
