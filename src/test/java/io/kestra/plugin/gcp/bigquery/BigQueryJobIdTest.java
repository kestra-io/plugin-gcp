package io.kestra.plugin.gcp.bigquery;

import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A random job id let a worker-loss resubmit start a second job doing the same work.
 *
 * @see <a href="https://github.com/kestra-io/plugin-gcp/issues/674">#674</a>
 */
@KestraTest
class BigQueryJobIdTest {
    private static final QueryJobConfiguration CONFIGURATION = QueryJobConfiguration.newBuilder("SELECT 1").build();

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldDeriveTheJobIdFromTheTaskrun() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var jobId = BigQueryService.jobId(runContext, task);

        assertThat(jobId.getJob(), notNullValue());
        assertThat(jobId.getJob(), startsWith("kestra_"));
        assertThat(jobId.getJob(), is("kestra_" + runContext.taskRunInfo().executionId() + "_" + runContext.taskRunInfo().taskRunId()));
    }

    @Test
    void shouldGiveTheSameJobIdToEveryAttemptOfATaskrun() throws Exception {
        var task = task();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        // The whole point: a resubmit on another worker must land on the id the lost worker used.
        // Compare the job segment, not the whole JobId: two absent ids also compare equal.
        var first = BigQueryService.jobId(runContext, task).getJob();
        var second = BigQueryService.jobId(runContext, task).getJob();

        assertThat(first, notNullValue());
        assertThat(second, is(first));
    }

    @Test
    void shouldGiveDifferentJobIdsToDifferentTaskruns() throws Exception {
        var task = task();
        var first = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var second = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        assertThat(BigQueryService.jobId(first, task).getJob(), not(BigQueryService.jobId(second, task).getJob()));
    }

    @Test
    void shouldAdoptTheRunningJobWhenTheIdIsAlreadyTaken() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_exec_taskrun");
        var running = job(null);

        Mockito.when(connection.create(jobInfo)).thenThrow(conflict());
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(running);

        var adopted = BigQueryService.createOrAdoptJob(connection, jobInfo, logger());

        assertThat("the job the lost worker started must be adopted, not duplicated", adopted, is(running));
        Mockito.verify(connection, Mockito.times(1)).create(Mockito.any(JobInfo.class));
    }

    @Test
    void shouldStartAFreshJobWhenTheTakenIdBelongsToAFailedJob() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_exec_taskrun");
        var replacement = job(null);
        var failed = job(new BigQueryError("invalidQuery", null, "Syntax error"));
        var submitted = new AtomicReference<JobInfo>();

        // BigQuery reserves the id for good, so a retry can only make progress with a new one.
        Mockito.when(connection.create(jobInfo)).thenThrow(conflict());
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(failed);
        Mockito.when(connection.create(Mockito.<JobInfo> argThat(info -> info != null && !jobInfo.equals(info))))
            .thenAnswer(invocation ->
            {
                submitted.set(invocation.getArgument(0));
                return replacement;
            });

        var created = BigQueryService.createOrAdoptJob(connection, jobInfo, logger());

        assertThat(created, is(replacement));
        assertThat("a burnt id must be replaced by one BigQuery assigns", submitted.get().getJobId().getJob(), nullValue());
        assertThat(submitted.get().getJobId().getProject(), is("my-project"));
        assertThat(submitted.get().getJobId().getLocation(), is("EU"));
    }

    @Test
    void shouldRethrowAConflictWhenTheJobCannotBeFetched() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_exec_taskrun");

        Mockito.when(connection.create(jobInfo)).thenThrow(conflict());
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(null);

        var thrown = assertThrows(
            com.google.cloud.bigquery.BigQueryException.class,
            () -> BigQueryService.createOrAdoptJob(connection, jobInfo, logger())
        );

        assertThat(thrown.getCode(), is(HttpURLConnection.HTTP_CONFLICT));
    }

    @Test
    void shouldNotSwallowAnErrorThatIsNotAConflict() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_exec_taskrun");

        Mockito.when(connection.create(jobInfo))
            .thenThrow(new com.google.cloud.bigquery.BigQueryException(403, "Access Denied"));

        var thrown = assertThrows(
            com.google.cloud.bigquery.BigQueryException.class,
            () -> BigQueryService.createOrAdoptJob(connection, jobInfo, logger())
        );

        assertThat(thrown.getCode(), is(403));
        Mockito.verify(connection, Mockito.never()).getJob(Mockito.any(JobId.class));
    }

    private static com.google.cloud.bigquery.BigQueryException conflict() {
        return new com.google.cloud.bigquery.BigQueryException(
            HttpURLConnection.HTTP_CONFLICT,
            "Already Exists: Job my-project:kestra_exec_taskrun"
        );
    }

    private static JobInfo jobInfo(String job) {
        return JobInfo.newBuilder(CONFIGURATION)
            .setJobId(JobId.newBuilder().setProject("my-project").setLocation("EU").setJob(job).build())
            .build();
    }

    private static Job job(BigQueryError error) {
        var status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getError()).thenReturn(error);
        Mockito.when(status.getState()).thenReturn(error == null ? JobStatus.State.RUNNING : JobStatus.State.DONE);

        var job = Mockito.mock(Job.class);
        Mockito.when(job.getStatus()).thenReturn(status);

        return job;
    }

    private org.slf4j.Logger logger() {
        return org.slf4j.LoggerFactory.getLogger(BigQueryJobIdTest.class);
    }

    private Query task() {
        return Query.builder()
            .id(BigQueryJobIdTest.class.getSimpleName())
            .type(Query.class.getName())
            .projectId(Property.ofValue("my-project"))
            .location(Property.ofValue("EU"))
            .sql(Property.ofValue("SELECT 1"))
            .build();
    }
}
