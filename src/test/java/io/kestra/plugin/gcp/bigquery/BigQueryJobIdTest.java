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
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
        assertThat(jobId.getJob(), is("kestra_" + runContext.taskRunInfo().taskRunId()));
    }

    @Test
    void shouldGiveTheSameJobIdToTwoContextsBuiltForTheSameTaskrun() throws Exception {
        // A resubmit builds a fresh RunContext on another worker, so the two must agree. Two separately
        // built contexts, not one context asked twice, which a pure function would satisfy trivially.
        var first = BigQueryService.jobId(contextOf("taskrun-1"), task()).getJob();
        var second = BigQueryService.jobId(contextOf("taskrun-1"), task()).getJob();

        assertThat(first, is("kestra_taskrun-1"));
        assertThat(second, is(first));
    }

    @Test
    void shouldLeaveTheJobIdUnsetWithoutATaskrun() throws Exception {
        // A trigger evaluates outside any execution. Deriving an id from a missing taskrun once gave every
        // poll of every trigger the literal "kestra_null_null", so the first job was adopted forever.
        var jobId = BigQueryService.jobId(runContextFactory.of(ImmutableMap.of()), task());

        assertThat("BigQuery must assign a random id when there is no taskrun to key on", jobId.getJob(), nullValue());
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
    void shouldAdoptAnAlreadyFinishedJobWhenTheIdIsAlreadyTaken() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var finished = job(null, JobStatus.State.DONE);

        // The resubmit can land after the original already succeeded, and its result is the answer.
        Mockito.when(connection.create(jobInfo)).thenThrow(conflict());
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(finished);

        var adopted = BigQueryService.createOrAdoptJob(connection, jobInfo, logger());

        assertThat("a finished job with no error is the outcome, not a reason to rerun", adopted, is(finished));
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
        assertThat("a burnt id is replaced by the next deterministic one", submitted.get().getJobId().getJob(), is("kestra_exec_taskrun_1"));
        assertThat(submitted.get().getJobId().getProject(), is("my-project"));
        assertThat(submitted.get().getJobId().getLocation(), is("EU"));
    }

    // The client does not surface the 409 for a caller-supplied id: BigQueryImpl.create catches "Already Exists"
    // and returns getJob(id, fields(STATISTICS)) for a job under 24 hours old, i.e. a creation time and no status.
    // adoptedByClient models that shape, which is what a task retry after a real failure receives.

    @Test
    void shouldStartAFreshJobWhenTheClientHandsBackTheOldFailedJob() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var adopted = adoptedByClient(jobInfo.getJobId(), System.currentTimeMillis() - 60_000);
        var oldFailure = job(new BigQueryError("rateLimitExceeded", null, "too many table dml insert operations"));
        var replacement = job(null);
        var submitted = new AtomicReference<JobInfo>();

        Mockito.when(connection.create(jobInfo)).thenReturn(adopted);
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(oldFailure);
        Mockito.when(connection.create(Mockito.<JobInfo> argThat(info -> info != null && !jobInfo.equals(info))))
            .thenAnswer(invocation ->
            {
                submitted.set(invocation.getArgument(0));
                return replacement;
            });

        var created = BigQueryService.createOrAdoptJob(connection, jobInfo, logger());

        assertThat("a retry must run again, not re-report the previous attempt's failure", created, is(replacement));
        assertThat("the retry keeps the kestra_ prefix and its taskrun", submitted.get().getJobId().getJob(), is("kestra_taskrun_1"));
        assertThat(submitted.get().getJobId().getLocation(), is("EU"));
        Mockito.verify(connection, Mockito.times(2)).create(Mockito.any(JobInfo.class));
    }

    @Test
    void shouldWalkTheChainPastEveryFailedRetry() {
        // Attempt 3: both kestra_taskrun and kestra_taskrun_1 already ran and failed.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var first = withJob(jobInfo, "kestra_taskrun_1");
        var adopted0 = adoptedByClient(jobInfo.getJobId(), System.currentTimeMillis() - 120_000);
        var adopted1 = adoptedByClient(first.getJobId(), System.currentTimeMillis() - 60_000);
        var failed0 = job(new BigQueryError("rateLimitExceeded", null, "x"));
        var failed1 = job(new BigQueryError("rateLimitExceeded", null, "x"));
        var replacement = job(null);
        var submitted = new AtomicReference<JobInfo>();

        Mockito.when(connection.create(jobInfo)).thenReturn(adopted0);
        Mockito.when(connection.create(first)).thenReturn(adopted1);
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(failed0);
        Mockito.when(connection.getJob(first.getJobId())).thenReturn(failed1);
        Mockito.when(connection.create(Mockito.<JobInfo> argThat(i -> i != null && !jobInfo.equals(i) && !first.equals(i))))
            .thenAnswer(invocation ->
            {
                submitted.set(invocation.getArgument(0));
                return replacement;
            });

        assertThat(BigQueryService.createOrAdoptJob(connection, jobInfo, logger()), is(replacement));
        assertThat(submitted.get().getJobId().getJob(), is("kestra_taskrun_2"));
    }

    @Test
    void shouldAdoptTheRetrysJobWhenAWorkerIsLostDuringTheRetry() {
        // #688's guarantee must hold for retries too: the retry's job is still running, so a resubmit
        // re-derives kestra_taskrun_1 and adopts it instead of starting a duplicate.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var first = withJob(jobInfo, "kestra_taskrun_1");
        var adopted0 = adoptedByClient(jobInfo.getJobId(), System.currentTimeMillis() - 120_000);
        var adopted1 = adoptedByClient(first.getJobId(), System.currentTimeMillis() - 60_000);
        var failed0 = job(new BigQueryError("rateLimitExceeded", null, "x"));
        var running1 = job(null, JobStatus.State.RUNNING);

        Mockito.when(connection.create(jobInfo)).thenReturn(adopted0);
        Mockito.when(connection.create(first)).thenReturn(adopted1);
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(failed0);
        Mockito.when(connection.getJob(first.getJobId())).thenReturn(running1);

        assertThat(BigQueryService.createOrAdoptJob(connection, jobInfo, logger()), is(adopted1));
        Mockito.verify(connection, Mockito.times(2)).create(Mockito.any(JobInfo.class));
    }

    private static JobInfo withJob(JobInfo jobInfo, String job) {
        var id = jobInfo.getJobId();
        return JobInfo.newBuilder(jobInfo.getConfiguration())
            .setJobId(JobId.newBuilder().setProject(id.getProject()).setLocation(id.getLocation()).setJob(job).build())
            .build();
    }

    @Test
    void shouldResubmitAnAdoptedFailureEvenWithASubSecondRetryInterval() {
        // Created 500 ms ago, inside the margin: only the adopted job's shape (no status) can tell it apart.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var adopted = adoptedByClient(jobInfo.getJobId(), System.currentTimeMillis() - 500);
        var failed = job(new BigQueryError("rateLimitExceeded", null, "x"));
        var replacement = job(null);
        var submitted = new AtomicReference<JobInfo>();

        Mockito.when(connection.create(jobInfo)).thenReturn(adopted);
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(failed);
        Mockito.when(connection.create(Mockito.<JobInfo> argThat(info -> info != null && !jobInfo.equals(info))))
            .thenAnswer(invocation ->
            {
                submitted.set(invocation.getArgument(0));
                return replacement;
            });

        assertThat(BigQueryService.createOrAdoptJob(connection, jobInfo, logger()), is(replacement));
        assertThat(submitted.get().getJobId().getJob(), is("kestra_taskrun_1"));
    }

    @Test
    void shouldTreatAJobCreatedWithinTheMarginAsThisSubmissions() {
        // Created 1 s ago, inside the 2 s margin: clock skew must not turn a fresh failure into a resubmit.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var justCreated = job(new BigQueryError("invalidQuery", null, "Syntax error"), JobStatus.State.DONE, System.currentTimeMillis() - 1_000);

        Mockito.when(connection.create(jobInfo)).thenReturn(justCreated);

        assertThat(BigQueryService.createOrAdoptJob(connection, jobInfo, logger()), is(justCreated));
        Mockito.verify(connection, Mockito.times(1)).create(Mockito.any(JobInfo.class));
    }

    @Test
    void shouldResubmitAJobWithAStatusCreatedOutsideTheMargin() {
        // The other side of the margin: a job carrying a status, created 3 s ago, is a previous attempt's.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var stale = job(new BigQueryError("rateLimitExceeded", null, "x"), JobStatus.State.DONE, System.currentTimeMillis() - 3_000);
        var replacement = job(null);

        Mockito.when(connection.create(jobInfo)).thenReturn(stale);
        Mockito.when(connection.create(Mockito.<JobInfo> argThat(info -> info != null && !jobInfo.equals(info)))).thenReturn(replacement);

        assertThat(BigQueryService.createOrAdoptJob(connection, jobInfo, logger()), is(replacement));
    }

    @Test
    void shouldLeaveAJobThatVanishedAfterAdoptionToThePoll() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var adopted = adoptedByClient(jobInfo.getJobId(), System.currentTimeMillis() - 60_000);

        Mockito.when(connection.create(jobInfo)).thenReturn(adopted);
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(null);

        assertThat(BigQueryService.createOrAdoptJob(connection, jobInfo, logger()), is(adopted));
        Mockito.verify(connection, Mockito.times(1)).create(Mockito.any(JobInfo.class));
    }

    @Test
    void shouldReportAJobThatFailedAtThisSubmission() {
        // A statement can fail the moment it is submitted (a syntax error). That job was created by this
        // call, so its failure is the answer; resubmitting it would only run a broken statement twice.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var freshFailure = job(
            new BigQueryError("invalidQuery", null, "Syntax error"),
            JobStatus.State.DONE, System.currentTimeMillis()
        );

        Mockito.when(connection.create(jobInfo)).thenReturn(freshFailure);

        var created = BigQueryService.createOrAdoptJob(connection, jobInfo, logger());

        assertThat(created, is(freshFailure));
        Mockito.verify(connection, Mockito.times(1)).create(Mockito.any(JobInfo.class));
    }

    @Test
    void shouldKeepAdoptingAnOldJobThatIsStillRunning() {
        // The worker-loss resubmit #688 exists for: the original job is still running and must be adopted.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var adopted = adoptedByClient(jobInfo.getJobId(), System.currentTimeMillis() - 600_000);

        var full = job(null, JobStatus.State.RUNNING);
        Mockito.when(connection.create(jobInfo)).thenReturn(adopted);
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(full);

        var created = BigQueryService.createOrAdoptJob(connection, jobInfo, logger());

        assertThat("never duplicate work that is still in flight", created, is(adopted));
        Mockito.verify(connection, Mockito.times(1)).create(Mockito.any(JobInfo.class));
    }

    @Test
    void shouldKeepAdoptingAnOldJobThatSucceeded() {
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = jobInfo("kestra_taskrun");
        var adopted = adoptedByClient(jobInfo.getJobId(), System.currentTimeMillis() - 600_000);

        var full = job(null, JobStatus.State.DONE);
        Mockito.when(connection.create(jobInfo)).thenReturn(adopted);
        Mockito.when(connection.getJob(jobInfo.getJobId())).thenReturn(full);

        var created = BigQueryService.createOrAdoptJob(connection, jobInfo, logger());

        assertThat("a finished job with no error is the outcome, not a reason to rerun", created, is(adopted));
        Mockito.verify(connection, Mockito.times(1)).create(Mockito.any(JobInfo.class));
    }

    @Test
    void shouldNotResubmitWhenBigQueryAssignedTheId() {
        // Without our own id there is nothing to adopt, so whatever came back is this submission's job.
        var connection = Mockito.mock(BigQuery.class);
        var jobInfo = JobInfo.newBuilder(CONFIGURATION)
            .setJobId(JobId.newBuilder().setProject("my-project").setLocation("EU").build())
            .build();
        var failure = job(new BigQueryError("invalidQuery", null, "boom"), JobStatus.State.DONE, System.currentTimeMillis() - 60_000);

        Mockito.when(connection.create(jobInfo)).thenReturn(failure);

        assertThat(BigQueryService.createOrAdoptJob(connection, jobInfo, logger()), is(failure));
        Mockito.verify(connection, Mockito.times(1)).create(Mockito.any(JobInfo.class));
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

    @Test
    void shouldRefuseToReportOnAJobWithoutAStatus() {
        // Never silently pass a job whose state is unknown: a dry run is not polled afterwards, so this
        // is its only check.
        var job = Mockito.mock(Job.class);
        Mockito.when(job.getStatus()).thenReturn(null);
        Mockito.when(job.getJobId()).thenReturn(JobId.of("my-project", "kestra_taskrun"));

        var thrown = assertThrows(IllegalStateException.class, () -> BigQueryService.handleErrors(job, logger()));

        assertThat(thrown.getMessage(), containsString("no status to report"));
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
        return job(error, error == null ? JobStatus.State.RUNNING : JobStatus.State.DONE);
    }

    private static Job job(BigQueryError error, JobStatus.State state) {
        var status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getError()).thenReturn(error);
        Mockito.when(status.getState()).thenReturn(state);

        var job = Mockito.mock(Job.class);
        Mockito.when(job.getStatus()).thenReturn(status);

        return job;
    }

    /** What BigQueryImpl.create hands back for a taken id: statistics only, no status. */
    private static Job adoptedByClient(JobId jobId, long creationTimeMs) {
        var statistics = Mockito.mock(com.google.cloud.bigquery.JobStatistics.class);
        Mockito.when(statistics.getCreationTime()).thenReturn(creationTimeMs);
        var job = Mockito.mock(Job.class);
        Mockito.when(job.getStatus()).thenReturn(null);
        Mockito.when(job.getStatistics()).thenReturn(statistics);
        Mockito.when(job.getJobId()).thenReturn(jobId);

        return job;
    }

    private static Job job(BigQueryError error, JobStatus.State state, long creationTimeMs) {
        var job = job(error, state);
        var statistics = Mockito.mock(com.google.cloud.bigquery.JobStatistics.class);
        Mockito.when(statistics.getCreationTime()).thenReturn(creationTimeMs);
        Mockito.when(job.getStatistics()).thenReturn(statistics);

        return job;
    }

    private org.slf4j.Logger logger() {
        return org.slf4j.LoggerFactory.getLogger(BigQueryJobIdTest.class);
    }

    private RunContext contextOf(String taskRunId) {
        return runContextFactory.of(ImmutableMap.of("taskrun", ImmutableMap.of("id", taskRunId)));
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
