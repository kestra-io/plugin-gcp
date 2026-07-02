package io.kestra.plugin.gcp.bigquery;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
@WireMockTest
public class QueryErrorTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    private static final String JOB_RESPONSE_DONE = """
        {
          "kind": "bigquery#job",
          "etag": "\\"abcdef1234567890\\"",
          "id": "kestra-unit-test:europe-west3.job_1234567890abcdef",
          "jobReference": {
            "projectId": "kestra-unit-test",
            "jobId": "job_1234567890abcdef",
            "location": "europe-west3"
          },
          "configuration": {
            "query": {
              "query": "SELECT * FROM `kestra-unit-test.my_dataset.my_table`",
              "destinationTable": {
                "projectId": "kestra-unit-test",
                "datasetId": "_temp_dataset",
                "tableId": "anon1234567890abcdef"
              },
              "useLegacySql": false
            },
            "jobType": "QUERY"
          },
          "status": { "state": "DONE" },
          "statistics": {
            "creationTime": "1697123456789",
            "startTime": "1697123457000",
            "endTime": "1697123459000",
            "query": { "statementType": "SELECT" }
          }
        }""";

    private static final String JOB_RESPONSE_RUNNING = """
        {
          "kind": "bigquery#job",
          "etag": "\\"abcdef1234567890\\"",
          "id": "kestra-unit-test:europe-west3.job_dup_test",
          "jobReference": {
            "projectId": "kestra-unit-test",
            "jobId": "job_dup_test",
            "location": "europe-west3"
          },
          "configuration": {
            "query": {
              "query": "SELECT * FROM `kestra-unit-test.my_dataset.my_table`",
              "useLegacySql": false
            },
            "jobType": "QUERY"
          },
          "status": { "state": "RUNNING" },
          "statistics": {
            "creationTime": "1697123456789",
            "startTime": "1697123457000",
            "query": { "statementType": "SELECT" }
          }
        }""";

    private static final String JOB_RESPONSE_DUP_TEST_DONE = """
        {
          "kind": "bigquery#job",
          "etag": "\\"abcdef1234567890\\"",
          "id": "kestra-unit-test:europe-west3.job_dup_test",
          "jobReference": {
            "projectId": "kestra-unit-test",
            "jobId": "job_dup_test",
            "location": "europe-west3"
          },
          "configuration": {
            "query": {
              "query": "SELECT * FROM `kestra-unit-test.my_dataset.my_table`",
              "useLegacySql": false
            },
            "jobType": "QUERY"
          },
          "status": { "state": "DONE" },
          "statistics": {
            "creationTime": "1697123456789",
            "startTime": "1697123457000",
            "endTime": "1697123459000",
            "query": { "statementType": "SELECT" }
          }
        }""";

    private static final String JOB_RESPONSE_DUP_TEST_DONE_WITH_ERROR = """
        {
          "kind": "bigquery#job",
          "etag": "\\"abcdef1234567890\\"",
          "id": "kestra-unit-test:europe-west3.job_dup_test",
          "jobReference": {
            "projectId": "kestra-unit-test",
            "jobId": "job_dup_test",
            "location": "europe-west3"
          },
          "configuration": {
            "query": {
              "query": "SELECT * FROM `kestra-unit-test.my_dataset.my_table`",
              "useLegacySql": false
            },
            "jobType": "QUERY"
          },
          "status": {
            "state": "DONE",
            "errorResult": {
              "reason": "backendError",
              "message": "Backend error while processing the job"
            }
          },
          "statistics": {
            "creationTime": "1697123456789",
            "startTime": "1697123457000",
            "endTime": "1697123459000",
            "query": { "statementType": "SELECT" }
          }
        }""";

    // Job#waitFor() polls the /queries/{jobId} endpoint, which returns a GetQueryResultsResponse,
    // not a Job resource: it needs "jobComplete" to be seen as done, unlike the /jobs/{jobId} shapes above.
    private static final String QUERY_RESULTS_RESPONSE_COMPLETE = """
        {
          "kind": "bigquery#getQueryResultsResponse",
          "etag": "\\"abcdef1234567890\\"",
          "jobReference": {
            "projectId": "kestra-unit-test",
            "jobId": "job_dup_test",
            "location": "europe-west3"
          },
          "totalRows": "0",
          "jobComplete": true,
          "cacheHit": false
        }""";

    private static final String JOB_NOT_FOUND_RESPONSE = """
        {
          "error": {
            "code": 404,
            "errors": [
              {
                "domain": "global",
                "message": "Not found: Job kestra-unit-test:europe-west3.job_dup_test",
                "reason": "notFound"
              }
            ],
            "message": "Not found: Job kestra-unit-test:europe-west3.job_dup_test",
            "status": "NOT_FOUND"
          }
        }""";

    private static final String BACKEND_ERROR_RESPONSE = """
        {
          "error": {
            "code": 503,
            "errors": [
              {
                "domain": "global",
                "message": "Visibility check was unavailable. Please retry the request and contact support if the problem persists",
                "reason": "backendError"
              }
            ],
            "message": "Visibility check was unavailable. Please retry the request and contact support if the problem persists",
            "status": "UNAVAILABLE"
          }
        }""";

    @Test
    void shouldRetryOnBackendError(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        String jobsPath = "/bigquery/v2/projects/.*/jobs";

        stubFor(post(urlPathMatching(jobsPath))
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody(BACKEND_ERROR_RESPONSE)));

        Query task = buildQuery(wmRuntimeInfo, 3);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        assertThrows(Exception.class, () -> task.run(runContext));

        verify(3, postRequestedFor(urlPathMatching(jobsPath)));
    }

    @Test
    void shouldRetryOnBackendErrorWhenFetchingResults(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        String jobsPath = "/bigquery/v2/projects/.*/jobs";
        String resultsPath = "/bigquery/v2/projects/.*/queries/job_1234567890abcdef";

        // Job creation and polling succeed
        stubFor(post(urlPathMatching(jobsPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DONE)));

        stubFor(get(urlPathMatching("/bigquery/v2/projects/.*/jobs/job_1234567890abcdef"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DONE)));

        // Result fetching returns 503
        stubFor(get(urlPathMatching(resultsPath))
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody(BACKEND_ERROR_RESPONSE)));

        Query task = buildQuery(wmRuntimeInfo, 3);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        assertThrows(Exception.class, () -> task.run(runContext));

        // job.waitFor() polls the same results endpoint and fails once before AbstractBigquery#waitForJob
        // finds the already-completed job via getJob() and short-circuits without submitting a duplicate job;
        // the 3 remaining hits come from the dedicated "fetch results" retry loop in Query#run.
        verify(4, getRequestedFor(urlPathMatching(resultsPath)));
        verify(1, postRequestedFor(urlPathMatching(jobsPath)));
    }

    @Test
    void shouldNotDuplicateJobWhenPollingFailsAfterJobActuallySucceeded(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        String jobsPath = "/bigquery/v2/projects/.*/jobs";
        // job.waitFor() polls a query job via the queries endpoint (maxResults=0), not jobs/{id}.
        String pollingPath = "/bigquery/v2/projects/.*/queries/job_dup_test.*";
        // our fix looks up the last submitted job's status via BigQuery#getJob(JobId), which hits jobs/{id}.
        String jobStatusPath = "/bigquery/v2/projects/.*/jobs/job_dup_test.*";

        stubFor(post(urlPathMatching(jobsPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_RUNNING)));

        // The poll consistently fails with a transient error, even though the job actually succeeded.
        stubFor(get(urlPathMatching(pollingPath))
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody(BACKEND_ERROR_RESPONSE)));

        // The job's real status, fetched directly, shows it already completed successfully.
        stubFor(get(urlPathMatching(jobStatusPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DUP_TEST_DONE)));

        Query task = buildQueryWithoutFetch(wmRuntimeInfo, 3);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Query.Output output = task.run(runContext);

        assertEquals("job_dup_test", output.getJobId());
        verify(1, postRequestedFor(urlPathMatching(jobsPath)));
    }

    @Test
    void shouldNotDuplicateJobWhenPreviousJobStillRunning(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        String jobsPath = "/bigquery/v2/projects/.*/jobs";
        String pollingPath = "/bigquery/v2/projects/.*/queries/job_dup_test.*";
        String jobStatusPath = "/bigquery/v2/projects/.*/jobs/job_dup_test.*";

        stubFor(post(urlPathMatching(jobsPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_RUNNING)));

        // The job's own first poll fails transiently; once the retry looks the job up directly and
        // finds it still running, it calls waitFor() again, which this time observes completion.
        stubFor(get(urlPathMatching(pollingPath))
            .inScenario("still-running")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody(BACKEND_ERROR_RESPONSE))
            .willSetStateTo("completed"));

        stubFor(get(urlPathMatching(pollingPath))
            .inScenario("still-running")
            .whenScenarioStateIs("completed")
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(QUERY_RESULTS_RESPONSE_COMPLETE)));

        // The job's real status, fetched directly, shows it is still running.
        stubFor(get(urlPathMatching(jobStatusPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_RUNNING)));

        Query task = buildQueryWithoutFetch(wmRuntimeInfo, 3);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Query.Output output;
        try {
            output = task.run(runContext);
        } catch (Exception e) {
            throw e;
        }

        assertEquals("job_dup_test", output.getJobId());
        verify(1, postRequestedFor(urlPathMatching(jobsPath)));
    }

    @Test
    void shouldCreateNewJobWhenPreviousJobDoneWithError(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        String jobsPath = "/bigquery/v2/projects/.*/jobs";
        String pollingPath = "/bigquery/v2/projects/.*/queries/job_dup_test.*";
        String jobStatusPath = "/bigquery/v2/projects/.*/jobs/job_dup_test.*";

        stubFor(post(urlPathMatching(jobsPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_RUNNING)));

        // The original job's poll fails transiently; the fresh job submitted after the lookback
        // then polls successfully.
        stubFor(get(urlPathMatching(pollingPath))
            .inScenario("done-with-error")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody(BACKEND_ERROR_RESPONSE))
            .willSetStateTo("completed"));

        stubFor(get(urlPathMatching(pollingPath))
            .inScenario("done-with-error")
            .whenScenarioStateIs("completed")
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(QUERY_RESULTS_RESPONSE_COMPLETE)));

        // The job's real status, fetched directly, shows it actually failed: no dedup, a new job is submitted.
        // Job#waitFor() itself calls reload() (another GET on this same path) once the fresh job completes,
        // to fetch its authoritative final status, so the stub must distinguish that call from the lookback.
        stubFor(get(urlPathMatching(jobStatusPath))
            .inScenario("done-with-error-status")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DUP_TEST_DONE_WITH_ERROR))
            .willSetStateTo("new-job-completed"));

        stubFor(get(urlPathMatching(jobStatusPath))
            .inScenario("done-with-error-status")
            .whenScenarioStateIs("new-job-completed")
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DUP_TEST_DONE)));

        Query task = buildQueryWithoutFetch(wmRuntimeInfo, 3);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Query.Output output = task.run(runContext);

        assertEquals("job_dup_test", output.getJobId());
        verify(2, postRequestedFor(urlPathMatching(jobsPath)));
    }

    @Test
    void shouldCreateNewJobWhenPreviousJobNotFound(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        String jobsPath = "/bigquery/v2/projects/.*/jobs";
        String pollingPath = "/bigquery/v2/projects/.*/queries/job_dup_test.*";
        String jobStatusPath = "/bigquery/v2/projects/.*/jobs/job_dup_test.*";

        stubFor(post(urlPathMatching(jobsPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_RUNNING)));

        // The original job's poll fails transiently; the fresh job submitted after the lookback
        // then polls successfully.
        stubFor(get(urlPathMatching(pollingPath))
            .inScenario("not-found")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody(BACKEND_ERROR_RESPONSE))
            .willSetStateTo("completed"));

        stubFor(get(urlPathMatching(pollingPath))
            .inScenario("not-found")
            .whenScenarioStateIs("completed")
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(QUERY_RESULTS_RESPONSE_COMPLETE)));

        // The job can no longer be found: no dedup, a new job is submitted.
        // Job#waitFor() itself calls reload() (another GET on this same path) once the fresh job completes,
        // to fetch its authoritative final status, so the stub must distinguish that call from the lookback.
        stubFor(get(urlPathMatching(jobStatusPath))
            .inScenario("not-found-status")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse()
                .withStatus(404)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_NOT_FOUND_RESPONSE))
            .willSetStateTo("new-job-completed"));

        stubFor(get(urlPathMatching(jobStatusPath))
            .inScenario("not-found-status")
            .whenScenarioStateIs("new-job-completed")
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DUP_TEST_DONE)));

        Query task = buildQueryWithoutFetch(wmRuntimeInfo, 3);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Query.Output output = task.run(runContext);

        assertEquals("job_dup_test", output.getJobId());
        verify(2, postRequestedFor(urlPathMatching(jobsPath)));
    }

    @Test
    void shouldSkipLookbackOnDryRunRetry(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        String jobsPath = "/bigquery/v2/projects/.*/jobs";
        String pollingPath = "/bigquery/v2/projects/.*/queries/job_dup_test.*";
        String jobStatusPath = "/bigquery/v2/projects/.*/jobs/job_dup_test.*";

        // A dry-run job is never polled, so its first submission fails with a job-level error that
        // is only visible once the job comes back as DONE; the retry submits a fresh dry-run job.
        stubFor(post(urlPathMatching(jobsPath))
            .inScenario("dry-run-retry")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DUP_TEST_DONE_WITH_ERROR))
            .willSetStateTo("completed"));

        stubFor(post(urlPathMatching(jobsPath))
            .inScenario("dry-run-retry")
            .whenScenarioStateIs("completed")
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DUP_TEST_DONE)));

        Query task = buildDryRunQuery(wmRuntimeInfo, 3);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Query.Output output = task.run(runContext);

        assertEquals("job_dup_test", output.getJobId());
        verify(2, postRequestedFor(urlPathMatching(jobsPath)));
        // The dryRun guard must skip the lookback entirely: no lookup of the previous job's status,
        // and no polling, since Job#isDone()/#waitFor() aren't reliable for dry-run jobs.
        verify(0, getRequestedFor(urlPathMatching(jobStatusPath)));
        verify(0, getRequestedFor(urlPathMatching(pollingPath)));
    }

    private Query buildQueryWithoutFetch(WireMockRuntimeInfo wmRuntimeInfo, int maxAttempts) throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .retryAuto(Exponential.builder()
                .type("exponential")
                .interval(Duration.ofMillis(100))
                .maxInterval(Duration.ofMillis(500))
                .maxDuration(Duration.ofSeconds(10))
                .maxAttempts(maxAttempts)
                .build())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue("SELECT * from test_table"))
            .dryRun(Property.ofValue(false))
            .build();

        Query spy = Mockito.spy(task);
        Mockito.doReturn(mockBigQueryClient(wmRuntimeInfo.getHttpBaseUrl()))
            .when(spy).connection(Mockito.any(RunContext.class));
        return spy;
    }

    private Query buildDryRunQuery(WireMockRuntimeInfo wmRuntimeInfo, int maxAttempts) throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .retryAuto(Exponential.builder()
                .type("exponential")
                .interval(Duration.ofMillis(100))
                .maxInterval(Duration.ofMillis(500))
                .maxDuration(Duration.ofSeconds(10))
                .maxAttempts(maxAttempts)
                .build())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue("SELECT * from test_table"))
            .dryRun(Property.ofValue(true))
            .build();

        Query spy = Mockito.spy(task);
        Mockito.doReturn(mockBigQueryClient(wmRuntimeInfo.getHttpBaseUrl()))
            .when(spy).connection(Mockito.any(RunContext.class));
        return spy;
    }

    private Query buildQuery(WireMockRuntimeInfo wmRuntimeInfo, int maxAttempts) throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .retryAuto(Exponential.builder()
                .type("exponential")
                .interval(Duration.ofMillis(100))
                .maxInterval(Duration.ofMillis(500))
                .maxDuration(Duration.ofSeconds(10))
                .maxAttempts(maxAttempts)
                .build())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue("SELECT * from test_table"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .dryRun(Property.ofValue(false))
            .build();

        Query spy = Mockito.spy(task);
        Mockito.doReturn(mockBigQueryClient(wmRuntimeInfo.getHttpBaseUrl()))
            .when(spy).connection(Mockito.any(RunContext.class));
        return spy;
    }

    private BigQuery mockBigQueryClient(String bigQueryApiHost) {
        return BigQueryOptions.newBuilder()
            .setHost(bigQueryApiHost)
            .setCredentials(NoCredentials.getInstance())
            .setProjectId(this.project)
            .setLocation("europe-west3")
            .setHeaderProvider(() -> Map.of("user-agent", "kestra-unit-test"))
            .build()
            .getService();
    }
}