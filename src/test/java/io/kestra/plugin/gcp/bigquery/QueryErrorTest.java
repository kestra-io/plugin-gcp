package io.kestra.plugin.gcp.bigquery;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
@Slf4j
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
          "id": "my-project:US.job_1234567890abcdef",
          "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/my-project/jobs/job_1234567890abcdef",
          "user_email": "user@example.com",
          "configuration": {
            "query": {
              "query": "SELECT * FROM `my-project.my_dataset.my_table` LIMIT 100",
              "destinationTable": {
                "projectId": "my-project",
                "datasetId": "_temp_dataset",
                "tableId": "anon1234567890abcdef"
              },
              "createDisposition": "CREATE_IF_NEEDED",
              "writeDisposition": "WRITE_TRUNCATE",
              "priority": "INTERACTIVE",
              "useLegacySql": false,
              "useQueryCache": true
            },
            "jobType": "QUERY"
          },
          "jobReference": {
            "projectId": "my-project",
            "jobId": "job_1234567890abcdef",
            "location": "US"
          },
          "status": {
            "state": "DONE"
          },
          "statistics": {
            "creationTime": "1697123456789",
            "startTime": "1697123457000",
            "endTime": "1697123459000",
            "query": {
                "statementType": "SELECT"
            }
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
        // Return a DONE job from creation
        stubFor(post(urlPathMatching("/bigquery/v2/projects/.*/jobs"))
            .withQueryParam("prettyPrint", equalTo("false"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(JOB_RESPONSE_DONE)));

        // Match the query results endpoint loosely because the SDK can vary project and location handling
        // across versions while still exercising the same retry path.
        String queriesPath = "/bigquery/v2/projects/.*/queries/job_1234567890abcdef";
        stubFor(get(urlPathMatching(queriesPath))
            .withQueryParam("prettyPrint", equalTo("false"))
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody(BACKEND_ERROR_RESPONSE)));

        Query task = buildQuery(wmRuntimeInfo, 3);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        assertThrows(Exception.class, () -> task.run(runContext));

        // Verify that retries actually happened (3 attempts = initial + 2 retries)
        verify(3, getRequestedFor(urlPathMatching(queriesPath)));
    }

    private Query buildQuery(WireMockRuntimeInfo wmRuntimeInfo, int maxAttempts) {
        return io.kestra.plugin.gcp.bigquery.Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .bigQueryFactory((runContext, credentials, projectId, location) -> mockBigQueryClient(wmRuntimeInfo.getHttpBaseUrl()))
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
            .dryRun(Property.ofValue(true))
            .build();
    }

    private BigQuery mockBigQueryClient(final String bigQueryApiHost) {
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
