package io.kestra.plugin.gcp.bigquery;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Slf4j
@WireMockTest
public class QueryErrorTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void shouldRetryOnBackendError(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(post("/bigquery/v2/projects/kestra-unit-test/jobs?prettyPrint=false").willReturn(aResponse().withStatus(200).withBody("""
            {
              "comment": "Query Job Response (RUNNING state)",
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
                "state": "RUNNING"
              },
              "statistics": {
                "creationTime": "1697123456789",
                "startTime": "1697123457000"
              }
            }""")));

        stubFor(get("/bigquery/v2/projects/my-project/jobs/job_1234567890abcdef?location=US&prettyPrint=false").willReturn(aResponse().withStatus(200).withBody("""
            {
              "comment": "Query Job Response (RUNNING state)",
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
            }""")));

        stubFor(get("/bigquery/v2/projects/my-project/queries/job_1234567890abcdef?location=US&maxResults=0&prettyPrint=false").willReturn(aResponse().withStatus(200).withBody("""
            {
                           "comment": "Example 1: Simple SELECT query with results (completed)",
                           "kind": "bigquery#getQueryResultsResponse",
                           "etag": "\\"abc123def456\\"",
                           "jobReference": {
                             "projectId": "my-project",
                             "jobId": "job_1234567890abcdef",
                             "location": "US"
                           },
                           "schema": {
                             "fields": [
                               {
                                 "name": "name",
                                 "type": "STRING",
                                 "mode": "NULLABLE"
                               },
                               {
                                 "name": "age",
                                 "type": "INTEGER",
                                 "mode": "NULLABLE"
                               },
                               {
                                 "name": "email",
                                 "type": "STRING",
                                 "mode": "NULLABLE"
                               }
                             ]
                           },
                           "jobComplete": true,
                           "totalRows": "3",
                           "rows": [
                             {
                               "f": [
                                 { "v": "John Doe" },
                                 { "v": "30" },
                                 { "v": "john.doe@example.com" }
                               ]
                             },
                             {
                               "f": [
                                 { "v": "Jane Smith" },
                                 { "v": "25" },
                                 { "v": "jane.smith@example.com" }
                               ]
                             },
                             {
                               "f": [
                                 { "v": "Bob Johnson" },
                                 { "v": "35" },
                                 { "v": "bob.johnson@example.com" }
                               ]
                             }
                           ],
                           "totalBytesProcessed": "1048576",
                           "cacheHit": false
                         }""")));

        stubFor(get("/bigquery/v2/projects/my-project/datasets/_temp_dataset/tables/anon1234567890abcdef/data?prettyPrint=false").willReturn(aResponse().withStatus(503).withBody("""
            {
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
            """)));

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .bigQueryFactory((runContext, credentials, projectId, location) -> mockBigQueryClient(wmRuntimeInfo.getHttpBaseUrl()))
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue("SELECT * from test_table"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        assertThrows(BigQueryException.class, () -> {
            task.run(runContext);
        });

        verify(1, getRequestedFor(urlEqualTo("/bigquery/v2/projects/my-project/datasets/_temp_dataset/tables/anon1234567890abcdef/data?prettyPrint=false")));
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