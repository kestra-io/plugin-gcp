package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.Download;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.UUID;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
public class ExtractToGcsTest extends AbstractBigquery {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Value("${kestra.tasks.bigquery.table}")
    private String table;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Value("${kestra.tasks.gcs.filename}")
    private String filename;

    @Value("true")
    private Boolean printHeader;

    private Job query(BigQuery bigQuery, String query) throws InterruptedException {
        return bigQuery
            .create(JobInfo
                .newBuilder(QueryJobConfiguration.newBuilder(query).build())
                .setJobId(JobId.of(UUID.randomUUID().toString()))
                .build()
            )
            .waitFor();
    }

    @Test
    void toCsv() throws Exception {
        // Sample table

        // Extract task
        ExtractToGcs task = ExtractToGcs.builder()
            .id(ExtractToGcsTest.class.getSimpleName())
            .type(ExtractToGcs.class.getName())
            .destinationUris(Collections.singletonList(
                "gs://" + this.bucket + "/" + this.filename
            ))
            .sourceTable(this.project + "." + this.dataset + "." + this.table)
            .printHeader(printHeader)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        query(
            task.connection(runContext),
            "CREATE OR REPLACE TABLE  `" + this.dataset + "." +  this.table + "`" +
            "(product STRING, quantity INT64)" +
            ";" +
            "INSERT `" + this.dataset + "." +  this.table + "` (product, quantity)" +
            "VALUES('top load washer', 10)" +
            ";"
        );

        ExtractToGcs.Output extractOutput = task.run(runContext);

        // Download task
        String testString = "product,quantity\n" +
            "top load washer,10\n";

        Download downloadTask = Download.builder()
            .id(ExtractToGcsTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(extractOutput.getDestinationUris().get(0))
            .build();

        Download.Output downloadOutput = downloadTask.run(runContext(downloadTask));
        InputStream get = storageInterface.get(downloadOutput.getUri());

        // Tests
        assertThat(extractOutput.getFileCounts().get(0), is(1L));
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(testString)
        );

        // Clean sample table
        query(task.connection(runContext), "DROP TABLE  `" + this.dataset + "." +  this.table + "` ;");

    }

    private RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of(
                "bucket", this.bucket
            )
        );
    }
}

