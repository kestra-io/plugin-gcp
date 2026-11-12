package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class CreateSnapshotLiveTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void shouldCreateSnapshotLive() throws Exception {
        String testProject = System.getProperty("kestra.tasks.bigquery.project", System.getenv().getOrDefault("GCP_PROJECT", this.project));
        String testDataset = System.getProperty("kestra.tasks.bigquery.dataset", System.getenv().getOrDefault("GCP_DATASET", this.dataset));

        String baseTableName = "base_tbl_" + FriendlyId.createFriendlyId().replace('-', '_').toLowerCase();
        String snapshotTableName = "snap_tbl_" + FriendlyId.createFriendlyId().replace('-', '_').toLowerCase();
        String baseTableIdStr = testProject + "." + testDataset + "." + baseTableName;

        Query createBaseTable = Query.builder()
            .id("create_base_table")
            .type(Query.class.getName())
            .projectId(Property.ofValue(testProject))
            .sql(Property.ofValue(QueryTest.sql()))
            .destinationTable(Property.ofValue(baseTableIdStr))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, createBaseTable, ImmutableMap.of());
        Query.Output queryOutput = createBaseTable.run(runContext);
        assertThat(queryOutput.getJobId()).isNotNull();

        CreateSnapshot snapshotTask = CreateSnapshot.builder()
            .id("create_snapshot")
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(testProject))
            .sourceDataset(Property.ofValue(testDataset))
            .sourceTable(Property.ofValue(baseTableName))
            .snapshotTable(Property.ofValue(snapshotTableName))
            .expirationDuration(Property.ofValue(Duration.ofHours(48)))
            .description("Live integration test snapshot")
            .friendlyName(Property.ofValue("Live Snapshot"))
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, snapshotTask, ImmutableMap.of());
        CreateSnapshot.Output output = snapshotTask.run(runContext);

        assertThat(output.getJobId()).isNotNull();
        assertThat(output.getTableId()).isEqualTo(snapshotTableName);
        assertThat(output.getDatasetId()).isEqualTo(testDataset);
        assertThat(output.getExpirationTime()).isNotNull();
        assertThat(output.getTotalRows()).isNotNull();

        try {
            BigQuery connection = snapshotTask.connection(runContext);
            connection.delete(TableId.of(testProject, testDataset, snapshotTableName));
            connection.delete(TableId.of(testProject, testDataset, baseTableName));
        } catch (Exception ignored) {
        }
    }
}
