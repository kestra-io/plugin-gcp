package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class CreateSnapshotConfigTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void defaultDestinationDataset() throws Exception {
        var task = CreateSnapshot.builder()
            .id("snapshot-default_" + IdUtils.create())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(project))
            .sourceDataset(Property.ofValue(dataset))
            .sourceTable(Property.ofValue("source_orders"))
            .snapshotTable(Property.ofValue("orders_snapshot"))
            .build();

        var runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());
        var config = task.jobConfiguration(runContext);

        assertThat(config.getOperationType(), is("SNAPSHOT"));
        assertThat(config.getSourceTables().size(), is(1));
        assertThat(config.getSourceTables().getFirst().getProject(), is(project));
        assertThat(config.getSourceTables().getFirst().getDataset(), is(dataset));
        assertThat(config.getSourceTables().getFirst().getTable(), is("source_orders"));

        assertThat(config.getDestinationTable().getProject(), is(project));
        assertThat(config.getDestinationTable().getDataset(), is(dataset));
        assertThat(config.getDestinationTable().getTable(), is("orders_snapshot"));
        assertThat(config.getDestinationExpirationTime(), nullValue());
    }

    @Test
    void customDestinationDatasetAndProjects() throws Exception {
        var task = CreateSnapshot.builder()
            .id("snapshot-custom-dest_" + IdUtils.create())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(project))
            .sourceProject(Property.ofValue("source-gcp-proj"))
            .sourceDataset(Property.ofValue("src_dataset"))
            .sourceTable(Property.ofValue("src_table"))
            .destinationProject(Property.ofValue("dest-gcp-proj"))
            .destinationDataset(Property.ofValue("dest_dataset"))
            .snapshotTable(Property.ofValue("dest_snap_table"))
            .expirationDuration(Property.ofValue(Duration.ofDays(7)))
            .jobTimeout(Property.ofValue(Duration.ofMinutes(10)))
            .build();

        var runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());
        var config = task.jobConfiguration(runContext);

        assertThat(config.getOperationType(), is("SNAPSHOT"));
        assertThat(config.getSourceTables().getFirst().getProject(), is("source-gcp-proj"));
        assertThat(config.getSourceTables().getFirst().getDataset(), is("src_dataset"));
        assertThat(config.getSourceTables().getFirst().getTable(), is("src_table"));

        assertThat(config.getDestinationTable().getProject(), is("dest-gcp-proj"));
        assertThat(config.getDestinationTable().getDataset(), is("dest_dataset"));
        assertThat(config.getDestinationTable().getTable(), is("dest_snap_table"));
        assertThat(config.getDestinationExpirationTime(), notNullValue());
        assertThat(config.getJobTimeoutMs(), is(Duration.ofMinutes(10).toMillis()));
    }

    @Test
    void timeTravelSnapshotTimestamp() throws Exception {
        var pastTimestamp = Instant.parse("2026-11-10T15:30:00Z");

        var task = CreateSnapshot.builder()
            .id("snapshot-time-travel_" + IdUtils.create())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(project))
            .sourceDataset(Property.ofValue(dataset))
            .sourceTable(Property.ofValue("ledger"))
            .snapshotTable(Property.ofValue("ledger_eod"))
            .snapshotTimestamp(Property.ofValue(pastTimestamp))
            .build();

        var runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());
        var config = task.jobConfiguration(runContext);

        var expectedSourceTable = "ledger@" + pastTimestamp.toEpochMilli();
        assertThat(config.getSourceTables().getFirst().getTable(), is(expectedSourceTable));
    }

    @Test
    void labelsAndExecutionMetadata() throws Exception {
        var taskId = ("snapshot-labels_" + IdUtils.create()).toLowerCase();
        var userLabels = new HashMap<String, String>();
        userLabels.put("environment", "production");
        userLabels.put("backup_policy", "daily");

        var task = CreateSnapshot.builder()
            .id(taskId)
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(project))
            .sourceDataset(Property.ofValue(dataset))
            .sourceTable(Property.ofValue("users"))
            .snapshotTable(Property.ofValue("users_snap"))
            .labels(Property.ofValue(userLabels))
            .build();

        var runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());
        var labels = task.jobConfiguration(runContext).getLabels();

        assertThat(labels.get("environment"), is("production"));
        assertThat(labels.get("backup_policy"), is("daily"));
        assertThat(labels.get("kestra_namespace"), is("io_kestra_plugin_gcp_bigquery_createsnapshotconfigtest"));
        assertThat(labels.get("kestra_flow_id"), notNullValue());
        assertThat(labels.get("kestra_execution_id"), notNullValue());
        assertThat(labels.get("kestra_task_id"), is(taskId));
    }
}
