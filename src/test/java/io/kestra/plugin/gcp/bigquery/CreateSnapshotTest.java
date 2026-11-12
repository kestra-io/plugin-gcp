package io.kestra.plugin.gcp.bigquery;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.SnapshotTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class CreateSnapshotTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void shouldBuildJobConfigurationWithAllProperties() throws Exception {
        String sourceTable = "orders_" + FriendlyId.createFriendlyId();
        String destinationDataset = "snapshots_" + FriendlyId.createFriendlyId();
        String snapshotTable = "orders_snap_" + FriendlyId.createFriendlyId();
        Instant historicalTime = Instant.parse("2026-11-10T12:00:00Z");

        Map<String, String> userLabels = new HashMap<>();
        userLabels.put("env", "test");
        userLabels.put("engine", "bigquery");

        CreateSnapshot task = CreateSnapshot.builder()
            .id(CreateSnapshot.class.getSimpleName())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(this.project))
            .sourceDataset(Property.ofValue(this.dataset))
            .sourceTable(Property.ofValue(sourceTable))
            .destinationDataset(Property.ofValue(destinationDataset))
            .snapshotTable(Property.ofValue(snapshotTable))
            .expirationDuration(Property.ofValue(Duration.ofDays(7)))
            .snapshotTimestamp(Property.ofValue(historicalTime))
            .description("Point-in-time snapshot")
            .friendlyName(Property.ofValue("Orders Snapshot"))
            .labels(Property.ofValue(userLabels))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());

        long beforeBuild = System.currentTimeMillis();
        CopyJobConfiguration config = task.jobConfiguration(runContext);
        long afterBuild = System.currentTimeMillis();

        assertThat(config.getOperationType()).isEqualTo("SNAPSHOT");
        assertThat(config.getDestinationTable()).isEqualTo(TableId.of(this.project, destinationDataset, snapshotTable));
        assertThat(config.getSourceTables()).hasSize(1);
        assertThat(config.getSourceTables().get(0)).isEqualTo(TableId.of(this.project, this.dataset, sourceTable + "@" + historicalTime.toEpochMilli()));

        assertThat(config.getDestinationExpirationTime()).isNotNull();
        Instant parsedExpiration = Instant.parse(config.getDestinationExpirationTime());
        long minExpected = beforeBuild + Duration.ofDays(7).toMillis();
        long maxExpected = afterBuild + Duration.ofDays(7).toMillis();
        assertThat(parsedExpiration.toEpochMilli()).isBetween(minExpected, maxExpected);

        Map<String, String> labels = config.getLabels();
        assertThat(labels.get("env")).isEqualTo("test");
        assertThat(labels.get("engine")).isEqualTo("bigquery");
        assertThat(labels.get("kestra_namespace")).isEqualTo("io_kestra_plugin_gcp_bigquery_createsnapshottest");
        assertThat(labels.get("kestra_flow_id")).isEqualTo("shouldbuildjobconfigurationwithallproperties");
        assertThat(labels.get("kestra_task_id")).isEqualTo("createsnapshot");
    }

    @Test
    void shouldFallbackDestinationDatasetToSourceDatasetWhenOmitted() throws Exception {
        String sourceTable = "customers_" + FriendlyId.createFriendlyId();
        String snapshotTable = "customers_snap_" + FriendlyId.createFriendlyId();

        CreateSnapshot task = CreateSnapshot.builder()
            .id(CreateSnapshot.class.getSimpleName())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(this.project))
            .sourceDataset(Property.ofValue(this.dataset))
            .sourceTable(Property.ofValue(sourceTable))
            .snapshotTable(Property.ofValue(snapshotTable))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());
        CopyJobConfiguration config = task.jobConfiguration(runContext);

        assertThat(config.getOperationType()).isEqualTo("SNAPSHOT");
        assertThat(config.getDestinationTable().getDataset()).isEqualTo(this.dataset);
        assertThat(config.getDestinationTable().getTable()).isEqualTo(snapshotTable);
        assertThat(config.getSourceTables().get(0).getTable()).isEqualTo(sourceTable);
        assertThat(config.getDestinationExpirationTime()).isNull();
    }

    @Test
    void shouldPassModelValidationWhenRequiredFieldsPresent() {
        CreateSnapshot task = CreateSnapshot.builder()
            .id("create_snapshot")
            .type(CreateSnapshot.class.getName())
            .sourceDataset(Property.ofValue("src_dataset"))
            .sourceTable(Property.ofValue("src_table"))
            .snapshotTable(Property.ofValue("snap_table"))
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(task);
        assertThat(valid).isEmpty();
    }

    @Test
    void shouldFailModelValidationWhenRequiredFieldsMissing() {
        CreateSnapshot task = CreateSnapshot.builder()
            .id("create_snapshot")
            .type(CreateSnapshot.class.getName())
            .sourceDataset(Property.ofValue("src_dataset"))
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(task);
        assertThat(valid).isPresent();
    }

    @Test
    void shouldSubmitSnapshotCopyJobAndMapOutputWhenRunning() throws Exception {
        CreateSnapshot task = Mockito.spy(
            CreateSnapshot.builder()
                .id("create_snapshot")
                .type(CreateSnapshot.class.getName())
                .projectId(Property.ofValue("my-project"))
                .sourceDataset(Property.ofValue("prod"))
                .sourceTable(Property.ofValue("orders"))
                .destinationDataset(Property.ofValue("backup"))
                .snapshotTable(Property.ofValue("orders_snap"))
                .expirationDuration(Property.ofValue(Duration.ofDays(7)))
                .build()
        );
        RunContext runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());

        JobId jobId = JobId.newBuilder().setProject("my-project").setLocation("US").setJob("job_1").build();
        JobStatus status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getState()).thenReturn(JobStatus.State.DONE);
        JobStatistics.CopyStatistics statistics = Mockito.mock(JobStatistics.CopyStatistics.class);
        Mockito.when(statistics.getStartTime()).thenReturn(1_000L);
        Mockito.when(statistics.getEndTime()).thenReturn(2_000L);
        Job job = Mockito.mock(Job.class);
        Mockito.when(job.getJobId()).thenReturn(jobId);
        Mockito.when(job.getStatus()).thenReturn(status);
        Mockito.when(job.getStatistics()).thenReturn(statistics);

        SnapshotTableDefinition definition = Mockito.mock(SnapshotTableDefinition.class);
        Mockito.when(definition.getSnapshotTime()).thenReturn("2026-11-10T12:00:00.000Z");
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getTableId()).thenReturn(TableId.of("my-project", "backup", "orders_snap"));
        Mockito.when(table.getCreationTime()).thenReturn(1_762_776_000_000L);
        Mockito.when(table.getExpirationTime()).thenReturn(1_763_380_800_000L);
        Mockito.when(table.getNumRows()).thenReturn(BigInteger.valueOf(42));
        Mockito.when(table.getNumTotalLogicalBytes()).thenReturn(4_200L);
        Mockito.doReturn(definition).when(table).getDefinition();

        BigQuery connection = Mockito.mock(BigQuery.class);
        ArgumentCaptor<JobInfo> submitted = ArgumentCaptor.forClass(JobInfo.class);
        Mockito.when(connection.create(submitted.capture())).thenReturn(job);
        Mockito.when(connection.getTable(TableId.of("my-project", "backup", "orders_snap"))).thenReturn(table);
        Mockito.doReturn(connection).when(task).connection(Mockito.any(RunContext.class));

        CreateSnapshot.Output output = task.run(runContext);

        CopyJobConfiguration submittedConfig = submitted.getValue().getConfiguration();
        assertThat(submittedConfig.getOperationType()).isEqualTo("SNAPSHOT");
        assertThat(submittedConfig.getDestinationExpirationTime()).isNotNull();
        assertThat(output.getJobId()).isEqualTo("job_1");
        assertThat(output.getProjectId()).isEqualTo("my-project");
        assertThat(output.getDatasetId()).isEqualTo("backup");
        assertThat(output.getTableId()).isEqualTo("orders_snap");
        assertThat(output.getTotalRows()).isEqualTo(BigInteger.valueOf(42));
        assertThat(output.getTotalBytes()).isEqualTo(4_200L);
        assertThat(output.getSnapshotTime()).isEqualTo(Instant.parse("2026-11-10T12:00:00Z"));
        assertThat(output.getExpirationTime()).isEqualTo(Instant.ofEpochMilli(1_763_380_800_000L));
    }

    @Test
    void shouldNotFetchTableWhenDryRun() throws Exception {
        CreateSnapshot task = Mockito.spy(
            CreateSnapshot.builder()
                .id("create_snapshot")
                .type(CreateSnapshot.class.getName())
                .projectId(Property.ofValue("my-project"))
                .sourceDataset(Property.ofValue("prod"))
                .sourceTable(Property.ofValue("orders"))
                .snapshotTable(Property.ofValue("orders_snap"))
                .dryRun(Property.ofValue(true))
                .build()
        );
        RunContext runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, task, ImmutableMap.of());

        JobId jobId = JobId.newBuilder().setProject("my-project").setLocation("US").setJob("job_dry").build();
        JobStatus status = Mockito.mock(JobStatus.class);
        Mockito.when(status.getState()).thenReturn(JobStatus.State.DONE);
        Job job = Mockito.mock(Job.class);
        Mockito.when(job.getJobId()).thenReturn(jobId);
        Mockito.when(job.getStatus()).thenReturn(status);

        BigQuery connection = Mockito.mock(BigQuery.class);
        Mockito.when(connection.create(Mockito.any(JobInfo.class))).thenReturn(job);
        Mockito.doReturn(connection).when(task).connection(Mockito.any(RunContext.class));

        CreateSnapshot.Output output = task.run(runContext);

        assertThat(output.getJobId()).isEqualTo("job_dry");
        assertThat(output.getTotalRows()).isNull();
        Mockito.verify(connection, Mockito.never()).getTable(Mockito.any(TableId.class));
    }
}
