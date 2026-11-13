package io.kestra.plugin.gcp.bigquery;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.Mockito;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.*;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class CreateSnapshotTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void runSuccess() throws Exception {
        var task = CreateSnapshot.builder()
            .id("create-snapshot-run_" + IdUtils.create())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(project))
            .sourceDataset(Property.ofValue(dataset))
            .sourceTable(Property.ofValue("source_table"))
            .destinationDataset(Property.ofValue("dest_dataset"))
            .snapshotTable(Property.ofValue("snapshot_table"))
            .description("Snapshot for audit")
            .expirationDuration(Property.ofValue(Duration.ofDays(7)))
            .build();

        var spy = Mockito.spy(task);

        var connection = mock(BigQuery.class);
        doReturn(connection).when(spy).connection(any());

        var job = mock(Job.class);
        var jobId = JobId.newBuilder().setProject(project).setJob("kestra_snapshot_job_1").setLocation("US").build();
        when(job.getJobId()).thenReturn(jobId);

        var status = mock(JobStatus.class);
        when(status.getState()).thenReturn(JobStatus.State.DONE);
        when(status.getError()).thenReturn(null);
        when(job.getStatus()).thenReturn(status);

        var copyStats = mock(JobStatistics.CopyStatistics.class);
        when(copyStats.getStartTime()).thenReturn(1000L);
        when(copyStats.getEndTime()).thenReturn(2000L);
        when(copyStats.getNumChildJobs()).thenReturn(1L);
        when(job.getStatistics()).thenReturn(copyStats);

        when(connection.create(any(JobInfo.class))).thenReturn(job);
        when(connection.getJob(jobId)).thenReturn(job);

        var table = mock(Table.class);
        var tableId = TableId.of(project, "dest_dataset", "snapshot_table");
        when(table.getTableId()).thenReturn(tableId);
        when(table.getCreationTime()).thenReturn(1700000000000L);
        when(table.getExpirationTime()).thenReturn(1700604800000L);
        when(table.getNumRows()).thenReturn(BigInteger.valueOf(1500L));
        when(table.getNumBytes()).thenReturn(40960L);

        var snapshotDef = mock(SnapshotTableDefinition.class);
        when(snapshotDef.getSnapshotTime()).thenReturn("2026-11-13T10:00:00Z");
        when(table.getDefinition()).thenReturn(snapshotDef);

        var tableBuilder = mock(Table.Builder.class);
        when(table.toBuilder()).thenReturn(tableBuilder);
        when(tableBuilder.setDescription(any())).thenReturn(tableBuilder);
        when(tableBuilder.setExpirationTime(anyLong())).thenReturn(tableBuilder);
        when(tableBuilder.build()).thenReturn(table);
        when(connection.getTable(tableId)).thenReturn(table);
        when(connection.update(any(TableInfo.class))).thenReturn(table);

        var runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, spy, ImmutableMap.of());
        var output = spy.run(runContext);

        assertThat(output.getJobId(), is("kestra_snapshot_job_1"));
        assertThat(output.getProjectId(), is(project));
        assertThat(output.getDatasetId(), is("dest_dataset"));
        assertThat(output.getTableId(), is("snapshot_table"));
        assertThat(output.getSnapshotTime(), is(Instant.parse("2026-11-13T10:00:00Z")));
        assertThat(output.getExpirationTime(), is(Instant.ofEpochMilli(1700604800000L)));
        assertThat(output.getTotalRows(), is(1500L));
        assertThat(output.getTotalBytes(), is(40960L));

        verify(connection, times(1)).getTable(tableId);
        verify(tableBuilder, times(1)).setDescription("Snapshot for audit");
        verify(connection, times(1)).update(any(TableInfo.class));
    }

    @Test
    void runDryRun() throws Exception {
        var task = CreateSnapshot.builder()
            .id("create-snapshot-dryrun_" + IdUtils.create())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(project))
            .sourceDataset(Property.ofValue(dataset))
            .sourceTable(Property.ofValue("source_table"))
            .snapshotTable(Property.ofValue("snapshot_dryrun_table"))
            .dryRun(Property.ofValue(true))
            .build();

        var spy = Mockito.spy(task);

        var connection = mock(BigQuery.class);
        doReturn(connection).when(spy).connection(any());

        var job = mock(Job.class);
        var jobId = JobId.of(project, "kestra_snapshot_dryrun_job");
        when(job.getJobId()).thenReturn(jobId);

        var status = mock(JobStatus.class);
        when(status.getState()).thenReturn(JobStatus.State.DONE);
        when(status.getError()).thenReturn(null);
        when(job.getStatus()).thenReturn(status);

        when(connection.create(any(JobInfo.class))).thenReturn(job);
        when(connection.getJob(jobId)).thenReturn(job);

        var runContext = TestsUtils.mockRunContext(MAIN_TENANT, runContextFactory, spy, ImmutableMap.of());
        var output = spy.run(runContext);

        assertThat(output.getJobId(), is("kestra_snapshot_dryrun_job"));
        assertThat(output.getProjectId(), is(project));
        assertThat(output.getDatasetId(), is(dataset));
        assertThat(output.getTableId(), is("snapshot_dryrun_table"));
        assertThat(output.getSnapshotTime(), nullValue());
        assertThat(output.getExpirationTime(), nullValue());

        verify(connection, never()).getTable(any(TableId.class));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    void runLive() throws Exception {
        var baseTableName = "tbl_" + FriendlyId.createFriendlyId().toLowerCase().replaceAll("[^a-z0-9_]", "_");
        var snapshotTableName = "snap_" + FriendlyId.createFriendlyId().toLowerCase().replaceAll("[^a-z0-9_]", "_");
        var baseTableFull = project + "." + dataset + "." + baseTableName;

        var create = Query.builder()
            .id(CreateSnapshotTest.class.getSimpleName() + "_create_" + IdUtils.create())
            .type(Query.class.getName())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue(QueryTest.sql()))
            .destinationTable(Property.ofValue(baseTableFull))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());
        var createOutput = create.run(runContext);
        assertThat(createOutput.getJobId(), is(notNullValue()));

        var snapshot = CreateSnapshot.builder()
            .id(CreateSnapshotTest.class.getSimpleName() + "_snapshot_" + IdUtils.create())
            .type(CreateSnapshot.class.getName())
            .projectId(Property.ofValue(project))
            .sourceDataset(Property.ofValue(dataset))
            .sourceTable(Property.ofValue(baseTableName))
            .snapshotTable(Property.ofValue(snapshotTableName))
            .expirationDuration(Property.ofValue(Duration.ofDays(2)))
            .description("Live test snapshot")
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, snapshot, ImmutableMap.of());
        var snapshotOutput = snapshot.run(runContext);

        assertThat(snapshotOutput.getJobId(), is(notNullValue()));
        assertThat(snapshotOutput.getProjectId(), is(project));
        assertThat(snapshotOutput.getDatasetId(), is(dataset));
        assertThat(snapshotOutput.getTableId(), is(snapshotTableName));
        assertThat(snapshotOutput.getSnapshotTime(), is(notNullValue()));
        assertThat(snapshotOutput.getExpirationTime(), is(notNullValue()));
    }
}
