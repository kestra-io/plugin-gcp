package io.kestra.plugin.gcp.bigtable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class ReadWriteRowsTest extends BigtableTestUtils {

    @Inject
    private RunContextFactory runContextFactory;

    private static final String TABLE_ID = "events";
    private static final String COLUMN_FAMILY = "cf1";

    @BeforeEach
    void setUp() throws Exception {
        createTestTable(TABLE_ID, COLUMN_FAMILY);
    }

    @Test
    void writeThenReadRow() throws Exception {
        try (BigtableDataClient client = createDataClient()) {
            client.mutateRow(
                RowMutation.create(TABLE_ID, "row-001")
                    .setCell(COLUMN_FAMILY, "value", "42")
            );
        }

        RunContext runContext = runContextFactory.of();

        ReadRows task = ReadRows.builder()
            .id("read")
            .type(ReadRows.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(TABLE_ID))
            .rowKeyPrefix(Property.ofValue("row-001"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        ReadRows.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is(1L));
        assertThat(output.getRow(), is(notNullValue()));
    }

    @Test
    void writeRowsTask() throws Exception {
        RunContext runContext = runContextFactory.of();

        WriteRows task = WriteRows.builder()
            .id("write")
            .type(WriteRows.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(TABLE_ID))
            .columnFamily(Property.ofValue(COLUMN_FAMILY))
            .rows(
                Property.ofValue(
                    List.of(
                        WriteRows.RowInput.builder()
                            .rowKey("row-100")
                            .cells(Map.of("value", "100"))
                            .build()
                    )
                )
            )
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        WriteRows.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is(1L));
    }

    @Test
    void createTableTask() throws Exception {
        RunContext runContext = runContextFactory.of();
        String newTableId = "new-table";

        CreateTable task = CreateTable.builder()
            .id("create-table")
            .type(CreateTable.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(newTableId))
            .columnFamilies(Property.ofValue(List.of("cf-new")))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        CreateTable.Output output = task.run(runContext);
        assertThat(output.getTableId(), is(newTableId));
        assertThat(output.getColumnFamilyCount(), is(1L));
    }

    @Test
    void deleteRowsTask() throws Exception {
        RunContext runContext = runContextFactory.of();
        try (BigtableDataClient client = createDataClient()) {
            client.mutateRow(
                RowMutation.create(TABLE_ID, "delete-001")
                    .setCell(COLUMN_FAMILY, "value", "to-delete")
            );
        }

        DeleteRows task = DeleteRows.builder()
            .id("delete-rows")
            .type(DeleteRows.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(TABLE_ID))
            .rowKeys(Property.ofValue(List.of("delete-001")))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        DeleteRows.Output output = task.run(runContext);
        assertThat(output.getRowCount(), is(1L));
    }

    @Test
    void deleteTableTask() throws Exception {
        RunContext runContext = runContextFactory.of();
        String tableToDelete = "delete-me";
        createTestTable(tableToDelete, COLUMN_FAMILY);

        DeleteTable task = DeleteTable.builder()
            .id("delete-table")
            .type(DeleteTable.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(tableToDelete))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        DeleteTable.Output output = task.run(runContext);
        assertThat(output.getTableId(), is(tableToDelete));
    }

    @Test
    void triggerTask() throws Exception {
        try (BigtableDataClient client = createDataClient()) {
            client.mutateRow(
                RowMutation.create(TABLE_ID, "trigger-001")
                    .setCell(COLUMN_FAMILY, "value", "trigger-val")
            );
        }

        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(TABLE_ID))
            .rowKeyPrefix(Property.ofValue("trigger-001"))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        java.util.Map.Entry<io.kestra.core.models.conditions.ConditionContext, io.kestra.core.models.triggers.Trigger> triggerContext = io.kestra.core.utils.TestsUtils
            .mockTrigger(runContextFactory, trigger);

        Optional<io.kestra.core.models.executions.Execution> execution = trigger.evaluate(triggerContext.getKey(), triggerContext.getValue());
        assertThat(execution.isPresent(), is(true));

        assertThat(execution.get().getTrigger().getVariables().get("rowCount"), is(1L));
    }
}
