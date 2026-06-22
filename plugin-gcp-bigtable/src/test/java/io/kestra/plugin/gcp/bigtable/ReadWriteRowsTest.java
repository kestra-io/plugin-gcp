package io.kestra.plugin.gcp.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
        assertThat(output.getRow(), is(notNullValueMatcher()));
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
            .rows(Property.ofValue(List.of(
                WriteRows.RowInput.builder()
                    .rowKey("row-100")
                    .cells(Map.of("value", "100"))
                    .build()
            )))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        WriteRows.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is(1L));
    }

    private static org.hamcrest.Matcher<Object> notNullValueMatcher() {
        return org.hamcrest.Matchers.notNullValue();
    }
}
