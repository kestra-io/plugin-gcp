package io.kestra.plugin.gcp.bigtable;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ReadRowsEncodingTest extends BigtableTestUtils {

    @Inject
    private RunContextFactory runContextFactory;

    private static final String TABLE_ID = "encoding-events";
    private static final String COLUMN_FAMILY = "cf1";

    // bytes that are not valid UTF-8
    private static final byte[] BINARY_SUFFIX = { (byte) 0xFF, 0x01 };
    private static final byte[] BINARY_QUALIFIER = { (byte) 0xFE, 'q' };
    private static final byte[] BINARY_VALUE = { (byte) 0xFF, (byte) 0xFE, 0, 0, 0, 0, 0, 7 };

    @BeforeEach
    void setUp() throws Exception {
        createTestTable(TABLE_ID, COLUMN_FAMILY);
    }

    @Test
    @SuppressWarnings("unchecked")
    void binaryKeyQualifierAndValueAreReturnedAsBase64() throws Exception {
        var key = writeBinaryRow("bin-");

        var task = ReadRows.builder()
            .id(IdUtils.create())
            .type(ReadRows.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(TABLE_ID))
            .rowKeyPrefix(Property.ofValue("bin-"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var row = task.run(runContextFactory.of()).getRow();
        var cell = ((List<Map<String, Object>>) row.get("cells")).getFirst();

        assertThat(row.get("rowKey"), is(base64(key)));
        assertThat(row.get("rowKeyEncoding"), is("base64"));
        assertThat(cell.get("qualifier"), is(base64(BINARY_QUALIFIER)));
        assertThat(cell.get("qualifierEncoding"), is("base64"));
        assertThat(cell.get("value"), is(base64(BINARY_VALUE)));
        assertThat(cell.get("valueEncoding"), is("base64"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void textKeyQualifierAndValueAreNotChanged() throws Exception {
        try (var client = createDataClient()) {
            client.mutateRow(
                RowMutation.create(TABLE_ID, "text-001")
                    .setCell(COLUMN_FAMILY, "name", timestampMicros(), "héllo")
            );
        }

        var task = ReadRows.builder()
            .id(IdUtils.create())
            .type(ReadRows.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(TABLE_ID))
            .rowKeyPrefix(Property.ofValue("text-001"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var row = task.run(runContextFactory.of()).getRow();
        var cell = ((List<Map<String, Object>>) row.get("cells")).getFirst();

        assertThat(row.get("rowKey"), is("text-001"));
        assertThat(cell.get("qualifier"), is("name"));
        assertThat(cell.get("value"), is("héllo"));
        assertThat(row.containsKey("rowKeyEncoding"), is(false));
        assertThat(cell.containsKey("qualifierEncoding"), is(false));
        assertThat(cell.containsKey("valueEncoding"), is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    void triggerReturnsBinaryValuesAsBase64() throws Exception {
        var key = writeBinaryRow("trigbin-");

        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .tableId(Property.ofValue(TABLE_ID))
            .rowKeyPrefix(Property.ofValue("trigbin-"))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(triggerContext.getKey(), triggerContext.getValue());
        assertThat(execution.isPresent(), is(true));

        var rows = (List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("rows");
        var cell = ((List<Map<String, Object>>) rows.getFirst().get("cells")).getFirst();

        assertThat(rows.getFirst().get("rowKey"), is(base64(key)));
        assertThat(rows.getFirst().get("rowKeyEncoding"), is("base64"));
        assertThat(cell.get("value"), is(base64(BINARY_VALUE)));
        assertThat(cell.get("valueEncoding"), is("base64"));
    }

    private byte[] writeBinaryRow(String prefix) throws Exception {
        var key = ByteString.copyFromUtf8(prefix).concat(ByteString.copyFrom(BINARY_SUFFIX));
        try (var client = createDataClient()) {
            client.mutateRow(
                RowMutation.create(TABLE_ID, key)
                    .setCell(COLUMN_FAMILY, ByteString.copyFrom(BINARY_QUALIFIER), timestampMicros(), ByteString.copyFrom(BINARY_VALUE))
            );
        }
        return key.toByteArray();
    }

    private static String base64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }
}
