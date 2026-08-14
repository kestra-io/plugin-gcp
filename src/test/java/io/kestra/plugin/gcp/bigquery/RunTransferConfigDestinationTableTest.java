package io.kestra.plugin.gcp.bigquery;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.datatransfer.v1.TransferConfig;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class RunTransferConfigDestinationTableTest {

    @Test
    void returnsLiteralDestinationTableName() {
        var config = configWithParam("destination_table_name_template", "dts_smoke");

        assertThat(RunTransferConfig.destinationTable(config), is("dts_smoke"));
    }

    @Test
    void returnsNullWhenTemplated() {
        var config = configWithParam("destination_table_name_template", "dts_smoke_{run_time}");

        assertThat(RunTransferConfig.destinationTable(config), nullValue());
    }

    @Test
    void returnsNullWhenKeyMissing() {
        var config = TransferConfig.newBuilder()
            .setParams(Struct.newBuilder().build())
            .build();

        assertThat(RunTransferConfig.destinationTable(config), nullValue());
    }

    @Test
    void returnsNullWhenBlank() {
        var config = configWithParam("destination_table_name_template", "  ");

        assertThat(RunTransferConfig.destinationTable(config), nullValue());
    }

    private static TransferConfig configWithParam(String key, String value) {
        var struct = Struct.newBuilder()
            .putFields(key, Value.newBuilder().setStringValue(value).build())
            .build();

        return TransferConfig.newBuilder()
            .setParams(struct)
            .build();
    }
}
