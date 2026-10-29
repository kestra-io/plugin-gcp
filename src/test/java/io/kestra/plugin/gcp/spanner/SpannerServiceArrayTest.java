package io.kestra.plugin.gcp.spanner;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class SpannerServiceArrayTest {
    @Test
    void shouldKeepNullInTimestampArray() {
        var row = Struct.newBuilder()
            .set("c").toTimestampArray(Arrays.asList(Timestamp.ofTimeSecondsAndNanos(0, 0), null))
            .build();

        List<?> values = (List<?>) SpannerService.rowToMap(row).get("c");

        assertThat(values, is(Arrays.asList(Instant.EPOCH, null)));
    }

    @Test
    void shouldKeepNullInDateArray() {
        var row = Struct.newBuilder()
            .set("c").toDateArray(Arrays.asList(Date.fromYearMonthDay(2024, 1, 2), null))
            .build();

        List<?> values = (List<?>) SpannerService.rowToMap(row).get("c");

        assertThat(values, is(Arrays.asList(LocalDate.of(2024, 1, 2), null)));
    }

    @Test
    void shouldKeepNullInStructArray() {
        var element = Struct.newBuilder().set("id").to(1L).build();
        var row = Struct.newBuilder()
            .set("c").toStructArray(Type.struct(Type.StructField.of("id", Type.int64())), Arrays.asList(element, null))
            .build();

        List<?> values = (List<?>) SpannerService.rowToMap(row).get("c");

        assertThat(values.size(), is(2));
        assertThat(values.get(0), is(Map.of("id", 1L)));
        assertThat(values.get(1), nullValue());
    }
}
