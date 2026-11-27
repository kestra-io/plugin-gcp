package io.kestra.plugin.gcp.spanner;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class SpannerServiceArrayTest {
    @Test
    void shouldKeepNullInTimestampArray() {
        var row = Struct.newBuilder()
            .set("c").toTimestampArray(Arrays.asList(Timestamp.ofTimeSecondsAndNanos(0, 0), null))
            .build();

        var values = (List<?>) SpannerService.rowToMap(row).get("c");

        assertThat(values, is(Arrays.asList(Instant.EPOCH, null)));
    }

    @Test
    void shouldKeepNullInDateArray() {
        var row = Struct.newBuilder()
            .set("c").toDateArray(Arrays.asList(Date.fromYearMonthDay(2024, 1, 2), null))
            .build();

        var values = (List<?>) SpannerService.rowToMap(row).get("c");

        assertThat(values, is(Arrays.asList(LocalDate.of(2024, 1, 2), null)));
    }

    @Test
    void shouldKeepNullInStructArray() {
        var element = Struct.newBuilder().set("id").to(1L).build();
        var row = Struct.newBuilder()
            .set("c").toStructArray(Type.struct(Type.StructField.of("id", Type.int64())), Arrays.asList(element, null))
            .build();

        var values = (List<?>) SpannerService.rowToMap(row).get("c");

        assertThat(values.size(), is(2));
        assertThat(values.get(0), is(Map.of("id", 1L)));
        assertThat(values.get(1), nullValue());
    }

    @Test
    void shouldDeserializeBytesArray() {
        var row = Struct.newBuilder()
            .set("c").to(Value.bytesArray(Arrays.asList(
                ByteArray.copyFrom("hello".getBytes()),
                null
            )))
            .build();

        @SuppressWarnings("unchecked")
        var values = (List<byte[]>) SpannerService.rowToMap(row).get("c");

        assertThat(values.size(), is(2));
        assertThat(new String(values.getFirst()), is("hello"));
        assertThat(values.get(1), nullValue());
    }

    @Test
    void shouldDeserializeJsonArray() {
        var row = Struct.newBuilder()
            .set("c").to(Value.jsonArray(Arrays.asList("{\"id\":1}", "[1,2,3]", "true", null)))
            .build();

        var values = (List<?>) SpannerService.rowToMap(row).get("c");

        assertThat(values.size(), is(4));
        assertThat(values.getFirst(), is(Map.of("id", 1)));
        assertThat(values.get(1), is(List.of(1, 2, 3)));
        assertThat(values.get(2), is(true));
        assertThat(values.get(3), nullValue());
    }

    @Test
    void shouldDeserializeEmptyArrays() {
        var row = Struct.newBuilder()
            .set("bytes_arr").to(Value.bytesArray(List.of()))
            .set("json_arr").to(Value.jsonArray(List.of()))
            .build();

        var map = SpannerService.rowToMap(row);

        assertThat(map.get("bytes_arr"), is(List.of()));
        assertThat(map.get("json_arr"), is(List.of()));
    }
}
