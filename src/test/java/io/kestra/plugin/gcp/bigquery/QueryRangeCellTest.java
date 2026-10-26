package io.kestra.plugin.gcp.bigquery;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldElementType;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Range;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class QueryRangeCellTest {

    private final Query query = Query.builder().build();

    @Test
    void shouldParseDatetimeRangeWithInstant() {
        Field field = Field.newBuilder("validity", LegacySQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(LegacySQLTypeName.DATETIME.toString()).build())
            .build();
        Range range = Range.newBuilder()
            .setStart("2026-01-01 00:00:00")
            .setEnd("2026-12-31 23:59:59")
            .setType(FieldElementType.newBuilder().setType(LegacySQLTypeName.DATETIME.toString()).build())
            .build();
        FieldValue value = FieldValue.of(FieldValue.Attribute.RANGE, range);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) query.convertCell(field, value, false);

        assertThat(result.get("start"), is(Instant.parse("2026-01-01T00:00:00Z")));
        assertThat(result.get("end"), is(Instant.parse("2026-12-31T23:59:59Z")));
    }

    @Test
    void shouldHandleUnboundedDatetimeRange() {
        Field field = Field.newBuilder("validity", LegacySQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(LegacySQLTypeName.DATETIME.toString()).build())
            .build();
        Range range = Range.newBuilder()
            .setEnd("2026-06-01 12:00:00")
            .setType(FieldElementType.newBuilder().setType(LegacySQLTypeName.DATETIME.toString()).build())
            .build();
        FieldValue value = FieldValue.of(FieldValue.Attribute.RANGE, range);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) query.convertCell(field, value, false);

        assertThat(result.get("start"), is(nullValue()));
        assertThat(result.get("end"), is(Instant.parse("2026-06-01T12:00:00Z")));
    }

    @Test
    void shouldHandleUnboundedDateRange() {
        Field field = Field.newBuilder("validity", LegacySQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(LegacySQLTypeName.DATE.toString()).build())
            .build();
        Range range = Range.newBuilder()
            .setStart("2026-01-01")
            .setType(FieldElementType.newBuilder().setType(LegacySQLTypeName.DATE.toString()).build())
            .build();
        FieldValue value = FieldValue.of(FieldValue.Attribute.RANGE, range);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) query.convertCell(field, value, false);

        assertThat(result.get("start"), is(LocalDate.parse("2026-01-01")));
        assertThat(result.get("end"), is(nullValue()));
    }

    @Test
    void shouldHandleUnboundedTimestampRange() {
        Field field = Field.newBuilder("validity", LegacySQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(LegacySQLTypeName.TIMESTAMP.toString()).build())
            .build();
        Range range = Range.newBuilder()
            .setEnd("1767225600.000000")
            .setType(FieldElementType.newBuilder().setType(LegacySQLTypeName.TIMESTAMP.toString()).build())
            .build();
        FieldValue value = FieldValue.of(FieldValue.Attribute.RANGE, range);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) query.convertCell(field, value, false);

        assertThat(result.get("start"), is(nullValue()));
        assertThat(result.get("end"), is(Instant.parse("2026-01-01T00:00:00Z")));
    }
}
