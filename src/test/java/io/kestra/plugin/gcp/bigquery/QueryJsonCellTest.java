package io.kestra.plugin.gcp.bigquery;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.LegacySQLTypeName;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class QueryJsonCellTest {
    private static Object convert(String json) {
        var field = Field.of("j", LegacySQLTypeName.JSON);
        var value = FieldValue.of(FieldValue.Attribute.PRIMITIVE, json);

        return Query.builder().build().convertCell(field, value, false);
    }

    @Test
    void shouldReadAJsonObject() {
        assertThat(convert("{\"a\":1}"), is(Map.of("a", 1)));
    }

    @Test
    void shouldReadAJsonArray() {
        assertThat(convert("[1,2,3]"), is(List.of(1, 2, 3)));
    }

    @Test
    void shouldReadAJsonString() {
        assertThat(convert("\"text\""), is("text"));
    }

    @Test
    void shouldReadAJsonNumber() {
        assertThat(convert("5"), is(5));
    }

    @Test
    void shouldReadAJsonBoolean() {
        assertThat(convert("true"), is(true));
    }
}
