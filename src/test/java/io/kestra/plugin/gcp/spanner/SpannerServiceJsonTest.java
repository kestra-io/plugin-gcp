package io.kestra.plugin.gcp.spanner;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SpannerServiceJsonTest {
    @Test
    void shouldRejectInvalidJsonWithColumnContext() {
        var row = Struct.newBuilder().set("payload").to(Value.json("{invalid")).build();

        var exception = assertThrows(IllegalArgumentException.class, () -> SpannerService.rowToMap(row));

        assertThat(exception.getMessage(), containsString("Invalid JSON in column 'payload'"));
        assertThat(exception.getMessage(), containsString("Check the JSON value stored in this column."));
    }

    @ParameterizedTest
    @MethodSource("jsonValues")
    void shouldDeserializeJsonValues(String json, Object expected) {
        var row = Struct.newBuilder().set("value").to(Value.json(json)).build();

        assertThat(SpannerService.rowToMap(row).get("value"), is(expected));
    }

    static Stream<Arguments> jsonValues() {
        return Stream.of(
            Arguments.of("{\"a\":1}", Map.of("a", 1)),
            Arguments.of("[1,2,3]", List.of(1, 2, 3)),
            Arguments.of("{\"a\":[1,{\"b\":null}]}", Map.of("a", List.of(1, Collections.singletonMap("b", null)))),
            Arguments.of("{}", Map.of()),
            Arguments.of("[]", List.of()),
            Arguments.of("\"text\"", "text"),
            Arguments.of("5", 5),
            Arguments.of("12345678901234567890", new BigInteger("12345678901234567890")),
            Arguments.of("1.5", 1.5D),
            Arguments.of("true", true),
            Arguments.of("null", null),
            Arguments.of(null, null)
        );
    }
}
