package io.kestra.plugin.gcp.bigquery;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class QueryGeographyTest {
    @Test
    void shouldReadAPointWithANegativeCoordinateAndKeepAnyOtherShape() {
        assertThat(Query.geographyValue("POINT(50.6833 2.9)"), is(List.of(50.6833d, 2.9d)));
        assertThat(Query.geographyValue("POINT(-74.006 40.7128)"), is(List.of(-74.006d, 40.7128d)));
        assertThat(Query.geographyValue("POINT(-0.1278 -51.5074)"), is(List.of(-0.1278d, -51.5074d)));
        assertThat(Query.geographyValue("POLYGON((0 0, 1 0, 1 1, 0 0))"), is("POLYGON((0 0, 1 0, 1 1, 0 0))"));
        assertThat(Query.geographyValue("LINESTRING(-74 40, -73 41)"), is("LINESTRING(-74 40, -73 41)"));
    }
}
