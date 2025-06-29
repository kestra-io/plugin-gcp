package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class StorageWriteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void run() throws Exception {
        String table = this.project  + "." + this.dataset + "." + FriendlyId.createFriendlyId();

        Query create = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("CREATE TABLE " + table + " AS " + "SELECT \n" +
                "  \"hello\" as string,\n" +
                "  CAST(NULL AS INT) AS `nullable`,\n" +
                "  TRUE AS `bool`,\n" +
                "  1 as int,\n" +
                "  1.25 AS float,\n" +
                "  DATE(\"2008-12-25\") AS date,\n" +
                "  DATETIME \"2008-12-25 15:30:00.123456\" AS datetime,\n" +
                "  TIME(DATETIME \"2008-12-25 15:30:00.123456\") AS time,\n" +
                "  TIMESTAMP(\"2008-12-25 15:30:00.123456\") AS timestamp,\n" +
                "  ST_GEOGPOINT(50.6833, 2.9) AS geopoint,\n" +
                "  ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS `array`,\n" +
                "  STRUCT(NULL as v, 4 AS x, 0 AS y, ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS z) AS `struct`"
            ))
            .build();
        create.run(TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of()));

        HashMap<String, Object> object = new HashMap<>();
        object.put("string", "hello");
        object.put("nullable", null);
        object.put("bool", true);
        object.put("int", 1L);
        object.put("float", 1.25D);
        object.put("date", LocalDate.parse("2008-12-25"));
        object.put("timestamp", ZonedDateTime.parse("2008-12-25T15:30:00.123+01:00"));
        object.put("time", LocalTime.parse("15:30:00.123456"));
        object.put("array", Arrays.asList(1L, 2L, 3L));
        object.put("struct", Map.of("x", 4L, "y", 0L, "z", Arrays.asList(1L, 2L, 3L)));
        object.put("datetime", LocalDateTime.parse("2008-12-25T15:30:00.123"));

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            FileSerde.write(outputStream, object);
        }

        URI put = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        StorageWrite task = StorageWrite.builder()
            .id("test-unit")
            .type(StorageWrite.class.getName())
            .destinationTable(Property.ofValue(table))
            .location(Property.ofValue("EU"))
            .from(Property.ofValue(put.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        StorageWrite.Output run = task.run(runContext);


//        assertThat(run.getRowsCount(), is(1));
        assertThat(run.getRows(), is(1));
    }
}
