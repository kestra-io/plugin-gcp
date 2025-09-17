package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class LoadTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void fromCsv() throws Exception {
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                .getResource("bigquery/insurance_sample.csv"))
                .toURI()))
        );

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .type(Load.class.getName())
            .projectId(Property.ofValue(project))
            .from(Property.ofValue(source.toString()))
            .destinationTable(Property.ofValue(project + "." + dataset + "." + FriendlyId.createFriendlyId()))
            .format(AbstractLoad.Format.CSV)
            .autodetect(Property.ofValue(true))
            .csvOptions(AbstractLoad.CsvOptions.builder()
                .fieldDelimiter(Property.ofValue("|"))
                .allowJaggedRows(Property.ofValue(true))
                .build()
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        AbstractLoad.Output run = task.run(runContext);

        assertThat(run.getRows(), is(5L));
    }

    @Test
    void fromAvro() throws Exception {
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                .getResource("bigquery/insurance_sample.avro"))
                .toURI()))
        );

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .type(Load.class.getName())
            .projectId(Property.ofValue(project))
            .from(Property.ofValue(source.toString()))
            .destinationTable(Property.ofValue(project + "." + dataset + "." + FriendlyId.createFriendlyId()))
            .format(AbstractLoad.Format.AVRO)
            .avroOptions(AbstractLoad.AvroOptions.builder()
                .useAvroLogicalTypes(Property.ofValue(true))
                .build()
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        AbstractLoad.Output run = task.run(runContext);
        assertThat(run.getRows(), is(5L));
    }


    @Test
    void fromEmpty() throws Exception {
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            IOUtils.toInputStream("", StandardCharsets.UTF_8)
        );

        Load.LoadBuilder<?, ?> task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .type(Load.class.getName())
            .projectId(Property.ofValue(project))
            .from(Property.ofValue(source.toString()))
            .destinationTable(Property.ofValue(project + "." + dataset + "." + FriendlyId.createFriendlyId()))
            .format(AbstractLoad.Format.CSV)
            .autodetect(Property.ofValue(true))
            .csvOptions(AbstractLoad.CsvOptions.builder()
                .fieldDelimiter(Property.ofValue("|"))
                .allowJaggedRows(Property.ofValue(true))
                .build()
            );

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task.build(), ImmutableMap.of());

        Exception exception = assertThrows(Exception.class, () -> task.build().run(runContext));
        assertThat(exception.getMessage(), containsString("Can't load an empty file"));

        AbstractLoad.Output run = task.failedOnEmpty(Property.ofValue(false)).build().run(runContext);
        assertThat(run.getRows(), is(0L));
    }
}
