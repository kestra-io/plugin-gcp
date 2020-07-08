package org.kestra.task.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
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
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                .getResource("bigquery/insurance_sample.csv"))
                .toURI()))
        );

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .type(Load.class.getName())
            .from(source.toString())
            .destinationTable(project + "." + dataset + "." + FriendlyId.createFriendlyId())
            .format(AbstractLoad.Format.CSV)
            .autodetect(true)
            .csvOptions(AbstractLoad.CsvOptions.builder()
                .fieldDelimiter("|")
                .allowJaggedRows(true)
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
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                .getResource("bigquery/insurance_sample.avro"))
                .toURI()))
        );

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .type(Load.class.getName())
            .from(source.toString())
            .destinationTable(project + "." + dataset + "." + FriendlyId.createFriendlyId())
            .format(AbstractLoad.Format.AVRO)
            .avroOptions(AbstractLoad.AvroOptions.builder()
                .useAvroLogicalTypes(true)
                .build()
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        AbstractLoad.Output run = task.run(runContext);
        assertThat(run.getRows(), is(5L));
    }
}
