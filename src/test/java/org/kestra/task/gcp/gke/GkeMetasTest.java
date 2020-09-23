package org.kestra.task.gcp.gke;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.gcp.bigquery.AbstractLoad;
import org.kestra.task.gcp.bigquery.Load;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class GkeMetasTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;
    /*
    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;
    */
    @Test
    void getToken() throws Exception {

        GkeMetas task = GkeMetas.builder()
            .id(GkeMetasTest.class.getSimpleName())
            .type(Load.class.getName())
            .zone("europe-west1-c")
            .clusterId("dcp-tools")
            .projectId("lmfr-ddp-dcp-prd")
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        GkeMetas.Output run = task.run(runContext);

        assertThat(run.getToken(), is("owi"));

//        URI source = storageInterface.put(
//            new URI("/" + FriendlyId.createFriendlyId()),
//            new FileInputStream(new File(Objects.requireNonNull(GkeMetasTest.class.getClassLoader()
//                .getResource("bigquery/insurance_sample.csv"))
//                .toURI()))
//        );
//
//        Load task = Load.builder()
//            .id(GkeMetasTest.class.getSimpleName())
//            .type(Load.class.getName())
//            .from(source.toString())
//            .destinationTable(project + "." + dataset + "." + FriendlyId.createFriendlyId())
//            .format(AbstractLoad.Format.CSV)
//            .autodetect(true)
//            .csvOptions(AbstractLoad.CsvOptions.builder()
//                .fieldDelimiter("|")
//                .allowJaggedRows(true)
//                .build()
//            )
//            .build();
//
//        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
//
//        AbstractLoad.Output run = task.run(runContext);
//
//        assertThat(run.getRows(), is(5L));
    }

}
