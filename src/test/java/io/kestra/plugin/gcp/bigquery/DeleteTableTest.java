package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DeleteTableTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void run() throws Exception {
        String table = "tbl_delete_" + FriendlyId.createFriendlyId();
        LocalDate today = LocalDate.now();
        LocalDate previous = LocalDate.now().minusDays(10);
        String partition = today.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        Query create = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("CREATE TABLE `" + project + "." + dataset + "." + table + "` (transaction_id INT64, transaction_date DATETIME)\n" +
                "PARTITION BY DATE(transaction_date)\n" +
                "AS (SELECT 1, DATETIME '" + today.format(DateTimeFormatter.ISO_LOCAL_DATE) + " 12:30:00.45')\n" +
                "UNION ALL\n" +
                "(SELECT 2, DATETIME '" + previous.format(DateTimeFormatter.ISO_LOCAL_DATE) + " 12:30:00.45')\n")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());
        create.run(runContext);

        DeleteTable task = DeleteTable.builder()
            .id(QueryTest.class.getSimpleName())
            .type(DeleteTable.class.getName())
            .projectId(this.project)
            .dataset(this.dataset)
            .table(table + "$" + partition)
            .build();
        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        DeleteTable.Output run = task.run(runContext);

        assertThat(run.getTable(), is(table + "$" + partition));

        Query query = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .fetchOne(true)
            .sql("SELECT COUNT(*) as cnt FROM `" + project + "." + dataset + "." + table + "`;")
            .build();
        runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output queryRun = query.run(runContext);

        assertThat(queryRun.getRow().get("cnt"), is(1L));
    }
}
