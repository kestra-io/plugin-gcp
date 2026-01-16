package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class CopyPartitionsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void run() throws Exception {
        String table = "tbl_copy_" + FriendlyId.createFriendlyId();
        String destinationTable = "tbl_copydest_" + FriendlyId.createFriendlyId();

        Query create = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue("CREATE TABLE `" + project + "." + dataset + "." + table + "` (transaction_id INT64, transaction_date DATETIME)\n" +
                    "PARTITION BY DATE(transaction_date)\n" +
                    "AS (SELECT 1, DATETIME '2020-04-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 2, DATETIME '2020-04-02 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 3, DATETIME '2020-04-03 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 4, DATETIME '2020-04-04 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 5, DATETIME '2020-04-05 12:30:00.45')"
                ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());
        create.run(runContext);

        // 2020-04-02T14:30:00.450000+02:00
        CopyPartitions task = CopyPartitions.builder()
            .id(QueryTest.class.getSimpleName())
            .type(CopyPartitions.class.getName())
            .projectId(Property.ofValue(this.project))
            .dataset(Property.ofValue(this.dataset))
            .partitionType(Property.ofValue(AbstractPartition.PartitionType.DAY))
            .table(Property.ofValue(table))
            .from(Property.ofExpression("{{ '2020-04-02' | date() }}"))
            .to(Property.ofExpression("{{ '2020-04-04' | date() }}"))
            .destinationTable(Property.ofValue(this.project + "." + this.dataset + "." + destinationTable))
            .build();
        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        CopyPartitions.Output run = task.run(runContext);

        assertThat(run.getPartitions().size(), is(3));

        Query query = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .projectId(Property.ofValue(project))
            .fetchOne(true)
            .sql(Property.ofValue("SELECT COUNT(*) as cnt FROM `" + project + "." + dataset + "." + destinationTable + "`;"))
            .build();
        runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output queryRun = query.run(runContext);

        assertThat(queryRun.getRow().get("cnt"), is(3L));
    }
}
