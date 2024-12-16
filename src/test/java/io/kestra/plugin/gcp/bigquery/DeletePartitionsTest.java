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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class DeletePartitionsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(
                "(SELECT 1, DATETIME '2020-04-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 2, DATETIME '2020-04-01 13:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 3, DATETIME '2020-04-01 14:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 4, DATETIME '2020-04-01 15:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 5, DATETIME '2020-04-01 16:30:00.45')",
                "DATE_TRUNC(transaction_date, HOUR)",
                AbstractPartition.PartitionType.HOUR,
                "2020-04-01T13:00:00",
                "2020-04-01T15:00:00"
            ),
            Arguments.of(
                "(SELECT 1, DATETIME '2020-04-01 12:30:00.45')\n" +
                "UNION ALL\n" +
                "(SELECT 2, DATETIME '2020-04-02 12:30:00.45')\n" +
                "UNION ALL\n" +
                "(SELECT 3, DATETIME '2020-04-03 12:30:00.45')\n" +
                "UNION ALL\n" +
                "(SELECT 4, DATETIME '2020-04-04 12:30:00.45')\n" +
                "UNION ALL\n" +
                "(SELECT 5, DATETIME '2020-04-05 12:30:00.45')",
                "DATE(transaction_date)",
                AbstractPartition.PartitionType.DAY,
                "2020-04-02T00:00:00",
                "2020-04-04T00:00:00"
            ),
            Arguments.of(
                "(SELECT 1, DATETIME '2020-01-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 2, DATETIME '2020-02-02 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 3, DATETIME '2020-03-03 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 4, DATETIME '2020-04-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 5, DATETIME '2020-05-05 12:30:00.45')",
                "DATE_TRUNC(transaction_date, MONTH)",
                AbstractPartition.PartitionType.MONTH,
                "2020-02-01T00:00:00",
                "2020-04-04T00:00:00"
            ),
            Arguments.of(
                "(SELECT 1, DATETIME '2020-01-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 2, DATETIME '2021-02-02 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 3, DATETIME '2022-03-03 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 4, DATETIME '2023-04-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 5, DATETIME '2024-05-05 12:30:00.45')",
                "DATE_TRUNC(transaction_date, YEAR)",
                AbstractPartition.PartitionType.YEAR,
                "2021-01-01T00:00:00",
                "2023-01-01T00:00:00"
            ),
            Arguments.of(
                "(SELECT 1, DATETIME '2020-01-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 2, DATETIME '2021-02-02 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 3, DATETIME '2022-03-03 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 4, DATETIME '2023-04-01 12:30:00.45')\n" +
                    "UNION ALL\n" +
                    "(SELECT 5, DATETIME '2024-05-05 12:30:00.45')",
                "RANGE_BUCKET(transaction_id, GENERATE_ARRAY(0, 100, 1))\n",
                AbstractPartition.PartitionType.RANGE,
                "2",
                "4"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void deleteDate(
        String insert,
        String partition,
        AbstractPartition.PartitionType partitionType,
        String from,
        String to
    ) throws Exception {
        String table = "tbl_delete_" + FriendlyId.createFriendlyId();

        Query create = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.of("CREATE TABLE `" + project + "." + dataset + "." + table + "` (transaction_id INT64, transaction_date DATETIME)\n" +
                "PARTITION BY " + partition + "\n" +
                "AS " + insert
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());
        create.run(runContext);

        DeletePartitions task = DeletePartitions.builder()
            .id(QueryTest.class.getSimpleName())
            .type(DeleteTable.class.getName())
            .projectId(Property.of(this.project))
            .dataset(Property.of(this.dataset))
            .partitionType(Property.of(partitionType))
            .table(Property.of(table))
            .from(Property.of(from))
            .to(Property.of(to))
            .build();
        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        DeletePartitions.Output run = task.run(runContext);

        assertThat(run.getPartitions().size(), is(3));

        Query query = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .fetchOne(true)
            .sql(Property.of("SELECT COUNT(*) as cnt FROM `" + project + "." + dataset + "." + table + "`;"))
            .build();
        runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output queryRun = query.run(runContext);

        assertThat(queryRun.getRow().get("cnt"), is(2L));
    }
}
