package org.kestra.task.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.JobInfo;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.FailsafeException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.utils.TestsUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.inject.Inject;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.kestra.core.utils.Rethrow.throwRunnable;

@MicronautTest
@Slf4j
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    @SuppressWarnings("unchecked")
    void fetch() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of(
            "sql", "SELECT \n" +
                "  \"hello\" as string,\n" +
                "  NULL AS `nullable`,\n" +
                "  1 as int,\n" +
                "  1.25 AS float,\n" +
                "  DATE(\"2008-12-25\") AS date,\n" +
                "  DATETIME \"2008-12-25 15:30:00.123456\" AS datetime,\n" +
                "  TIME(DATETIME \"2008-12-25 15:30:00.123456\") AS time,\n" +
                "  TIMESTAMP(\"2008-12-25 15:30:00.123456\") AS timestamp,\n" +
                "  ST_GEOGPOINT(50.6833, 2.9) AS geopoint,\n" +
                "  ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS `array`,\n" +
                "  STRUCT(4 AS x, 0 AS y, ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS z) AS `struct`",
            "flow", ImmutableMap.of("id", FriendlyId.createFriendlyId(), "namespace", "org.kestra.tests"),
            "execution", ImmutableMap.of("id", FriendlyId.createFriendlyId()),
            "taskrun", ImmutableMap.of("id", FriendlyId.createFriendlyId())
        ));

        Query task = Query.builder()
            .sql("{{sql}}")
            .fetch(true)
            .build();

        Query.Output run = task.run(runContext);

        List<Map<String, Object>> rows = (List<Map<String, Object>>) run.getRows();
        assertThat(rows.size(), is(1));

        assertThat(rows.get(0).get("string"), is("hello"));
        assertThat(rows.get(0).get("nullable"), is(nullValue()));
        assertThat(rows.get(0).get("int"), is(1L));
        assertThat(rows.get(0).get("float"), is(1.25D));
        assertThat(rows.get(0).get("date"), is(LocalDate.parse("2008-12-25")));
        assertThat(rows.get(0).get("time"), is(LocalTime.parse("15:30:00.123456")));
        assertThat(rows.get(0).get("timestamp"), is(Instant.parse("2008-12-25T15:30:00.123Z")));
        assertThat((List<Double>) rows.get(0).get("geopoint"), containsInAnyOrder(50.6833, 2.9));
        assertThat((List<Long>) rows.get(0).get("array"), containsInAnyOrder(1L, 2L, 3L));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("x"), is(4L));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("y"), is(0L));
        assertThat((List<Long>) ((Map<String, Object>) rows.get(0).get("struct")).get("z"), containsInAnyOrder(1L, 2L, 3L));
    }

    @Test
    void fetchLongPage() throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("SELECT repository_forks FROM `bigquery-public-data.samples.github_timeline` LIMIT 100000")
            .fetch(true)
            .build();

        Query.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(100000));
    }

    @Test
    void destination() throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("{{#each inputs.loop}}" +
                "SELECT" +
                "  \"{{execution.id}}\" as execution_id," +
                "  TIMESTAMP \"{{dateFormat execution.startDate \"yyyy-MM-dd HH:mm:ss.SSSSSS\"}}\" as execution_date," +
                "  {{@key}} as counter" +
                "{{#unless @last}}\nUNION ALL\n{{/unless}}" +
                "{{/each}}"
            )
            .destinationTable(project + "." + dataset + "." + FriendlyId.createFriendlyId())
            .timePartitioningField("execution_date")
            .clusteringFields(Arrays.asList("execution_id", "counter"))
            .schemaUpdateOptions(Collections.singletonList(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION))
            .writeDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
            "loop", ContiguousSet.create(Range.closed(1, 25), DiscreteDomain.integers())
        ));

        Query.Output run = task.run(runContext);
        assertThat(run.getJobId(), is(notNullValue()));
    }

    @Test
    void error() {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("SELECT * from `{{execution.id}}`")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        FailsafeException e = assertThrows(FailsafeException.class, () -> {
            task.run(runContext);
        });

        assertThat(e.getCause().getMessage(), containsString("missing dataset while no default dataset"));
    }

    @Test
    void script() throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("{{#each inputs.loop}}" +
                "SELECT" +
                "  \"{{execution.id}}\" as execution_id," +
                "  TIMESTAMP \"{{dateFormat execution.startDate \"yyyy-MM-dd HH:mm:ss.SSSSSS\"}}\" as execution_date," +
                "  {{@key}} as counter;" +
                "{{/each}}"
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
            "loop", ContiguousSet.create(Range.closed(1, 2), DiscreteDomain.integers())
        ));

        Query.Output run = task.run(runContext);
        assertThat(run.getJobId(), is(notNullValue()));
    }

    @Test
    void retry() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
        String table = project + "." + dataset + "." + FriendlyId.createFriendlyId();

        List<Callable<Query.Output>> tasks = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("SELECT \"" + i + "\" as value")
                .destinationTable(table)
                .createDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                .writeDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                .build();

            RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

            tasks.add(() -> task.run(runContext));
        }

        List<Future<Query.Output>> futures = executorService.invokeAll(tasks);
        executorService.shutdown();

        List<Query.Output> results = futures
            .stream()
            .map(outputFuture -> {
                try {
                    return outputFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Failed on ", e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        assertThat(results.size(), is(tasks.size()));
    }

    @Test
    void scriptError() {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("{{#each inputs.loop}}" +
                "SELECT * from `{{execution.id}}`;" +
                "{{/each}}"
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
            "loop", ContiguousSet.create(Range.closed(1, 2), DiscreteDomain.integers())
        ));

        FailsafeException e = assertThrows(FailsafeException.class, () -> {
            task.run(runContext);
        });

        assertThat(e.getCause().getMessage(), containsString("missing dataset while no default dataset"));
    }

    @Test
    @Disabled
    void concurrency() throws Exception {
        String table = this.dataset + "." + FriendlyId.createFriendlyId();

        Query create = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("CREATE TABLE " + table + " AS SELECT 1 AS number")
            .build();

        create.run(TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of()));

        ExecutorService executorService = Executors.newFixedThreadPool(250);

        final int COUNT = 1000;

        CountDownLatch countDownLatch = new CountDownLatch(COUNT);

        Query task = Query.builder()
            .id("test")
            .type(Query.class.getName())
            .sql("SELECT * FROM " + table + ";")
            .fetchOne(true)
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        for (int i = 0; i < COUNT; i++) {
            executorService.execute(throwRunnable(() -> {
                Query.Output result = task.run(runContext);
                assertThat(result.getRow().get("number"), is(1L));

                countDownLatch.countDown();
            }));
        }

        countDownLatch.await();
    }
}
