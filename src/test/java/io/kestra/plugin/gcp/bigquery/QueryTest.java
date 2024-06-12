package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.JobInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import dev.failsafe.FailsafeException;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;

import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.inject.Inject;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.kestra.core.utils.Rethrow.throwRunnable;

@KestraTest
@Slf4j
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    static String sql() {
        return "SELECT \n" +
            "  \"hello\" as string,\n" +
            "  CAST(NULL AS INT) AS `nullable`,\n" +
            "  TRUE AS `bool`,\n" +
            "  1 as int,\n" +
            "  1.25 AS float,\n" +
            "  CAST(1.25 AS BIGNUMERIC) AS bignumeric,\n" +
            "  DATE(\"2008-12-25\") AS date,\n" +
            "  DATETIME \"2008-12-25 15:30:00.123456\" AS datetime,\n" +
            "  TIME(DATETIME \"2008-12-25 15:30:00.123456\") AS time,\n" +
            "  TIMESTAMP(\"2008-12-25 15:30:00.123456\") AS timestamp,\n" +
            "  ST_GEOGPOINT(50.6833, 2.9) AS geopoint,\n" +
            "  ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS `array`,\n" +
            "  STRUCT(NULL as v, 4 AS x, 0 AS y, ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS z) AS `struct`," +
            "  RANGE(DATE '2022-12-01', DATE '2022-12-31') AS `range`," +
            "  INTERVAL 1 YEAR AS `interval`," +
            "  JSON '{\"name\": \"Alice\", \"age\": 30}' AS `json`";

    }

    @Test
    @SuppressWarnings("unchecked")
    void fetch() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of(
            "sql", sql(),
            "flow", ImmutableMap.of("id", FriendlyId.createFriendlyId(), "namespace", "io.kestra.tests"),
            "execution", ImmutableMap.of("id", FriendlyId.createFriendlyId()),
            "taskrun", ImmutableMap.of("id", FriendlyId.createFriendlyId())
        ));

        Query task = Query.builder()
            .sql("{{sql}}")
            .location("EU")
            .fetch(true)
            .build();

        Query.Output run = task.run(runContext);

        List<Map<String, Object>> rows = (List<Map<String, Object>>) run.getRows();
        assertThat(rows.size(), is(1));

        assertThat(rows.get(0).get("string"), is("hello"));
        assertThat(rows.get(0).get("nullable"), is(nullValue()));
        assertThat(rows.get(0).get("int"), is(1L));
        assertThat(rows.get(0).get("float"), is(1.25D));
        assertThat(rows.get(0).get("bignumeric"), is(new BigDecimal("1.25")));
        assertThat(rows.get(0).get("date"), is(LocalDate.parse("2008-12-25")));
        assertThat(rows.get(0).get("time"), is(LocalTime.parse("15:30:00.123456")));
        assertThat(rows.get(0).get("timestamp"), is(Instant.parse("2008-12-25T15:30:00.123456Z")));
        assertThat((List<Double>) rows.get(0).get("geopoint"), containsInAnyOrder(50.6833, 2.9));
        assertThat((List<Long>) rows.get(0).get("array"), containsInAnyOrder(1L, 2L, 3L));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("v"), is(nullValue()));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("x"), is(4L));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("y"), is(0L));
        assertThat((List<Long>) ((Map<String, Object>) rows.get(0).get("struct")).get("z"), containsInAnyOrder(1L, 2L, 3L));
        assertThat(rows.get(0).get("range"), is("[2022-12-01, 2022-12-31)"));
        assertThat(rows.get(0).get("interval"), is("1-0 0 0:0:0"));
        assertThat(((Map<String, Object>) rows.get(0).get("json")).get("name"), is("Alice"));
        assertThat(((Map<String, Object>) rows.get(0).get("json")).get("age"), is(30));
    }

    @Test
    void store() throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(sql() + "\n UNION ALL \n " + sql())
            .store(true)
            .build();

        Query.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get(null, run.getUri()))),
            is(StringUtils.repeat(
                "{string:\"hello\",nullable:null,bool:true,int:1,float:1.25e0,bignumeric:1.25,date:2008-12-25,datetime:2008-12-25T15:30:00.123Z,time:LocalTime::\"15:30:00.123456\",timestamp:2008-12-25T15:30:00.123Z,geopoint:[50.6833e0,2.9e0],array:[1,2,3],struct:{v:null,x:4,y:0,z:[1,2,3]},range:\"[2022-12-01, 2022-12-31)\",interval:\"1-0 0 0:0:0\",json:{age:30,name:\"Alice\"}}\n",
                2
            ))
        );
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
        String friendlyId = FriendlyId.createFriendlyId();
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("{% for input in inputs.loop %}" +
                "SELECT" +
                "  \"{{execution.id}}\" as execution_id," +
                "  TIMESTAMP \"{{execution.startDate | date(\"yyyy-MM-dd HH:mm:ss.SSSSSS\")}}\" as execution_date," +
                "  {{ input }} as counter" +
                "{{ loop.last  == false ? '\nUNION ALL\n' : '\n' }}" +
                "{% endfor %}"
            )
            .destinationTable(project + "." + dataset + "." + friendlyId)
            .timePartitioningField("execution_date")
            .clusteringFields(Arrays.asList("execution_id", "counter"))
            .schemaUpdateOptions(Collections.singletonList(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION))
            .writeDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
            "loop", IntStream.range(1, 26).boxed().toList()
        ));

        Query.Output run = task.run(runContext);
        assertThat(run.getJobId(), is(notNullValue()));
        assertThat(run.getDestinationTable().getProject(),is(project));
        assertThat(run.getDestinationTable().getDataset(),is(dataset));
        assertThat(run.getDestinationTable().getTable(),is(friendlyId));
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

        assertThat(e.getCause().getMessage(), containsString("must be qualified with a dataset"));
    }

    @Test
    void script() throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("{% for input in inputs.loop %}" +
                "SELECT" +
                "  \"{{execution.id}}\" as execution_id," +
                "  TIMESTAMP \"{{execution.startDate | date(\"yyyy-MM-dd HH:mm:ss.SSSSSS\") }}\" as execution_date," +
                "  {{input}} as counter;" +
                "{% endfor %}"
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
            "loop", IntStream.range(1, 3).boxed().toList()
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
            .toList();

        assertThat(results.size(), is(tasks.size()));
    }

    @Test
    void scriptError() {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("{% for input in  inputs.loop %}" +
                "SELECT * from `{{execution.id}}`;" +
                "{% endfor %}"
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
            "loop", IntStream.range(1, 3).boxed().toList()
        ));

        FailsafeException e = assertThrows(FailsafeException.class, () -> {
            task.run(runContext);
        });

        assertThat(e.getMessage(), containsString("must be qualified with a dataset"));
    }

    @Test
    void scriptMultipleNoOutput() throws Exception {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql("DROP TABLE IF EXISTS `" + project + "." + this.dataset + ".not`;" +
                "DROP TABLE IF EXISTS `" + project + "." + this.dataset + ".exist`;"
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        Query.Output run = task.run(runContext);

        assertThat(run.getDestinationTable(), nullValue());
        assertThat(run.getJobId(), notNullValue());
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
