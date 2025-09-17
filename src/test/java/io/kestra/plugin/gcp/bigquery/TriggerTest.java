package io.kestra.plugin.gcp.bigquery;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Value("${kestra.variables.globals.dataset}")
    private String dataset;

    @Value("${kestra.variables.globals.table}")
    private String table;

    @Test
    void flow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
        ) {
            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("bigquery-listen"));
            });

            String tableName = table.contains(".") ? table : project + "." + dataset + "." + table;
            Query createTable = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .projectId(Property.ofValue(project))
                .sql(Property.ofValue("CREATE OR REPLACE TABLE `" + tableName + "` AS (SELECT 1 AS number UNION ALL SELECT 2 AS number)"))
                .build();

            worker.run();
            scheduler.run();

            repositoryLoader.load(MAIN_TENANT, Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/bigquery")));

            createTable.run(TestsUtils.mockRunContext(runContextFactory, createTable, ImmutableMap.of()));

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            @SuppressWarnings("unchecked")
            java.util.List<Blob> trigger = (java.util.List<Blob>) receive.blockLast().getTrigger().getVariables().get("rows");

            assertThat(trigger.size(), is(2));

            Query deleteTable = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .projectId(Property.ofValue(project))
                .sql(Property.ofValue("DROP TABLE `" + project + "." + dataset + "." + table + "`"))
                .build();
            deleteTable.run(TestsUtils.mockRunContext(runContextFactory, createTable, ImmutableMap.of()));
        }
    }
}
