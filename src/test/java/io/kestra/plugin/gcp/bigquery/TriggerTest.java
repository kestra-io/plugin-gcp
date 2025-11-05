package io.kestra.plugin.gcp.bigquery;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.serializers.FileSerde;
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

import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Value("${kestra.variables.globals.dataset}")
    private String dataset;

    @Test
    void flow() throws Exception {
        var tableName = String.format("%s.%s.%s", project, dataset, IdUtils.create());

        Query createTable = Query.builder()
            .id("create-" + IdUtils.create())
            .type(Query.class.getName())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue(
                "CREATE OR REPLACE TABLE `" + tableName + "` AS (SELECT 1 AS number UNION ALL SELECT 2 AS number)"
            ))
            .build();

        var output  = createTable.run(TestsUtils.mockRunContext(runContextFactory, createTable, Map.of()));
        assertThat(output, notNullValue());

        Trigger trigger = Trigger.builder()
            .id("watch")
            .type(io.kestra.plugin.gcp.bigquery.Trigger.class.getName())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue(
                "SELECT * FROM `" + tableName + "`"
            ))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        @SuppressWarnings("unchecked")
        java.util.List<Blob> blobs = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("rows");

        assertThat(blobs.size(), is(2));

        Query deleteTable = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue("DROP TABLE `" + tableName + "`"))
            .build();
        deleteTable.run(TestsUtils.mockRunContext(runContextFactory, createTable, ImmutableMap.of()));
    }
}

