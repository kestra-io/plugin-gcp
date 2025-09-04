package io.kestra.plugin.gcp.pubsub;

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
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
                assertThat(execution.getLeft().getFlowId(), is("pubsub-listen"));
            });


            worker.run();
            scheduler.run();

            repositoryLoader.load(MAIN_TENANT, Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/pubsub/pubsub-listen.yaml")));

            // publish two messages to trigger the flow
            Publish task = Publish.builder()
                .id(Publish.class.getSimpleName())
                .type(Publish.class.getName())
                .topic(Property.ofValue("test-topic"))
                .projectId(Property.ofValue(this.project))
                .from(
                    List.of(
                        Message.builder().data("Hello World".getBytes()).build(),
                        Message.builder().attributes(Map.of("key", "value")).build()
                    )
                )
                .build();
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            Execution last = receive.blockLast();
            var count = (Integer) last.getTrigger().getVariables().get("count");
            var uri = (String) last.getTrigger().getVariables().get("uri");
            assertThat(count, is(2));
            assertThat(uri, is(notNullValue()));
        }
    }
}
