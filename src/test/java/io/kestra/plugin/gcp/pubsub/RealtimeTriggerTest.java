package io.kestra.plugin.gcp.pubsub;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class RealtimeTriggerTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private SchedulerTriggerStateInterface triggerState;

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
        try (
            Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null);
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(RealtimeTriggerTest.class, execution -> {
                last.set(execution.getLeft());

                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("realtime-listen"));
            });


            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader().getResource("flows/pubsub/realtime.yaml")));

            // publish message to trigger the flow
            Publish task = Publish.builder()
                .id(Publish.class.getSimpleName())
                .type(Publish.class.getName())
                .topic("test-topic")
                .projectId(this.project)
                .from(
                    List.of(
                        Message.builder().data("Hello World").build()
                    )
                )
                .build();
            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

            queueCount.await(1, TimeUnit.MINUTES);

            Map<String, Object> variables = last.get().getTrigger().getVariables();
            assertThat(new String(Base64.getDecoder().decode((String) variables.get("data")), StandardCharsets.UTF_8), is("Hello World"));
        }
    }
}
