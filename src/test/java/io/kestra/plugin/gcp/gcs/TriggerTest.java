package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Inject
    private GcsTestUtils testUtils;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void moveFromFlow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
                Execution execution = executionWithError.getLeft();
                if (execution.getFlowId().equals("gcs-listen")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });


            String out1 = FriendlyId.createFriendlyId();
            testUtils.upload("trigger/" + out1);
            String out2 = FriendlyId.createFriendlyId();
            testUtils.upload("trigger/" + out2);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/gcs")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
                receive.blockLast();
            }

            @SuppressWarnings("unchecked")
            java.util.List<Blob> trigger = (java.util.List<Blob>) last.get().getTrigger().getVariables().get("blobs");

            assertThat(trigger.size(), is(2));
        }
    }

    @Test
    void move() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .from(Property.of("gs://" + bucket + "/tasks/gcp/upload/trigger/"))
            .action(Property.of(ActionInterface.Action.MOVE))
            .moveDirectory(Property.of("gs://" + bucket + "/test/move"))
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = testUtils.upload( "trigger/" + out);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> urls = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(urls.size(), is(1));

        assertThrows(IllegalArgumentException.class, () -> {
            Download task = Download.builder()
                .id(DownloadTest.class.getSimpleName())
                .type(Download.class.getName())
                .from(Property.of(upload.getUri().toString()))
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });


        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.of("gs://" + bucket + "/test/move/" + out + ".yml"))
            .build();

        Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBucket(), is(bucket));
    }

    @Test
    void none() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .from(Property.of("gs://" + bucket + "/tasks/gcp/upload/trigger/"))
            .action(Property.of(ActionInterface.Action.NONE))
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = testUtils.upload("trigger/" + out);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> urls = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(urls.size(), is(1));

        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.of(upload.getUri().toString()))
            .build();

        Assertions.assertDoesNotThrow(() -> task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of())));

        Delete delete = Delete.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.of(upload.getUri().toString()))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
