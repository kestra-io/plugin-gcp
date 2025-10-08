package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
        DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null);
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
            repositoryLoader.load(MAIN_TENANT, Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/gcs")));

            boolean await = queueCount.await(20, TimeUnit.SECONDS);
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
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/trigger/"))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveDirectory(Property.ofValue("gs://" + bucket + "/test/move"))
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
                .from(Property.ofValue(upload.getUri().toString()))
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });


        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/test/move/" + out + ".yml"))
            .build();

        Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBucket(), is(bucket));
    }

    @Test
    void none() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/trigger/none"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(Trigger.OnEvent.CREATE))
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = testUtils.upload("trigger/none" + out);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> urls = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(urls.size(), is(1));

        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(upload.getUri().toString()))
            .build();

        Assertions.assertDoesNotThrow(() -> task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of())));

        Delete delete = Delete.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.ofValue(upload.getUri().toString()))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }

    @Test
    void shouldExecuteOnCreate() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/trigger/on-create/"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(Trigger.OnEvent.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        String file = "trigger/on-create/" + FriendlyId.createFriendlyId();
        var upload = testUtils.upload(file);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        Delete delete = Delete.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.ofValue(upload.getUri().toString()))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of()));
    }

    @Test
    void shouldExecuteOnUpdate() throws Exception {
        String file = "trigger/on-update/" + FriendlyId.createFriendlyId();
        var upload = testUtils.upload(file);

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/trigger/on-update/"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(Trigger.OnEvent.UPDATE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue());

        // we update the file to trigger the update event
        testUtils.update(file);
        Thread.sleep(3000);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        Delete delete = Delete.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.ofValue(upload.getUri().toString()))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of()));
    }

    @Test
    void shouldExecuteOnCreateOrUpdate() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/trigger/on-create_or_update/"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(Trigger.OnEvent.CREATE_OR_UPDATE))
            .interval(Duration.ofSeconds(10))
            .build();

        String file = "trigger/on-create_or_update/" + FriendlyId.createFriendlyId();
        var upload = testUtils.upload(file);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> createExecution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(createExecution.isPresent(), is(true));

        // we update the file to trigger the update event
        testUtils.update(file);
        Thread.sleep(3000);

        var updateExecution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(updateExecution.isPresent(), is(true));

        Delete delete = Delete.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.ofValue(upload.getUri().toString()))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of()));
    }
}
