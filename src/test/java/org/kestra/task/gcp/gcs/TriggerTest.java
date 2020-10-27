package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.triggers.TriggerContext;
import org.kestra.core.queues.QueueFactoryInterface;
import org.kestra.core.queues.QueueInterface;
import org.kestra.core.repositories.ExecutionRepositoryInterface;
import org.kestra.core.repositories.LocalFlowRepositoryLoader;
import org.kestra.core.repositories.TriggerRepositoryInterface;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.schedulers.Scheduler;
import org.kestra.core.services.FlowListenersService;
import org.kestra.core.utils.ExecutorsUtils;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.gcp.gcs.models.Blob;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Named;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class TriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private ExecutorsUtils executorsUtils;

    @Inject
    private TriggerRepositoryInterface triggerContextRepository;

    @Inject
    private ExecutionRepositoryInterface executionRepository;

    @Inject
    private FlowListenersService flowListenersService;

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

    @Value("${kestra.variables.globals.random}")
    private String random;

    @BeforeEach
    private void init() throws IOException, URISyntaxException {
        repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows")));
    }

    @Test
    void flow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (Scheduler scheduler = new Scheduler(
            this.applicationContext,
            this.executorsUtils,
            this.executionQueue,
            this.flowListenersService,
            this.executionRepository,
            this.triggerContextRepository
        )) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(TriggerTest.class, execution -> {
                last.set(execution);

                queueCount.countDown();
                assertThat(execution.getFlowId(), is("gcs-listen"));
            });


            String out1 = FriendlyId.createFriendlyId();
            testUtils.upload(random + "/" + out1);
            String out2 = FriendlyId.createFriendlyId();
            testUtils.upload(random + "/" + out2);

            scheduler.run();

            queueCount.await();

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
            .from("gs://" + bucket + "/tasks/gcp/upload/" + random + "/")
            .action(Downloads.Action.MOVE)
            .moveDirectory("gs://" + bucket + "/test/move")
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = testUtils.upload(random + "/" + out);

        Map.Entry<RunContext, TriggerContext> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> urls = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(urls.size(), is(1));

        assertThrows(IllegalArgumentException.class, () -> {
            Download task = Download.builder()
                .id(DownloadTest.class.getSimpleName())
                .type(Download.class.getName())
                .from(upload.getUri().toString())
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });


        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from("gs://" + bucket + "/test/move/" + out + ".yml")
            .build();

        Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBucket(), is(bucket));
    }
}
