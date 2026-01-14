package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwSupplier;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private GcsTestUtils testUtils;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void moveFromFlow() throws Exception {
        String folderId = IdUtils.create();

        var fromDir = "tasks/gcp/upload/trigger/move-from/" + folderId + "/";
        var moveDir = "tasks/gcp/move-to/" + folderId + "/";

        testUtils.upload("trigger/move-from/" + folderId + "/" + IdUtils.create());
        testUtils.upload("trigger/move-from/" + folderId + "/" + IdUtils.create());

        io.kestra.plugin.gcp.gcs.Trigger trigger = io.kestra.plugin.gcp.gcs.Trigger.builder()
            .id("watch")
            .type(io.kestra.plugin.gcp.gcs.Trigger.class.getName())
            .from(Property.ofValue(String.format("gs://%s/%s", bucket, fromDir)))
            .interval(java.time.Duration.ofSeconds(10))
            .action(Property.ofValue(io.kestra.plugin.gcp.gcs.Trigger.Action.MOVE))
            .moveDirectory(Property.ofValue(String.format("gs://%s/%s", bucket, moveDir)))
            .build();


        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> blobs = (List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(blobs.size(), is(2));
    }

    @Test
    void move() throws Exception {
        String out = FriendlyId.createFriendlyId();
        String fileId = IdUtils.create();

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/trigger/" + out + "/"))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveDirectory(Property.ofValue("gs://" + bucket + "/test/move"))
            .build();

        Upload.Output upload = testUtils.upload("trigger/" + out + "/" + fileId);

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
            .from(Property.ofValue("gs://" + bucket + "/test/move/" + fileId + ".yml"))
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
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
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
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        String file = "trigger/on-create/" + FriendlyId.createFriendlyId();
        var upload = testUtils.upload(file);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = Optional.ofNullable(Await.until(
            throwSupplier(() -> trigger.evaluate(context.getKey(), context.getValue()).orElse(null)),
            Duration.ofMillis(500),
            Duration.ofSeconds(20)
        ));

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
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue());

        // we update the file to trigger the update event
        testUtils.update(file);
        Thread.sleep(3000);

        Optional<Execution> execution = Optional.ofNullable(Await.until(
            throwSupplier(() -> trigger.evaluate(context.getKey(), context.getValue()).orElse(null)),
            Duration.ofMillis(500),
            Duration.ofSeconds(20)
        ));

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
            .interval(Duration.ofSeconds(10))
            .build();

        String file = "trigger/on-create_or_update/" + FriendlyId.createFriendlyId();
        var upload = testUtils.upload(file);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> createExecution = Optional.ofNullable(Await.until(
            throwSupplier(() -> trigger.evaluate(context.getKey(), context.getValue()).orElse(null)),
            Duration.ofMillis(500),
            Duration.ofSeconds(20)
        ));

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
