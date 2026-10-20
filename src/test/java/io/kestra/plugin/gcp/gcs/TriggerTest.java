package io.kestra.plugin.gcp.gcs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.gcs.models.Blob;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwSupplier;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers
class TriggerTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private GcsTestUtils testUtils;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    private AutoCloseable emulatorEndpoint;
    private final List<String> cleanupPrefixes = new ArrayList<>();

    @BeforeEach
    void useFlociEndpoint() {
        emulatorEndpoint = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint());
    }

    @AfterEach
    void cleanUpGcsObjects() throws Exception {
        try {
            for (String prefix : cleanupPrefixes) {
                var objects = new ArrayList<com.google.cloud.storage.Blob>();
                storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll().forEach(objects::add);
                objects.forEach(blob -> storage.delete(blob.getBlobId()));
            }
        } finally {
            if (emulatorEndpoint != null) {
                emulatorEndpoint.close();
            }
        }
    }

    @Test
    void moveFromFlow() throws Exception {
        String folderId = IdUtils.create();

        var fromDir = "tasks/gcp/upload/trigger/move-from/" + folderId + "/";
        var moveDir = "tasks/gcp/move-to/" + folderId + "/";
        cleanupPrefixes.add(fromDir);
        cleanupPrefixes.add(moveDir);

        testUtils.upload("trigger/move-from/" + folderId + "/" + IdUtils.create());
        testUtils.upload("trigger/move-from/" + folderId + "/" + IdUtils.create());

        io.kestra.plugin.gcp.gcs.Trigger trigger = io.kestra.plugin.gcp.gcs.Trigger.builder()
            .id("watch-" + folderId)
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
        assertThat(objectCount(fromDir), is(0L));
        assertThat(objectCount(moveDir), is(2L));
    }

    @Test
    void move() throws Exception {
        String out = FriendlyId.createFriendlyId();
        String fileId = IdUtils.create();
        String sourcePrefix = "tasks/gcp/upload/trigger/" + out + "/";
        String movePrefix = "test/move/" + out + "/";
        cleanupPrefixes.add(sourcePrefix);
        cleanupPrefixes.add(movePrefix);

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/" + sourcePrefix))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveDirectory(Property.ofValue("gs://" + bucket + "/" + movePrefix))
            .build();

        Upload.Output upload = testUtils.upload("trigger/" + out + "/" + fileId);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> urls = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(urls.size(), is(1));

        assertThrows(IllegalArgumentException.class, () ->
        {
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
            .from(Property.ofValue("gs://" + bucket + "/" + movePrefix + fileId + ".yml"))
            .build();

        Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBucket(), is(bucket));
        assertThat(objectCount(sourcePrefix), is(0L));
        assertThat(objectCount(movePrefix), is(1L));
    }

    @Test
    void none() throws Exception {
        String base = "trigger/none/" + IdUtils.create() + "/";
        String file = base + FriendlyId.createFriendlyId();
        cleanupPrefixes.add("tasks/gcp/upload/" + base);

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + base))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .build();

        Upload.Output upload = testUtils.upload(file);

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
    void shouldFireWhenActionNotConfigured() throws Exception {
        // Regression test for Pylon #1888: when no `action` is set, evaluating a trigger
        // that detects files must not fail with "No value present" (NoSuchElementException).
        String base = "trigger/no-action/" + IdUtils.create() + "/";
        String file = base + FriendlyId.createFriendlyId();
        cleanupPrefixes.add("tasks/gcp/upload/" + base);

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + base))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .build();

        Upload.Output upload = testUtils.upload(file);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = Assertions.assertDoesNotThrow(
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> urls = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(urls.size(), is(1));

        Delete delete = Delete.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.ofValue(upload.getUri().toString()))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of()));
    }

    @Test
    void shouldExecuteOnCreate() throws Exception {
        String base = "trigger/on-create/" + IdUtils.create() + "/";
        String file = base + FriendlyId.createFriendlyId();
        cleanupPrefixes.add("tasks/gcp/upload/" + base);

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + base))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        var upload = testUtils.upload(file);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = Optional.ofNullable(
            Await.until(
                throwSupplier(() -> trigger.evaluate(context.getKey(), context.getValue()).orElse(null)),
                Duration.ofMillis(500),
                Duration.ofSeconds(20)
            )
        );

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
        String base = "trigger/on-update/" + IdUtils.create() + "/";
        String file = base + FriendlyId.createFriendlyId();
        cleanupPrefixes.add("tasks/gcp/upload/" + base);
        var upload = testUtils.upload(file);

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + base))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue());

        // we update the file to trigger the update event
        testUtils.update(file);
        Thread.sleep(3000);

        Optional<Execution> execution = Optional.ofNullable(
            Await.until(
                throwSupplier(() -> trigger.evaluate(context.getKey(), context.getValue()).orElse(null)),
                Duration.ofMillis(500),
                Duration.ofSeconds(20)
            )
        );

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
        String base = "trigger/on-create_or_update/" + IdUtils.create() + "/";
        String file = base + FriendlyId.createFriendlyId();
        cleanupPrefixes.add("tasks/gcp/upload/" + base);

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + base))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .interval(Duration.ofSeconds(10))
            .build();

        var upload = testUtils.upload(file);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> createExecution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(createExecution.isPresent(), is(true));

        // we update the file to trigger the update event
        testUtils.update(file);

        Optional<Execution> updateExecution = Optional.ofNullable(
            Await.until(
                throwSupplier(() -> trigger.evaluate(context.getKey(), context.getValue()).orElse(null)),
                Duration.ofMillis(500),
                Duration.ofSeconds(20)
            )
        );

        assertThat(updateExecution.isPresent(), is(true));

        Delete delete = Delete.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.ofValue(upload.getUri().toString()))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of()));
    }

    private long objectCount(String prefix) {
        return Iterables.size(storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll());
    }
}
