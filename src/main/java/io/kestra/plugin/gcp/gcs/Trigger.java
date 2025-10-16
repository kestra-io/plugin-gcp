    package io.kestra.plugin.gcp.gcs;

    import com.fasterxml.jackson.annotation.JsonUnwrapped;
    import com.fasterxml.jackson.databind.ObjectMapper;
    import com.google.cloud.storage.BlobId;
    import com.google.cloud.storage.Storage;
    import io.kestra.core.models.annotations.Example;
    import io.kestra.core.models.annotations.Plugin;
    import io.kestra.core.models.conditions.ConditionContext;
    import io.kestra.core.models.executions.Execution;
    import io.kestra.core.models.property.Property;
    import io.kestra.core.models.triggers.*;
    import io.kestra.core.serializers.JacksonMapper;
    import io.kestra.plugin.gcp.GcpInterface;
    import io.kestra.plugin.gcp.gcs.models.Blob;
    import io.swagger.v3.oas.annotations.media.Schema;
    import lombok.*;
    import lombok.experimental.SuperBuilder;

    import java.io.File;
    import java.net.URI;
    import java.time.Duration;
    import java.time.Instant;
    import java.time.OffsetDateTime;
    import java.util.*;
    import java.util.stream.Stream;

    import static io.kestra.core.models.triggers.StatefulTriggerService.*;
    import static io.kestra.core.utils.Rethrow.throwFunction;

    @SuperBuilder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @Schema(
        title = "Trigger a flow on a new file arrival in a Google Cloud Storage bucket.",
        description = "This trigger will poll every `interval` a GCS bucket. " +
            "You can search for all files in a bucket or directory in `from` or you can filter the files with a `regExp`." +
            "The detection is atomic, internally we do a list and interact only with files listed.\n" +
            "Once a file is detected, we download the file on internal storage and processed with declared `action` " +
            "in order to move or delete the files from the bucket (to avoid double detection on new poll)."
    )
    @Plugin(
        examples = {
            @Example(
                title = "Wait for a list of files on a GCS bucket, and iterate through the files.",
                full = true,
                code = """
                    id: gcs_listen
                    namespace: company.team

                    tasks:
                      - id: each
                        type: io.kestra.plugin.core.flow.ForEach
                        values: "{{ trigger.blobs | jq('.[].uri') }}"
                        tasks:
                          - id: return
                            type: io.kestra.plugin.core.debug.Return
                            format: "{{ taskrun.value }}"

                    triggers:
                      - id: watch
                        type: io.kestra.plugin.gcp.gcs.Trigger
                        interval: "PT5M"
                        from: gs://my-bucket/kestra/listen/
                        action: MOVE
                        moveDirectory: gs://my-bucket/kestra/archive/
                    """
            ),
            @Example(
                title = "Wait for a list of files on a GCS bucket and iterate through the files. Delete files manually after processing to prevent infinite triggering.",
                full = true,
                code = """
                    id: gcs_listen
                    namespace: company.team

                    tasks:
                      - id: each
                        type: io.kestra.plugin.core.flow.EachSequential
                        values: "{{ trigger.blobs | jq('.[].uri') }}"
                        tasks:
                          - id: return
                            type: io.kestra.plugin.core.debug.Return
                            format: "{{ taskrun.value }}"
                          - id: delete
                            type: io.kestra.plugin.gcp.gcs.Delete
                            uri: "{{ taskrun.value }}"

                    triggers:
                      - id: watch
                        type: io.kestra.plugin.gcp.gcs.Trigger
                        interval: "PT5M"
                        from: gs://my-bucket/kestra/listen/
                        action: NONE
                    """
            )
        }
    )
    public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, GcpInterface, ListInterface, ActionInterface, StatefulTriggerInterface {
        private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

        @Builder.Default
        private final Duration interval = Duration.ofSeconds(60);

        protected Property<String> projectId;
        protected Property<String> serviceAccount;
        protected Property<String> impersonatedServiceAccount;

        @Builder.Default
        protected Property<java.util.List<String>> scopes = Property.ofValue(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        private Property<String> from;

        private Property<ActionInterface.Action> action;

        private Property<String> moveDirectory;

        @Builder.Default
        private final Property<List.ListingType> listingType = Property.ofValue(ListingType.DIRECTORY);

        private Property<String> regExp;

        @Builder.Default
        private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

        private Property<String> stateKey;

        private Property<Duration> stateTtl;

        @Override
        public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
            var runContext = conditionContext.getRunContext();

            var listTask = List.builder()
                .id(id)
                .type(List.class.getName())
                .projectId(projectId)
                .serviceAccount(serviceAccount)
                .scopes(scopes)
                .from(from)
                .filter(Property.ofValue(Filter.FILES))
                .listingType(listingType)
                .regExp(regExp)
                .build();

            var blobs = listTask.run(runContext).getBlobs();
            if (blobs.isEmpty()) {
                return Optional.empty();
            }

            try (Storage connection = listTask.connection(runContext)) {
                var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
                var rStateKey = runContext.render(stateKey).as(String.class).orElse(defaultKey(context.getNamespace(), context.getFlowId(), id));
                var rStateTtl = runContext.render(stateTtl).as(Duration.class);

                Map<String, StatefulTriggerService.Entry> state = readState(runContext, rStateKey, rStateTtl);

                java.util.List<Blob> actionBlobs = new ArrayList<>();

                java.util.List<TriggeredBlob> toFire = blobs.stream()
                    .flatMap(throwFunction(blob -> {
                        var uri = "gs://" + blob.getBucket() + "/" + blob.getName();
                        var meta = connection.get(BlobId.of(blob.getBucket(), blob.getName()));
                        var version = String.format("generation:%d_metageneration:%d", Objects.requireNonNullElse(meta.getGeneration(), 0L), Objects.requireNonNullElse(meta.getMetageneration(), 0L));

                        Instant modifiedAt = Optional.ofNullable(meta.getUpdateTimeOffsetDateTime())
                            .map(OffsetDateTime::toInstant)
                            .orElse(Instant.now());

                        var candidate = StatefulTriggerService.Entry.candidate(uri, version, modifiedAt);
                        var update = computeAndUpdateState(state, candidate, rOn);

                        if (update.fire()) {
                            var changeType = update.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;
                            actionBlobs.add(blob);

                            File downloaded = Download.download(runContext, connection, BlobId.of(blob.getBucket(), blob.getName()));
                            URI kestraUri = runContext.storage().putFile(downloaded);

                            return Stream.of(TriggeredBlob.builder()
                                .blob(blob.withUri(kestraUri))
                                .changeType(changeType)
                                .build());
                        }
                        return Stream.empty();
                    }))
                    .toList();

                writeState(runContext, rStateKey, state, rStateTtl);

                if (toFire.isEmpty()) {
                    return Optional.empty();
                }

                Downloads.performAction(actionBlobs, runContext.render(action).as(Action.class).orElseThrow(), moveDirectory, runContext, projectId, serviceAccount, scopes);

                var output = Output.builder().blobs(toFire).build();
                return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
            }
        }

        public enum ChangeType {
            CREATE,
            UPDATE
        }

        @Getter
        @AllArgsConstructor
        @Builder
        public static class TriggeredBlob {
            @JsonUnwrapped
            private final Blob blob;
            private final Trigger.ChangeType changeType;

            public Blob toBlob() {
                return blob;
            }
        }

        @Builder
        @Getter
        public static class Output implements io.kestra.core.models.tasks.Output {
            @Schema(
                title = "List of blobs that triggered the flow, each with its change type."
            )
            private final java.util.List<TriggeredBlob> blobs;
        }
    }
