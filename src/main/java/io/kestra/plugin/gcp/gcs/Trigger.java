package io.kestra.plugin.gcp.gcs;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.kestra.plugin.gcp.shared.GcpInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on new or updated GCS objects",
    description = "Polls GCS every `interval` (default 60s) under `from` with optional regex. Detects CREATE or UPDATE (configurable), downloads matching files to internal storage, and can MOVE or DELETE them to avoid retriggering."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of files on a GCS bucket, and iterate through the files",
            full = true,
            code = """
                id: gcs_listen
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.blobs | jq('.[].uri') }}"
                    tasks:
                      - id: log_file
                        type: io.kestra.plugin.core.log.Log
                        message: "{{ taskrun.value }}"

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
            title = "Wait for a list of files on a GCS bucket and iterate through the files. Delete files manually after processing to prevent infinite triggering",
            full = true,
            code = """
                id: gcs_listen
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.blobs | jq('.[].uri') }}"
                    tasks:
                      - id: log_file
                        type: io.kestra.plugin.core.log.Log
                        message: "{{ taskrun.value }}"
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

    @Schema(
        title = "Source prefix",
        description = "gs:// path to poll for new or updated objects"
    )
    @PluginProperty(group = "source")
    private Property<String> from;

    @Schema(
        title = "Post-detection action",
        description = "NONE (default), MOVE (requires moveDirectory), or DELETE on source objects after download"
    )
    @Builder.Default
    private final Property<ActionInterface.Action> action = Property.ofValue(ActionInterface.Action.NONE);

    @Schema(
        title = "Move destination",
        description = """
            Target gs:// prefix when action is MOVE.
            Relative directory paths under `from` are preserved in the destination. When `from` ends with `/`, the full prefix is stripped; when it does not end with `/`, the parent folder of `from` is stripped so sibling prefixes remain differentiated."""
    )
    @PluginProperty(group = "advanced")
    private Property<String> moveDirectory;

    @Builder.Default
    private final Property<List.ListingType> listingType = Property.ofValue(ListingType.DIRECTORY);

    private Property<String> regExp;

    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Schema(
        title = "State key",
        description = "Override key used to persist trigger state; defaults to namespace/flow/id"
    )
    @PluginProperty(group = "connection")
    private Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = "Optional TTL for trigger state entries"
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Schema(
        title = "Max files",
        description = """
            The maximum number of files to process per evaluation. The remaining new or updated files are processed on later evaluations.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> maxFiles = Property.ofValue(25);

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

        try (Storage connection = listTask.connection(runContext)) {
            var rFrom = AbstractGcs.encode(runContext, runContext.render(from).as(String.class).orElse(null));
            var rRegExp = runContext.render(regExp).as(String.class).orElse(null);
            var rMaxFiles = runContext.render(maxFiles).as(Integer.class).orElse(25);

            // No limit on the listing: maxFiles applies to the files that fire, after comparing them with the state.
            var blobs = StreamSupport.stream(listTask.iterator(connection, rFrom, runContext), false)
                .filter(blob -> listTask.filter(blob, rRegExp, Filter.FILES))
                .toList();
            if (blobs.isEmpty()) {
                return Optional.empty();
            }

            var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
            var rStateKey = runContext.render(stateKey).as(String.class).orElse(defaultKey(context.getNamespace(), context.getFlowId(), id));
            var rStateTtl = runContext.render(stateTtl).as(Duration.class);

            Map<String, StatefulTriggerService.Entry> state = readState(runContext, rStateKey, rStateTtl);

            var actionBlobs = new ArrayList<Blob>();
            var toFireCandidates = new ArrayList<Candidate>();

            var skipped = 0;

            for (var blob : blobs) {
                var uri = "gs://" + blob.getBucket() + "/" + blob.getName();
                var version = String
                    .format("generation:%d_metageneration:%d", Objects.requireNonNullElse(blob.getGeneration(), 0L), Objects.requireNonNullElse(blob.getMetageneration(), 0L));

                Instant modifiedAt = Optional.ofNullable(blob.getUpdateTimeOffsetDateTime())
                    .map(OffsetDateTime::toInstant)
                    .orElse(Instant.now());

                var candidate = StatefulTriggerService.Entry.candidate(uri, version, modifiedAt);
                var previous = state.get(uri);
                var update = computeAndUpdateState(state, candidate, rOn);

                if (update.fire()) {
                    if (toFireCandidates.size() >= rMaxFiles) {
                        // Over the limit: restore the previous state so the file fires on a later evaluation.
                        if (previous == null) {
                            state.remove(uri);
                        } else {
                            state.put(uri, previous);
                        }
                        skipped++;
                        continue;
                    }

                    var changeType = update.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;
                    var output = Blob.of(blob);
                    actionBlobs.add(output);
                    toFireCandidates.add(new Candidate(output, changeType));
                }
            }

            if (skipped > 0) {
                runContext.logger().warn(
                    "{} files exceeded the maxFiles limit ({}); they will be processed on a later evaluation.", skipped, rMaxFiles
                );
            }

            if (toFireCandidates.isEmpty()) {
                writeState(runContext, rStateKey, state, rStateTtl);
                return Optional.empty();
            }

            var rAction = runContext.render(action).as(Action.class).orElse(Action.NONE);
            Downloads.destinations(actionBlobs, rAction, moveDirectory, from, runContext);

            java.util.List<TriggeredBlob> toFire = new ArrayList<>();
            for (var candidate : toFireCandidates) {
                var blob = candidate.blob();
                File downloaded = Download.download(runContext, connection, BlobId.of(blob.getBucket(), blob.getName()), false);
                URI kestraUri = runContext.storage().putFile(downloaded);

                toFire.add(TriggeredBlob.builder()
                    .blob(blob.withUri(kestraUri))
                    .changeType(candidate.changeType())
                    .build());
            }

            Downloads.performAction(actionBlobs, rAction, moveDirectory, from, runContext, projectId, serviceAccount, scopes);

            writeState(runContext, rStateKey, state, rStateTtl);

            var output = Output.builder().blobs(toFire).build();
            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
        }
    }

    private record Candidate(Blob blob, ChangeType changeType) {}

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
            title = "List of blobs that triggered the flow, each with its change type"
        )
        private final java.util.List<TriggeredBlob> blobs;
    }
}
