package io.kestra.plugin.gcp.gcs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.GcpInterface;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Downloads.Output>, GcpInterface, ListInterface, ActionInterface {
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

    @Schema(
        title = "Trigger event type",
        description = """
            Defines when the trigger fires.
            - `CREATE`: only for newly discovered objects (Default).
            - `UPDATE`: only when an already-seen object's version changed.
            - `CREATE_OR_UPDATE`: fires on either event.
            """
    )
    @Builder.Default
    private Property<OnEvent> on = Property.ofValue(OnEvent.CREATE);

    @Schema(
        title = "State key",
        description = """
            JSON-type KV key for persisted state.
            Default: `<namespace>__<flowId>__<triggerId> (e.g., company.team__myflow__mytrigger)`"""
    )
    private Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = "TTL for persisted state (e.g., PT24H, P7D)."
    )
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
            runContext.workingDir()
            var rOn = runContext.render(on).as(OnEvent.class).orElse(OnEvent.CREATE);
            var rStateKey = runContext.render(stateKey).as(String.class).orElse(defaultStateKey(context));
            var rStateTtl = runContext.render(stateTtl).as(Duration.class);

            var stateContext = new StateContext(true, "gcs_trigger_state", id, rStateKey, rStateTtl);
            Map<String, StateEntry> state = readStatePruned(runContext, stateContext);


            java.util.List<Blob> toFire = blobs.stream()
                .flatMap(throwFunction(blob -> {
                    var uri = "gs://" + blob.getBucket() + "/" + blob.getName();
                    var meta = connection.get(BlobId.of(blob.getBucket(), blob.getName()));
                    var version = "metageneration:" + Objects.requireNonNullElse(meta.getMetageneration(), 0L);

                    Instant modifiedAt = Optional.ofNullable(meta.getUpdateTimeOffsetDateTime())
                        .map(OffsetDateTime::toInstant)
                        .orElse(Instant.now());

                    StateEntry prev = state.get(uri);
                    boolean fire = shouldFire(prev, version, rOn);

                    state.put(uri, StateEntry.builder()
                        .uri(uri)
                        .version(version)
                        .modifiedAt(modifiedAt)
                        .lastSeenAt(Instant.now())
                        .build());

                    if (fire) {
                        runContext.storage().putFile(Download.download(runContext, connection, BlobId.of(blob.getBucket(), blob.getName())));
                        return Stream.of(blob.withUri(URI.create(uri)));
                    }
                    return Stream.empty();
                }))
                .toList();

            writeState(runContext, stateContext, state);
            if (toFire.isEmpty()) {
                return Optional.empty();
            }

            Downloads.performAction(toFire, runContext.render(action).as(Action.class).orElseThrow(), moveDirectory, runContext, projectId, serviceAccount, scopes);

            var output = Downloads.Output.builder().blobs(toFire).build();
            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
        }
    }

    private Map<String, StateEntry> readStatePruned(RunContext runContext, StateContext stateContext) {
        try (var in = runContext.stateStore().getState(stateContext.flowScoped(), stateContext.stateName(), stateContext.stateSubName(), stateContext.taskRunValue())) {
            var entries = MAPPER.readValue(in, new TypeReference<java.util.List<StateEntry>>() {});

            var cutoff = stateContext.ttl()
                .map(d -> Instant.now().minus(d))
                .orElse(Instant.MIN);

            return entries.stream()
                .filter(e -> e.getLastSeenAt() == null || !e.getLastSeenAt().isBefore(cutoff))
                .collect(Collectors.toMap(StateEntry::getUri, Function.identity(), (a, b) -> a, HashMap::new));

        } catch (Exception e) {
            runContext.logger().warn("Unable to read state: {}", e.toString());
            return new HashMap<>();
        }
    }

    private void writeState(RunContext runContext, StateContext stateCtx, Map<String, StateEntry> state) {
        try {
            var bytes = MAPPER.writeValueAsBytes(new ArrayList<>(state.values()));
            runContext.stateStore().putState(stateCtx.flowScoped(), stateCtx.stateName(), stateCtx.stateSubName(), stateCtx.taskRunValue(), bytes);
        } catch (Exception e) {
            runContext.logger().error("Unable to write state: {}", e.toString());
        }
    }

    private String defaultStateKey(TriggerContext triggerContext) {
        return triggerContext.getNamespace() + "_" + triggerContext.getFlowId() + "_" + id;
    }

    private boolean shouldFire(StateEntry prev, String version, OnEvent rOn) {
        if (prev == null) {
            return rOn == OnEvent.CREATE || rOn == OnEvent.CREATE_OR_UPDATE;
        }

        if (!Objects.equals(prev.getVersion(), version)) {
            return rOn == OnEvent.UPDATE || rOn == OnEvent.CREATE_OR_UPDATE;
        }
        return false;
    }

    public enum OnEvent {
        CREATE,
        UPDATE,
        CREATE_OR_UPDATE
    }

    record StateContext(boolean flowScoped, String stateName, String stateSubName, String taskRunValue, Optional<Duration> ttl) { }

    @Getter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StateEntry {
        private String uri;
        private String version;
        private Instant modifiedAt;
        private Instant lastSeenAt;
    }
}
