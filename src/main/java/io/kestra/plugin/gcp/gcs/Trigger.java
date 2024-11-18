package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.GcpInterface;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for files on Google Cloud Storage.",
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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Downloads.Output>, GcpInterface, ListInterface, ActionInterface{
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected String projectId;
    protected String serviceAccount;
    protected String impersonatedServiceAccount;

    @Builder.Default
    protected java.util.List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    private String from;

    private ActionInterface.Action action;

    private String moveDirectory;

    @Builder.Default
    private final List.ListingType listingType = ListInterface.ListingType.DIRECTORY;

    @PluginProperty(dynamic = true)
    private String regExp;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .from(this.from)
            .filter(ListInterface.Filter.FILES)
            .listingType(this.listingType)
            .regExp(this.regExp)
            .build();
        List.Output run = task.run(runContext);

        if (run.getBlobs().isEmpty()) {
            return Optional.empty();
        }

        Storage connection = task.connection(runContext);

        java.util.List<Blob> list = run
            .getBlobs()
            .stream()
            .map(throwFunction(blob -> {
                URI uri = runContext.storage().putFile(
                    Download.download(runContext, connection, BlobId.of(blob.getBucket(), blob.getName()))
                );

                return blob.withUri(uri);
            }))
            .collect(Collectors.toList());

        Downloads.performAction(
            run.getBlobs(),
            this.action,
            this.moveDirectory,
            runContext,
            this.projectId,
            this.serviceAccount,
            this.scopes
        );

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, Downloads.Output.builder().blobs(list).build());

        return Optional.of(execution);
    }
}
