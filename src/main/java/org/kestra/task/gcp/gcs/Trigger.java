package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.executions.ExecutionTrigger;
import org.kestra.core.models.flows.State;
import org.kestra.core.models.triggers.AbstractTrigger;
import org.kestra.core.models.triggers.PollingTriggerInterface;
import org.kestra.core.models.triggers.TriggerContext;
import org.kestra.core.models.triggers.TriggerOutput;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.IdUtils;
import org.kestra.task.gcp.GcpInterface;
import org.kestra.task.gcp.gcs.models.Blob;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static org.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for files on Google cloud storage"
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of file on a GCS bucket and iterate through the files",
            full = true,
            code = {
                "id: gcs-listen",
                "namespace: org.kestra.tests",
                "",
                "tasks:",
                "  - id: each",
                "    type: org.kestra.core.tasks.flows.EachSequential",
                "    tasks:",
                "      - id: return",
                "        type: org.kestra.core.tasks.debugs.Return",
                "        format: \"{{taskrun.value}}\"",
                "    value: \"{{ jq trigger '.blobs[].uri' true }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: org.kestra.task.gcp.gcs.Trigger",
                "    interval: \"PT5M\"",
                "    from: gs://my-bucket/kestra/listen/",
                "    action: MOVE",
                "    moveDirectory: gs://my-bucket/kestra/archive/",
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Downloads.Output>, GcpInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected String projectId;
    protected String serviceAccount;

    @Builder.Default
    protected java.util.List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    @Schema(
        title = "The directory to list"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The action to do on find files"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Downloads.Action action;

    @Schema(
        title = "The destination directory in case off `MOVE` "
    )
    @PluginProperty(dynamic = true)
    private String moveDirectory;

    @Schema(
        title = "The filter files or directory"
    )
    @Builder.Default
    private final List.Filter filter = List.Filter.BOTH;

    @Schema(
        title = "The listing type you want (like directory or recursive)",
        description = "if DIRECTORY, will only list objects in the specified directory\n" +
            "if RECURSIVE, will list objects in the specified directory recursively\n" +
            "Default value is DIRECTORY\n" +
            "When using RECURSIVE value, be carefull to move your files to a location not in the `from` scope"
    )
    @Builder.Default
    private final List.ListingType listingType = List.ListingType.DIRECTORY;

    @Schema(
        title = "A regexp to filter on full path",
        description = "ex:\n"+
            "`regExp: .*` to match all files\n"+
            "`regExp: .*2020-01-0.\\\\.csv` to match files between 01 and 09 of january ending with `.csv`"
    )
    @PluginProperty(dynamic = true)
    private String regExp;

    @Override
    public Optional<Execution> evaluate(RunContext runContext, TriggerContext context) throws Exception {
        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .from(this.from)
            .filter(this.filter)
            .listingType(this.listingType)
            .regExp(this.regExp)
            .build();
        List.Output run = task.run(runContext);

        if (run.getBlobs().size() == 0) {
            return Optional.empty();
        }

        String executionId = IdUtils.create();

        Storage connection = task.connection(runContext);

        java.util.List<Blob> list = run
            .getBlobs()
            .stream()
            .map(throwFunction(blob -> {
                URI uri = runContext.putTempFile(
                    Download.download(connection, BlobId.of(blob.getBucket(), blob.getName())),
                    executionId,
                    this
                );

                return blob.withUri(uri);
            }))
            .collect(Collectors.toList());

        Downloads.archive(
            run.getBlobs(),
            this.action,
            this.moveDirectory,
            runContext,
            this.projectId,
            this.serviceAccount,
            this.scopes
        );

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            Downloads.Output.builder().blobs(list).build()
        );

        Execution execution = Execution.builder()
            .id(executionId)
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }
}
