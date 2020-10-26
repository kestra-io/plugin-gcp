package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.executions.ExecutionTrigger;
import org.kestra.core.models.flows.State;
import org.kestra.core.models.triggers.AbstractTrigger;
import org.kestra.core.models.triggers.PollingTriggerInterface;
import org.kestra.core.models.triggers.TriggerContext;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.IdUtils;
import org.kestra.task.gcp.gcs.models.Blob;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.RegEx;
import javax.validation.constraints.NotNull;

import static org.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Wait for files on Google cloud storage"
)
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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface {
    @InputProperty(
        description = "The interval between test of triggers"
    )
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @InputProperty(
        description = "The directory to list",
        dynamic = true
    )
    @NotNull
    private String from;

    @InputProperty(
        description = "The action to do on find files",
        dynamic = true
    )
    @NotNull
    private Downloads.Action action;

    @InputProperty(
        description = "The destination directory in case off `MOVE` ",
        dynamic = true
    )
    private String moveDirectory;

    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    private String projectId;

    @InputProperty(
        description = "The filter files or directory"
    )
    @Builder.Default
    private final List.Filter filter = List.Filter.BOTH;

    @InputProperty(
        description = "The listing type you want (like directory or recursive)"
    )
    @Builder.Default
    private final List.ListingType listingType = List.ListingType.DIRECTORY;

    @InputProperty(
        description = "A regexp to filter on full path"
    )
    @RegEx
    private String regExp;

    @Override
    public Optional<Execution> evaluate(RunContext runContext, TriggerContext context) throws Exception {
        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .from(this.from)
            .projectId(this.projectId)
            .filter(this.filter)
            .listingType(this.listingType)
            .regExp(this.regExp)
            .build();
        List.Output run = task.run(runContext);

        if (run.getBlobs().size() == 0) {
            return Optional.empty();
        }

        String executionId = IdUtils.create();

        Storage connection = new Connection().of(runContext.render(this.projectId));

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
            runContext
        );

        Execution execution = Execution.builder()
            .id(executionId)
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(ExecutionTrigger.builder()
                .id(this.id)
                .type(this.type)
                .variables(ImmutableMap.of(
                    "blobs", list
                ))
                .build()
            )
            .build();

        return Optional.of(execution);
    }
}
