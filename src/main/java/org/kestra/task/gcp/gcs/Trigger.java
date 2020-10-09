package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.flows.State;
import org.kestra.core.models.triggers.AbstractTrigger;
import org.kestra.core.models.triggers.PollingTriggerInterface;
import org.kestra.core.models.triggers.TriggerContext;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.IdUtils;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.RegEx;
import javax.validation.constraints.NotNull;

import static org.kestra.core.utils.Rethrow.throwConsumer;
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
        "    value: \"{{ jq trigger '.uri' true }}\"",
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
    private Action action;

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
        description = "If set to `true`, lists all versions of a blob. The default is `false`.",
        dynamic = true
    )
    private Boolean allVersions;

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
            .allVersions(this.allVersions)
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

        java.util.List<URI> list = run
            .getBlobs()
            .stream()
            .map(throwFunction(blob -> runContext.putTempFile(
                Download.download(connection, BlobId.of(blob.getBucket(), blob.getName())),
                executionId,
                this
            )))
            .collect(Collectors.toList());

        if (this.action == Action.DELETE) {
            run
                .getBlobs()
                .forEach(throwConsumer(blob -> {
                    Delete delete = Delete.builder()
                        .id(this.id)
                        .type(Delete.class.getName())
                        .uri(blob.getUri().toString())
                        .build();
                    delete.run(runContext);
                }));
        } else if (this.action == Action.MOVE) {
            run
                .getBlobs()
                .forEach(throwConsumer(blob -> {
                    Copy copy = Copy.builder()
                        .id(this.id)
                        .type(Copy.class.getName())
                        .from(blob.getUri().toString())
                        .to(StringUtils.stripEnd(runContext.render(this.moveDirectory) + "/", "/")
                            + "/" + FilenameUtils.getName(blob.getName())
                        )
                        .delete(true)
                        .build();
                    copy.run(runContext);
                }));
        }

        Execution execution = Execution.builder()
            .id(executionId)
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .variables(ImmutableMap.of(
                "trigger", ImmutableMap.of(
                     "uri", list
                )
            ))
            .build();

        return Optional.of(execution);
    }

    public enum Action {
        MOVE,
        DELETE
    }
}
