package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.task.gcp.gcs.models.Blob;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
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
@Example(
    title = "Download a list of files and move it to an archive folders",
    code = {
        "from: gs://my-bucket/kestra/files/",
        "action: MOVE",
        "moveDirectory: gs://my-bucket/kestra/archive/",
    }
)
@Documentation(
    description = "Download multiple files from a GCS bucket."
)
public class Downloads extends Task implements RunnableTask<Downloads.Output> {
    @InputProperty(
        description = "The directory to list",
        dynamic = true
    )
    @NotNull
    private String from;

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

    @InputProperty(
        description = "The action to do on find files",
        body = "Can be null, in this case no action is perform",
        dynamic = true
    )
    private Downloads.Action action;

    @InputProperty(
        description = "The destination directory in case off `MOVE` ",
        dynamic = true
    )
    private String moveDirectory;

    static void archive(
        java.util.List<org.kestra.task.gcp.gcs.models.Blob> blobList,
        Action action,
        String moveDirectory,
        RunContext runContext
    ) throws Exception {
        if (action == Action.DELETE) {
            blobList
                .forEach(throwConsumer(blob -> {
                    Delete delete = Delete.builder()
                        .id("archive")
                        .type(Delete.class.getName())
                        .uri(blob.getUri().toString())
                        .build();
                    delete.run(runContext);
                }));
        } else if (action == Action.MOVE) {
            blobList
                .forEach(throwConsumer(blob -> {
                    Copy copy = Copy.builder()
                        .id("archive")
                        .type(Copy.class.getName())
                        .from(blob.getUri().toString())
                        .to(StringUtils.stripEnd(runContext.render(moveDirectory) + "/", "/")
                            + "/" + FilenameUtils.getName(blob.getName())
                        )
                        .delete(true)
                        .build();
                    copy.run(runContext);
                }));
        }
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .from(this.from)
            .projectId(this.projectId)
            .filter(this.filter)
            .listingType(this.listingType)
            .regExp(this.regExp)
            .allVersions(this.allVersions)
            .build();
        List.Output run = task.run(runContext);

        Storage connection = new Connection().of(runContext.render(this.projectId));

        java.util.List<Blob> list = run
            .getBlobs()
            .stream()
            .map(throwFunction(blob -> {
                BlobId source = BlobId.of(
                    blob.getBucket(),
                    blob.getName()
                );
                File tempFile = Download.download(connection, source);

                return blob.withUri(runContext.putTempFile(tempFile));
            }))
            .collect(Collectors.toList());

        Downloads.archive(
            run.getBlobs(),
            this.action,
            this.moveDirectory,
            runContext
        );

        return Output
            .builder()
            .blobs(list)
            .build();
    }

    public enum Action {
        MOVE,
        DELETE
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The bucket of the downloaded file"
        )
        private final java.util.List<Blob>  blobs;
    }
}
