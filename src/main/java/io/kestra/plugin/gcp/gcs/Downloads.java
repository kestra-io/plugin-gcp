package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.gcs.models.Blob;

import java.io.File;
import java.util.stream.Collectors;
import javax.annotation.RegEx;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Download a list of files and move it to an archive folders",
            code = {
                "from: gs://my-bucket/kestra/files/",
                "action: MOVE",
                "moveDirectory: gs://my-bucket/kestra/archive/",
            }
        )
    }
)
@Schema(
    title = "Download multiple files from a GCS bucket."
)
public class Downloads extends AbstractGcs implements RunnableTask<Downloads.Output> {
    @Schema(
        title = "The directory to list"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "If set to `true`, lists all versions of a blob. The default is `false`."
    )
    @PluginProperty(dynamic = true)
    private Boolean allVersions;

    @Schema(
        title = "The filter files or directory"
    )
    @Builder.Default
    private final List.Filter filter = List.Filter.BOTH;

    @Schema(
        title = "The listing type you want (like directory or recursive)"
    )
    @Builder.Default
    private final List.ListingType listingType = List.ListingType.DIRECTORY;

    @Schema(
        title = "A regexp to filter on full path"
    )
    @PluginProperty(dynamic = true)
    private String regExp;

    @Schema(
        title = "The action to do on find files",
        description = "Can be null, in this case no action is perform"
    )
    @PluginProperty(dynamic = true)
    private Downloads.Action action;

    @Schema(
        title = "The destination directory in case off `MOVE` "
    )
    @PluginProperty(dynamic = true)
    private String moveDirectory;

    static void archive(
        java.util.List<io.kestra.plugin.gcp.gcs.models.Blob> blobList,
        Action action,
        String moveDirectory,
        RunContext runContext,
        String projectId,
        String serviceAccount,
        java.util.List<String> scopes
    ) throws Exception {
        if (action == Action.DELETE) {
            blobList
                .forEach(throwConsumer(blob -> {
                    Delete delete = Delete.builder()
                        .id("archive")
                        .type(Delete.class.getName())
                        .uri(blob.getUri().toString())
                        .serviceAccount(serviceAccount)
                        .projectId(projectId)
                        .scopes(scopes)
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
                        .serviceAccount(serviceAccount)
                        .projectId(projectId)
                        .scopes(scopes)
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
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .from(this.from)
            .filter(this.filter)
            .listingType(this.listingType)
            .regExp(this.regExp)
            .allVersions(this.allVersions)
            .build();
        List.Output run = task.run(runContext);

        Storage connection = this.connection(runContext);

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
            runContext,
            this.projectId,
            this.serviceAccount,
            this.scopes
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket of the downloaded file"
        )
        @PluginProperty(additionalProperties = Blob.class)
        private final java.util.List<Blob>  blobs;
    }
}
