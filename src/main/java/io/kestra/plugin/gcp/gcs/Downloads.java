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
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.gcs.models.Blob;

import java.io.File;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

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
public class Downloads extends AbstractGcs implements RunnableTask<Downloads.Output>, ListInterface, ActionInterface {
    private String from;

    @Schema(
        title = "If set to `true`, lists all versions of a blob. The default is `false`."
    )
    @PluginProperty(dynamic = true)
    private Boolean allVersions;

    @Builder.Default
    private final List.ListingType listingType = ListInterface.ListingType.DIRECTORY;

    private String regExp;

    private ActionInterface.Action action;

    private String moveDirectory;

    static void performAction(
        java.util.List<io.kestra.plugin.gcp.gcs.models.Blob> blobList,
        ActionInterface.Action action,
        String moveDirectory,
        RunContext runContext,
        String projectId,
        String serviceAccount,
        java.util.List<String> scopes
    ) throws Exception {
        if (action == ActionInterface.Action.DELETE) {
            for (Blob blob : blobList) {
                Delete delete = Delete.builder()
                    .id("archive")
                    .type(Delete.class.getName())
                    .uri(blob.getUri().toString())
                    .serviceAccount(serviceAccount)
                    .projectId(projectId)
                    .scopes(scopes)
                    .build();
                delete.run(runContext);
            }
        } else if (action == ActionInterface.Action.MOVE) {
            for (Blob blob : blobList) {
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
            }
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
            .filter(Filter.FILES)
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
                File tempFile = Download.download(runContext, connection, source);

                return blob.withUri(runContext.storage().putFile(tempFile));
            }))
            .collect(Collectors.toList());

        Map<String, URI> outputFiles = list.stream()
            .filter(blob -> !blob.isDirectory())
            .map(blob -> new AbstractMap.SimpleEntry<>(blob.getName(), blob.getUri()))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        Downloads.performAction(
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
            .outputFiles(outputFiles)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket of the downloaded file"
        )
        private final java.util.List<Blob>  blobs;

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }
}
