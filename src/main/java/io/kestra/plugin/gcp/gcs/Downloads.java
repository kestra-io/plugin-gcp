package io.kestra.plugin.gcp.gcs;

import java.io.File;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.gcs.models.Blob;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwFunction;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Download a list of files and move it to an archive folders",
            full = true,
            code = """
                id: gcp_gcs_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.gcp.gcs.Downloads
                    from: gs://my-bucket/kestra/files/
                    action: MOVE
                    moveDirectory: gs://my-bucket/kestra/archive/
                """
        ),
        @Example(
            title = "Download a list of files with end-to-end checksum validation",
            full = true,
            code = """
                id: gcp_gcs_downloads_validated
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.gcp.gcs.Downloads
                    from: gs://my-bucket/kestra/files/
                    action: DELETE
                    validateChecksum: true
                """
        )
    },
    metrics = {
        @Metric(
            name = "checksum.validated",
            type = Counter.TYPE,
            description = "Number of files whose checksum was successfully validated after download."
        )
    }
)
@Schema(
    title = "Download multiple GCS objects",
    description = "Lists objects under a prefix (with optional regex and versioning) then downloads them to Kestra storage. Can MOVE (copy then delete) or DELETE source objects after download."
)
public class Downloads extends AbstractGcs implements RunnableTask<Downloads.Output>, ListInterface, ActionInterface {
    @Schema(
        title = "Source prefix",
        description = "gs:// bucket path to list and download from"
    )
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(
        title = "If set to `true`, lists all versions of a blob. The default is `false`"
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> allVersions;

    @Builder.Default
    private final Property<List.ListingType> listingType = Property.ofValue(ListingType.DIRECTORY);

    @Schema(
        title = "Regex filter",
        description = "Optional regex applied to object names"
    )
    @PluginProperty(group = "processing")
    private Property<String> regExp;

    @Schema(
        title = "Post-download action",
        description = "NONE (default), DELETE, or MOVE (copy to moveDirectory then delete source)"
    )
    @Builder.Default
    private final Property<ActionInterface.Action> action = Property.ofValue(ActionInterface.Action.NONE);

    @Schema(
        title = "Move destination",
        description = "Required when action is MOVE; gs:// prefix for moved objects"
    )
    @PluginProperty(group = "destination")
    private Property<String> moveDirectory;

    @Schema(
        title = "Max files",
        description = "Maximum number of files to list and download"
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Schema(
        title = "Validate each downloaded file against the GCS checksum",
        description = "When `true`, every downloaded file is streamed through a CRC32C hasher (or MD5 if the object has no CRC32C) and compared to the checksum reported by Google Cloud Storage. If any file mismatches, the task fails. Used for end-to-end integrity, not for security/authentication."
    )
    @PluginProperty(group = "reliability")
    @Builder.Default
    private Property<Boolean> validateChecksum = Property.ofValue(false);

    static void performAction(
        java.util.List<io.kestra.plugin.gcp.gcs.models.Blob> blobList,
        ActionInterface.Action action,
        Property<String> moveDirectory,
        RunContext runContext,
        Property<String> projectId,
        Property<String> serviceAccount,
        Property<java.util.List<String>> scopes) throws Exception {
        if (action == ActionInterface.Action.DELETE) {
            for (Blob blob : blobList) {
                Delete delete = Delete.builder()
                    .id("archive")
                    .type(Delete.class.getName())
                    .uri(Property.ofValue(blob.getUri().toString()))
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
                    .from(Property.ofValue(blob.getUri().toString()))
                    .to(
                        Property.ofValue(
                            StringUtils.stripEnd(runContext.render(moveDirectory).as(String.class).orElseThrow() + "/", "/")
                                + "/" + FilenameUtils.getName(blob.getName())
                        )
                    )
                    .delete(Property.ofValue(true))
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
            .filter(Property.ofValue(Filter.FILES))
            .listingType(this.listingType)
            .regExp(this.regExp)
            .allVersions(this.allVersions)
            .maxFiles(this.maxFiles)
            .build();
        List.Output run = task.run(runContext);

        Storage connection = this.connection(runContext);

        boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);

        java.util.List<Blob> list = run
            .getBlobs()
            .stream()
            .map(throwFunction(blob ->
            {
                BlobId source = BlobId.of(
                    blob.getBucket(),
                    blob.getName()
                );
                File tempFile = Download.download(runContext, connection, source, rValidateChecksum);

                return blob.withUri(runContext.storage().putFile(tempFile));
            }))
            .collect(Collectors.toList());

        Map<String, URI> outputFiles = list.stream()
            .filter(blob -> !blob.isDirectory())
            .map(blob -> new AbstractMap.SimpleEntry<>(blob.getName(), blob.getUri()))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        Downloads.performAction(
            run.getBlobs(),
            runContext.render(this.action).as(Action.class).orElseThrow(),
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
            title = "Downloaded objects"
        )
        private final java.util.List<Blob> blobs;

        @Schema(
            title = "Downloaded file map",
            description = "Map of object names to Kestra storage URIs"
        )
        private final Map<String, URI> outputFiles;
    }
}
