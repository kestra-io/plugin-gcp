package org.kestra.task.gcp.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.task.gcp.gcs.models.Blob;
import org.slf4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.RegEx;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "List files in a bucket",
            code = {
                "from: \"gs://my_bucket/dir/\""
            }
        )
    }
)
@Schema(
    title = "List file on a GCS bucket."
)
public class List extends Task implements RunnableTask<List.Output> {
    @Schema(
        title = "The directory to list"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The GCP project id"
    )
    @PluginProperty(dynamic = true)
    private String projectId;

    @Schema(
        title = "If set to `true`, lists all versions of a blob. The default is `false`."
    )
    @PluginProperty(dynamic = true)
    private Boolean allVersions;

    @Schema(
        title = "The filter files or directory"
    )
    @Builder.Default
    private final Filter filter = Filter.BOTH;

    @Schema(
        title = "The listing type you want (like directory or recursive)"
    )
    @Builder.Default
    private final ListingType listingType = ListingType.DIRECTORY;

    @Schema(
        title = "A regexp to filter on full path"
    )
    @PluginProperty(dynamic = true)
    private String regExp;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));

        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));
        String regExp = runContext.render(this.regExp);

        Page<com.google.cloud.storage.Blob> list = connection.list(from.getAuthority(), options(from));

        java.util.List<Blob> blobs = StreamSupport
            .stream(list.iterateAll().spliterator(), false)
            .map(Blob::of)
            .filter(blob -> filter == Filter.DIRECTORY ? blob.isDirectory() :
                (filter != Filter.FILES || !blob.isDirectory())
            )
            .filter(blob -> regExp == null || blob.getUri().toString().matches(regExp))
            .collect(Collectors.toList());

        runContext.metric(Counter.of("size", blobs.size()));

        logger.debug("Found '{}' blobs from '{}'", blobs.size(), from);

        return Output
            .builder()
            .blobs(blobs)
            .build();
    }

    private Storage.BlobListOption[] options(URI from) {
        java.util.List<Storage.BlobListOption> options = new ArrayList<>();

        if (!from.getPath().equals("")) {
            options.add(Storage.BlobListOption.prefix(from.getPath().substring(1)));
        }

        if (this.allVersions != null) {
            options.add(Storage.BlobListOption.versions(this.allVersions));
        }

        if (this.listingType == ListingType.DIRECTORY) {
            options.add(Storage.BlobListOption.currentDirectory());
        }

        return Iterables.toArray(options, Storage.BlobListOption.class);
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of blobs"
        )
        private final java.util.List<Blob> blobs;
    }

    public enum Filter {
        FILES,
        DIRECTORY,
        BOTH
    }

    public enum ListingType {
        RECURSIVE,
        DIRECTORY,
    }
}
