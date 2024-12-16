package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.net.URI;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Concat files in a bucket",
            full = true,
            code = """
                id: gcp_gcs_compose
                namespace: company.team

                tasks:
                  - id: compose
                    type: io.kestra.plugin.gcp.gcs.Compose
                    list:
                      from: "gs://my_bucket/dir/"
                    to: "gs://my_bucket/destination/my-compose-file.txt"
                """
        )
    }
)
@Schema(
    title = "List file on a GCS bucket."
)
public class Compose extends AbstractGcs implements RunnableTask<Compose.Output> {
    @Schema(
        title = "The directory to list"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private List list;

    @Schema(
        title = "The destination path"
    )
    private Property<String> to;

    @Schema(
        title = "if `true`, don't failed if no result"
    )
    @PluginProperty
    @Builder.Default
    private Property<Boolean> allowEmpty = Property.of(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);
        Logger logger = runContext.logger();

        URI to = encode(runContext, runContext.render(this.to).as(String.class).orElse(null));

        // target
        BlobInfo destination = BlobInfo
            .newBuilder(BlobId.of(to.getScheme().equals("gs") ? to.getAuthority() : to.getScheme(), blobPath(to.getPath().substring(1))))
            .build();

        Storage.ComposeRequest.Builder builder = Storage.ComposeRequest.newBuilder()
            .setTarget(destination);

        io.kestra.plugin.gcp.gcs.List listActions = io.kestra.plugin.gcp.gcs.List.builder()
            .id(this.id)
            .type(io.kestra.plugin.gcp.gcs.List.class.getName())
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .from(this.list.getFrom())
            .filter(Property.of(ListInterface.Filter.FILES))
            .listingType(this.list.getListingType())
            .regExp(this.list.getRegExp())
            .build();

        io.kestra.plugin.gcp.gcs.List.Output run = listActions.run(runContext);

        if (run.getBlobs().size() == 0 && runContext.render(this.allowEmpty).as(Boolean.class).orElse(false).equals(false)) {
            throw new FileNotFoundException("No files founds");
        }

        run.getBlobs()
            .forEach(blob -> builder.addSource(blob.getUri().getPath().substring(1)));

        Storage.ComposeRequest composeRequest = builder.build();

        logger.debug("Compose from '{}' to '{}'", composeRequest.getSourceBlobs(), to);

        com.google.cloud.storage.Blob compose = connection.compose(composeRequest);

        runContext.metric(Counter.of("count", run.getBlobs().size()));
        runContext.metric(Counter.of("size", compose.getSize()));

        return Output
            .builder()
            .uri(new URI("gs://" + compose.getBucket() + "/" + encode(destination.getName())))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private URI uri;
    }

    @Builder
    @Getter
    public static class List implements ListInterface {
        @NotNull
        private Property<String> from;

        @Builder.Default
        private final Property<io.kestra.plugin.gcp.gcs.List.Filter> filter = Property.of(Filter.BOTH);

        @Builder.Default
        private final Property<io.kestra.plugin.gcp.gcs.List.ListingType> listingType = Property.of(ListingType.DIRECTORY);

        private Property<String> regExp;
    }
}
