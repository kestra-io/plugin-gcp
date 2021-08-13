package io.kestra.plugin.gcp.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.gcs.models.Blob;
import org.slf4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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
public class List extends AbstractList implements RunnableTask<List.Output>, ListInterface {
    @Schema(
        title = "The filter files or directory"
    )
    @Builder.Default
    protected final Filter filter = Filter.BOTH;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);
        Logger logger = runContext.logger();

        URI from = encode(runContext, this.from);
        String regExp = runContext.render(this.regExp);

        java.util.List<Blob> blobs = StreamSupport
            .stream(this.iterator(connection, from), false)
            .filter(blob -> this.filter(blob, regExp))
            .map(Blob::of)
            .collect(Collectors.toList());

        runContext.metric(Counter.of("size", blobs.size()));

        logger.debug("Found '{}' blobs from '{}'", blobs.size(), from);

        return Output
            .builder()
            .blobs(blobs)
            .build();
    }

    protected boolean filter(com.google.cloud.storage.Blob blob, String regExp) {
        boolean b = filter == Filter.DIRECTORY ? blob.isDirectory() :
            (filter != Filter.FILES || !blob.isDirectory());

        if (!b) {
            return false;
        }

        return super.filter(blob, regExp);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of blobs"
        )
        private final java.util.List<Blob> blobs;
    }
}
