package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.Storage;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.gcs.models.Blob;
import org.slf4j.Logger;

import java.net.URI;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "List files in a GCS bucket",
            full = true,
            code = """
                id: gcp_gcs_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.gcp.gcs.List
                    from: "gs://my_bucket/dir/"
                """
        )
    },
    metrics = {
        @Metric(
            name = "size",
            type = Counter.TYPE,
            unit = "files",
            description = "Number of blobs listed."
        )
    }
)
@Schema(
    title = "List files from a GCS bucket."
)
public class List extends AbstractList implements RunnableTask<List.Output>, ListInterface {
    @Schema(
        title = "The filter for files or a directory"
    )
    @Builder.Default
    protected final Property<Filter> filter = Property.ofValue(Filter.BOTH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);
        Logger logger = runContext.logger();

        URI from = encode(runContext, runContext.render(this.from).as(String.class).orElse(null));
        String regExp = runContext.render(this.regExp).as(String.class).orElse(null);

        java.util.List<Blob> blobs = StreamSupport
            .stream(this.iterator(connection, from, runContext), false)
            .filter(blob -> {
                try {
                    return this.filter(blob, regExp, runContext.render(this.filter).as(Filter.class).orElseThrow());
                } catch (IllegalVariableEvaluationException e) {
                    throw new RuntimeException(e);
                }
            })
            .map(Blob::of)
            .collect(Collectors.toList());

        runContext.metric(Counter.of("size", blobs.size()));

        logger.debug("Found '{}' blobs from '{}'", blobs.size(), from);

        return Output
            .builder()
            .blobs(blobs)
            .build();
    }

    protected boolean filter(com.google.cloud.storage.Blob blob, String regExp, Filter filter) {
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
