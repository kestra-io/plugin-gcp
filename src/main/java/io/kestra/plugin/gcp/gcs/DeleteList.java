package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.net.URI;
import java.util.NoSuchElementException;
import javax.validation.constraints.Min;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "from: \"gs://my_bucket/dir/\""
            }
        )
    }
)
@Schema(
    title = "Delete all file to a GCS bucket."
)
public class DeleteList extends AbstractList implements RunnableTask<DeleteList.Output>, ListInterface {
    @Schema(
        title = "raise an error if the file is not found"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final Boolean errorOnEmpty = false;

    @Min(2)
    @Schema(
        title = "Number of concurrent parrallels deletion"
    )
    @PluginProperty(dynamic = false)
    private Integer concurrent;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);
        Logger logger = runContext.logger();

        URI from = encode(runContext, this.from);
        String regExp = runContext.render(this.regExp);

        Flowable<com.google.cloud.storage.Blob> flowable = Flowable
            .create(emitter -> {
                this.iterator(connection, from)
                    .forEachRemaining(emitter::onNext);
                emitter.onComplete();
            }, BackpressureStrategy.BUFFER);

        Flowable<Long> result;

        if (this.concurrent != null) {
            result = flowable
                .parallel(this.concurrent)
                .runOn(Schedulers.io())
                .filter(blob -> this.filter(blob, regExp))
                .map(delete(logger, connection))
                .sequential();
        } else {
            result = flowable
                .filter(blob -> this.filter(blob, regExp))
                .map(delete(logger, connection));
        }

        Pair<Long, Long> finalResult = result
            .reduce(Pair.of(0L, 0L), (pair, size) -> Pair.of(pair.getLeft() + 1, pair.getRight() + size))
            .blockingGet();

        runContext.metric(Counter.of("count", finalResult.getLeft()));
        runContext.metric(Counter.of("size", finalResult.getRight()));

        if (errorOnEmpty && finalResult.getLeft() == 0) {
            throw new NoSuchElementException("Unable to find any files to delete on '" + from + "'");
        }

        logger.info("Deleted {} files for {} bytes", finalResult.getLeft(), finalResult.getValue());

        return Output
            .builder()
            .count(finalResult.getLeft())
            .size(finalResult.getRight())
            .build();
    }

    protected boolean filter(com.google.cloud.storage.Blob blob, String regExp) {
        return !blob.isDirectory() && super.filter(blob, regExp);
    }

    private static Function<Blob, Long> delete(Logger logger, Storage connection) {
        return o -> {
            logger.debug("Deleting '{}'", io.kestra.plugin.gcp.gcs.models.Blob.uri(o));
            if (connection.delete(BlobId.of(o.getBucket(), o.getName()))) {
                return o.getSize();
            } else {
                return 0L;
            }
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Builder.Default
        @Schema(
            title = "The count of blobs deleted"
        )
        private final long count = 0;

        @Builder.Default
        @Schema(
            title = "The size of all blobs deleted"
        )
        private final long size = 0;
    }
}
