package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load documents in Firestore using Kestra Internal Storage file"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "collection: \"my_collection\"",
                "from: \"{{ inputs.file }}\"",
            }
        )
    }
)
public class Load extends AbstractFirestore implements RunnableTask<Load.Output> {
    @Schema(
        title = "The source file."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "Use this key as document child path."
    )
    @PluginProperty(dynamic = true)
    private String keyPath;

    @Schema(
        title = "Remove child path key from the final document"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean removeChildPathKey = true;

    @Schema(
        title = "The size of chunk for every bulk request"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    @NotNull
    private Integer chunk = 1000;

    @SuppressWarnings("unchecked")
    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var from = new URI(runContext.render(this.from));
        var childPathKey = runContext.render(this.keyPath);

        try (
            var firestore = this.connection(runContext);
            var inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))
        ) {
            var collectionRef = this.collection(runContext, firestore);
            var requestCount = new AtomicLong();

            Flowable<WriteResult> flowable = Flowable
                .create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER)
                .buffer(this.chunk, this.chunk)
                .observeOn(Schedulers.io())
                .flatMap(list -> {
                    WriteBatch batch = firestore.batch();
                    requestCount.incrementAndGet();

                    list.forEach(o -> {
                        Map<String, Object> values = (Map<String, Object>) o;

                        var document = childPathKey != null ? collectionRef.document(values.get(childPathKey).toString()) : collectionRef.document();
                        if (childPathKey != null && removeChildPathKey) {
                            values.remove(childPathKey);
                        }

                        batch.set(document, values);
                    });

                    return Flowable.fromIterable(batch.commit().get());
                });

            // metrics & finalize
            Long rowsCount = flowable.count().blockingGet();
            runContext.metric(Counter.of("requests.count", requestCount.get(), "collection", collectionRef.getId()));
            runContext.metric(Counter.of("records", rowsCount, "collection", collectionRef.getId()));

            logger.info(
                "Successfully send {} requests for {} records",
                requestCount.get(),
                rowsCount
            );

            return Output.builder()
                .size(rowsCount)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The size of the processed rows"
        )
        private Long size;
    }
}
