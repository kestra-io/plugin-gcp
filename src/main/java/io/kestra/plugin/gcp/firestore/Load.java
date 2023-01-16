package io.kestra.plugin.gcp.firestore;

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

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
            title = "The source file"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
            title = "Use this key as document child path."
    )
    @PluginProperty(dynamic = false)
    private String childPathKey;

    @Schema(
            title = "Remove child path key from the final document"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean removeChildPathKey = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var from = new URI(runContext.render(this.from));

        try (
                var firestore = this.connection(runContext);
                var inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))
        ) {
            var collectionRef = this.collection(runContext, firestore);
            var count = new AtomicLong();

            Flowable<WriteResult> flowable = Flowable
                    .create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER)
                    .observeOn(Schedulers.io())
                    .map(o -> {
                        System.out.println(o);
                        Map<String, Object> values = (Map<String, Object>) o;
                        count.incrementAndGet();
                        var document = childPathKey != null ? collectionRef.document(values.get(childPathKey).toString()) : collectionRef.document();
                        if(childPathKey != null && removeChildPathKey) {
                            values.remove(childPathKey);
                        }
                        return document.set(values).get();
                    });

            // metrics & finalize
            Long requestCount = flowable.count().blockingGet();
            runContext.metric(Counter.of(
                    "requests.count", requestCount,
                    "collection", this.collection
            ));
            runContext.metric(Counter.of(
                    "records", count.get(),
                    "collection", this.collection
            ));

            logger.info(
                    "Successfully send {} requests for {} records",
                    requestCount,
                    count.get()
            );

            return Output.builder()
                    .size(count.get())
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
