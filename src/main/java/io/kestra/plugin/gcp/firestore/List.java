package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.DocumentReference;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "List all documents of a collection.",
        description = "List all documents of a collection."
)
@Plugin(
        examples = {
                @Example(
                        title = "List all documents of a collection.",
                        code = {
                                "collection: \"persons\""
                        }
                )
        }
)
public class List extends AbstractFirestore implements RunnableTask<List.Output> {

    @Schema(
            title = "Whether to store the data from the query results into an ion serialized data file"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean store = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try(var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);
            var documentReferences = collectionRef.listDocuments();

            var outputBuilder = Output.builder();
            if (this.store) {
                Pair<URI, Long> store = this.store(runContext, documentReferences);
                outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight());
            } else {
                Pair<java.util.List<Object>, Long> fetch = this.fetch(documentReferences);
                outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
            }

            Output output = outputBuilder.build();

            runContext.metric(Counter.of(
                    "records", output.getSize(),
                    "collection", this.collection
            ));

            return output;
        }
    }

    private Pair<URI, Long> store(RunContext runContext, Iterable<DocumentReference> documents) throws Exception {
        File tempFile = runContext.tempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (var output = new FileOutputStream(tempFile)) {
            documents.forEach(throwConsumer(document -> {
                count.incrementAndGet();
                var snapshot = document.get().get();
                FileSerde.write(output, snapshot.getData());
            }));
        }

        return Pair.of(
                runContext.putTempFile(tempFile),
                count.get()
        );
    }

    private Pair<java.util.List<Object>, Long> fetch(Iterable<DocumentReference> documents) throws Exception {
        java.util.List<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        documents.forEach(throwConsumer(document -> {
            count.incrementAndGet();
            var snapshot = document.get().get();
            result.add(snapshot.getData());
        }));

        return Pair.of(result, count.get());
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "List containing the fetched data",
                description = "Only populated if `store` parameter is set to false."
        )
        private java.util.List<Object> rows;

        @Schema(
                title = "The size of the fetched rows"
        )
        private Long size;

        @Schema(
                title = "The uri of stored results",
                description = "Only populated if `store` parameter is set to true."
        )
        private URI uri;
    }
}
