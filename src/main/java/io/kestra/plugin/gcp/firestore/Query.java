package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
        title = "Query documents of a collection.",
        description = "Query documents of a collection."
)
@Plugin(
        examples = {
                @Example(
                        title = "Query documents of a collection.",
                        code = {
                                "collection: \"persons\"",
                                "field: \"firstname\"",
                                "value: \"Doe\""
                        }
                )
        }
)
public class Query extends AbstractFirestore implements RunnableTask<Query.Output> {

    @Schema(
            title = "Whether to store the data from the query result into an ion serialized data file."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean store = false;

    @Schema(
            title = "Field name for the where clause."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    private String field;

    @Schema(
            title = "Field value for the where clause.",
            description = "Field value for the where clause. Only strings are supported at the moment."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String value;

    @Schema(
            title = "The query operator for the where clause, by default EQUAL_TO that will call 'collection.whereEqualTo(name, value)'",
            description = "Can be one of EQUAL_TO, " +
                    "NOT_EQUAL_TO, " +
                    "LESS_THAN, " +
                    "LESS_THAN_OR_EQUAL_TO, " +
                    "GREATER_THAN, " +
                    "GREATER_THAN_OR_EQUAL_TO"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private QueryOperator queryOperator = QueryOperator.EQUAL_TO;

    @Schema(
            title = "Field name for the order by clause."
    )
    @PluginProperty(dynamic = false)
    private String orderBy;

    @Schema(
            title = "Start offset for pagination of the query results."
    )
    @PluginProperty(dynamic = false)
    private Integer offset;

    @Schema(
            title = "Maximum numbers of returned results."
    )
    @PluginProperty(dynamic = false)
    private Integer limit;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        try(var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);

            var query = getQuery(runContext, collectionRef, this.queryOperator);
            if(this.orderBy != null) {
                query.orderBy(this.orderBy);
            }
            if(this.offset != null) {
                query.offset(this.offset);
            }
            if(this.limit != null) {
                query.limit(this.limit);
            }
            var queryDocumentSnapshots = query.get().get().getDocuments();

            var outputBuilder = Query.Output.builder();
            if (this.store) {
                Pair<URI, Long> store = this.store(runContext, queryDocumentSnapshots);
                outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight());
            } else {
                Pair<java.util.List<Object>, Long> fetch = this.fetch(queryDocumentSnapshots);
                outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
            }

            Query.Output output = outputBuilder.build();

            runContext.metric(Counter.of(
                    "records", output.getSize(),
                    "collection", this.collection
            ));

            return output;
        }
    }

    private com.google.cloud.firestore.Query getQuery(RunContext runContext, CollectionReference collectionRef, QueryOperator queryOperator)
            throws IllegalVariableEvaluationException {
        switch(queryOperator) {
            case EQUAL_TO: {
                return collectionRef.whereEqualTo(this.field, runContext.render(this.value));
            }
            case NOT_EQUAL_TO: {
                return collectionRef.whereNotEqualTo(this.field, runContext.render(this.value));
            }
            case LESS_THAN: {
                return collectionRef.whereLessThan(this.field, runContext.render(this.value));
            }
            case LESS_THAN_OR_EQUAL_TO: {
                return collectionRef.whereLessThanOrEqualTo(this.field, runContext.render(this.value));
            }
            case GREATER_THAN: {
                return collectionRef.whereGreaterThan(this.field, runContext.render(this.value));
            }
            case GREATER_THAN_OR_EQUAL_TO: {
                return collectionRef.whereGreaterThanOrEqualTo(this.field, runContext.render(this.value));
            }
            // more where clause could be supported but we need to validate that the value is a list
        }
        // Should never occur
        throw new IllegalArgumentException("Unknown QueryOperator: " + queryOperator);
    }

    private Pair<URI, Long> store(RunContext runContext, java.util.List<QueryDocumentSnapshot> documents) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (var output = new FileOutputStream(tempFile)) {
            documents.forEach(throwConsumer(snapshot -> {
                count.incrementAndGet();
                FileSerde.write(output, snapshot.getData());
            }));
        }

        return Pair.of(
                runContext.putTempFile(tempFile),
                count.get()
        );
    }

    private Pair<java.util.List<Object>, Long> fetch(java.util.List<QueryDocumentSnapshot> documents)  {
        java.util.List<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        documents.forEach(throwConsumer(snapshot -> {
            count.incrementAndGet();
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
                title = "The size of the rows fetch"
        )
        private Long size;

        @Schema(
                title = "The uri of store result",
                description = "Only populated if `store` parameter is set to true."
        )
        private URI uri;
    }

    public enum QueryOperator {
        EQUAL_TO,
        NOT_EQUAL_TO,
        LESS_THAN,
        LESS_THAN_OR_EQUAL_TO,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL_TO
    }
}
