package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Query.Direction;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.gcp.StoreType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query documents of a collection."
)
@Plugin(
    examples = {
        @Example(
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
        title = "The way you want to store the data",
        description = "FETCHONE output the first row"
            + "FETCH output all the row"
            + "STORE store all row in a file"
            + "NONE do nothing"
    )
    @Builder.Default
    private StoreType storeType = StoreType.STORE;

    @Schema(
        title = "Field name for the where clause.",
        description = "Field name for the where clause. If null all the collection will be retrieved."
    )
    @PluginProperty(dynamic = false)
    private String field;

    @Schema(
        title = "Field value for the where clause.",
        description = "Field value for the where clause. Only strings are supported at the moment."
    )
    @PluginProperty(dynamic = true)
    private String value;

    @Schema(
        title = "The query operator for the where clause, by default EQUAL_TO that will call 'collection.whereEqualTo(name, value)'"
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
        title = "Field name for the order by clause."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Direction orderDirection = Direction.ASCENDING;

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
        try (var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);

            var query = getQuery(runContext, collectionRef, this.queryOperator);

            if (this.orderBy != null) {
                query.orderBy(this.orderBy, this.orderDirection);
            }

            if (this.offset != null) {
                query.offset(this.offset);
            }

            if (this.limit != null) {
                query.limit(this.limit);
            }

            var queryDocumentSnapshots = query.get().get().getDocuments();

            var outputBuilder = Query.Output.builder();
            switch (storeType) {
                case FETCH:
                    Pair<java.util.List<Object>, Long> fetch = this.fetch(queryDocumentSnapshots);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                break;

                case FETCHONE:
                    var o = this.fetchOne(queryDocumentSnapshots);

                    outputBuilder
                        .row(o)
                        .size(o != null ? 1L : 0L);
                break;

                case STORE:
                    Pair<URI, Long> store = this.store(runContext, queryDocumentSnapshots);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight());
                break;
            }

            Query.Output output = outputBuilder.build();

            runContext.metric(Counter.of(
                "records", output.getSize(),
                "collection", collectionRef.getId()
            ));

            return output;
        }
    }

    private com.google.cloud.firestore.Query getQuery(RunContext runContext, CollectionReference collectionRef, QueryOperator queryOperator)
        throws IllegalVariableEvaluationException {
        if(this.field == null) {
            // this is a no-op but allow to get an empty query
            return collectionRef.offset(0);
        }
        switch (queryOperator) {
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
            // more where clause could be supported, but we need to validate that the value is a list
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

    private Pair<java.util.List<Object>, Long> fetch(java.util.List<QueryDocumentSnapshot> documents) {
        java.util.List<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        documents.forEach(throwConsumer(snapshot -> {
            count.incrementAndGet();
            result.add(snapshot.getData());
        }));

        return Pair.of(result, count.get());
    }


    private Map<String, Object> fetchOne(java.util.List<QueryDocumentSnapshot> documents) {
        if (documents.isEmpty()) {
            return null;
        }

        return documents.get(0).getData();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List containing the fetched data",
            description = "Only populated if using `FETCH`."
        )
        private java.util.List<Object> rows;

        @Schema(
            title = "Map containing the first row of fetched data",
            description = "Only populated if using `FETCHONE`."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The uri of store result",
            description = "Only populated if using `STORE`"
        )
        private URI uri;

        @Schema(
            title = "The size of the rows fetch"
        )
        private Long size;
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
