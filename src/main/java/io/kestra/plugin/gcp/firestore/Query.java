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
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                "filters: ",
                "- field: \"lastname\"",
                "  value: \"Doe\""
            }
        )
    }
)
public class Query extends AbstractFirestore implements RunnableTask<Query.Output> {
    @Schema(
        title = "The way you want to store the data",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    private StoreType storeType = StoreType.STORE;

    @Schema(
        title = "List of query filters that will be added as a where clause."
    )
    @PluginProperty(dynamic = false)
    private List<Filter> filters;

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

            var query = getQuery(runContext, collectionRef, this.filters);

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
                    Pair<List<Object>, Long> fetch = this.fetch(queryDocumentSnapshots);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                break;

                case FETCH_ONE:
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

    private com.google.cloud.firestore.Query getQuery(RunContext runContext, CollectionReference collectionRef, List<Filter> filters)
        throws IllegalVariableEvaluationException {
        // this is a no-op but allow to create an empty query
        var query = collectionRef.offset(0);
        if (this.filters == null || this.filters.isEmpty()) {
            return query;
        }

        for(Filter option : filters)  {
           query = appendQueryPart(runContext, query, option);
        }
        return query;
    }

    private com.google.cloud.firestore.Query appendQueryPart(RunContext runContext, com.google.cloud.firestore.Query query,
                                                             Filter filter)
        throws IllegalVariableEvaluationException {
        switch (filter.getOperator()) {
            case EQUAL_TO: {
                return query.whereEqualTo(filter.getField(), runContext.render(filter.getValue()));
            }
            case NOT_EQUAL_TO: {
                return query.whereNotEqualTo(filter.getField(), runContext.render(filter.getValue()));
            }
            case LESS_THAN: {
                return query.whereLessThan(filter.getField(), runContext.render(filter.getValue()));
            }
            case LESS_THAN_OR_EQUAL_TO: {
                return query.whereLessThanOrEqualTo(filter.getField(), runContext.render(filter.getValue()));
            }
            case GREATER_THAN: {
                return query.whereGreaterThan(filter.getField(), runContext.render(filter.getValue()));
            }
            case GREATER_THAN_OR_EQUAL_TO: {
                return query.whereGreaterThanOrEqualTo(filter.getField(), runContext.render(filter.getValue()));
            }
            // more where clause could be supported, but we need to validate that the value is a List
        }
        // Should never occur
        throw new IllegalArgumentException("Unknown QueryOperator: " + filter.getOperator());
    }

    private Pair<URI, Long> store(RunContext runContext, List<QueryDocumentSnapshot> documents) throws IOException {
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

    private Pair<List<Object>, Long> fetch(List<QueryDocumentSnapshot> documents) {
        List<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        documents.forEach(throwConsumer(snapshot -> {
            count.incrementAndGet();
            result.add(snapshot.getData());
        }));

        return Pair.of(result, count.get());
    }


    private Map<String, Object> fetchOne(List<QueryDocumentSnapshot> documents) {
        if (documents.isEmpty()) {
            return null;
        }

        return documents.get(0).getData();
    }

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    @Introspected
    @Schema(
        title = "A filter for the where clause"
    )
    public static class Filter {
        @Schema(
            title = "Field name for the filter."
        )
        @PluginProperty(dynamic = false)
        @NotNull
        private String field;

        @Schema(
            title = "Field value for the filter.",
            description = "Field value for the filter. Only strings are supported at the moment."
        )
        @PluginProperty(dynamic = true)
        @NotNull
        private String value;

        @Schema(
            title = "The operator for the filter, by default EQUAL_TO that will call 'collection.whereEqualTo(name, value)'"
        )
        @PluginProperty(dynamic = false)
        @Builder.Default
        private QueryOperator operator = QueryOperator.EQUAL_TO;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List containing the fetched data",
            description = "Only populated if using `FETCH`."
        )
        private List<Object> rows;

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
