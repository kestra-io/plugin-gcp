package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Query.Direction;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
            full = true,
            code = """
                id: gcp_firestore_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.gcp.firestore.Query
                    collection: "persons"
                    filters:
                      - field: "lastname"
                        value: "Doe"
                """
        )
    }
)
public class Query extends AbstractFirestore implements RunnableTask<FetchOutput> {
    @Schema(
        title = "The way you want to store the data",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.STORE);

    @Schema(
        title = "List of query filters that will be added as a where clause."
    )
    @PluginProperty
    private List<Filter> filters;

    @Schema(
        title = "Field name for the order by clause."
    )
    private Property<String> orderBy;

    @Schema(
        title = "Field name for the order by clause."
    )
    @Builder.Default
    private Property<Direction> orderDirection = Property.of(Direction.ASCENDING);

    @Schema(
        title = "Start offset for pagination of the query results."
    )
    private Property<Integer> offset;

    @Schema(
        title = "Maximum numbers of returned results."
    )
    private Property<Integer> limit;

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        try (var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);

            var query = getQuery(runContext, collectionRef, this.filters);

            if (this.orderBy != null) {
                query.orderBy(
                    runContext.render(this.orderBy).as(String.class).orElseThrow(),
                    runContext.render(this.orderDirection).as(Direction.class).orElseThrow()
                );
            }

            if (this.offset != null) {
                query.offset(runContext.render(this.offset).as(Integer.class).orElseThrow());
            }

            if (this.limit != null) {
                query.limit(runContext.render(this.limit).as(Integer.class).orElseThrow());
            }

            var queryDocumentSnapshots = query.get().get().getDocuments();

            var outputBuilder = FetchOutput.builder();
            switch (runContext.render(fetchType).as(FetchType.class).orElseThrow()) {
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

            var output = outputBuilder.build();

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
        switch (runContext.render(filter.getOperator()).as(QueryOperator.class).orElseThrow()) {
            case EQUAL_TO: {
                return query.whereEqualTo(
                    runContext.render(filter.getField()).as(String.class).orElseThrow(),
                    runContext.render(filter.getValue()).as(String.class).orElse(null)
                );
            }
            case NOT_EQUAL_TO: {
                return query.whereNotEqualTo(
                    runContext.render(filter.getField()).as(String.class).orElseThrow(),
                    runContext.render(filter.getValue()).as(String.class).orElse(null)
                );
            }
            case LESS_THAN: {
                return query.whereLessThan(
                    runContext.render(filter.getField()).as(String.class).orElseThrow(),
                    runContext.render(filter.getValue()).as(String.class).orElseThrow()
                );
            }
            case LESS_THAN_OR_EQUAL_TO: {
                return query.whereLessThanOrEqualTo(
                    runContext.render(filter.getField()).as(String.class).orElseThrow(),
                    runContext.render(filter.getValue()).as(String.class).orElseThrow()
                );
            }
            case GREATER_THAN: {
                return query.whereGreaterThan(
                    runContext.render(filter.getField()).as(String.class).orElseThrow(),
                    runContext.render(filter.getValue()).as(String.class).orElseThrow()
                );
            }
            case GREATER_THAN_OR_EQUAL_TO: {
                return query.whereGreaterThanOrEqualTo(
                    runContext.render(filter.getField()).as(String.class).orElseThrow(),
                    runContext.render(filter.getValue()).as(String.class).orElseThrow()
                );
            }
            // more where clause could be supported, but we need to validate that the value is a List
        }
        // Should never occur
        throw new IllegalArgumentException("Unknown QueryOperator: " + filter.getOperator());
    }

    private Pair<URI, Long> store(RunContext runContext, List<QueryDocumentSnapshot> documents) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Flux<Map<String, Object>> flux = Flux.fromIterable(documents).map(snapshot -> snapshot.getData());
            Long count = FileSerde.writeAll(output, flux).block();

            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
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
    @Schema(
        title = "A filter for the where clause"
    )
    public static class Filter {
        @Schema(
            title = "Field name for the filter."
        )
        @NotNull
        private Property<String> field;

        @Schema(
            title = "Field value for the filter.",
            description = "Field value for the filter. Only strings are supported at the moment."
        )
        @NotNull
        private Property<String> value;

        @Schema(
            title = "The operator for the filter, by default EQUAL_TO that will call 'collection.whereEqualTo(name, value)'"
        )
        @Builder.Default
        private Property<QueryOperator> operator = Property.of(QueryOperator.EQUAL_TO);
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
