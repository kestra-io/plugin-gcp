package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Getter
@Builder
@Jacksonized
public class Schema {
    @io.swagger.v3.oas.annotations.media.Schema(
        name = "the fields associated to this schema."
    )
    private final List<Field> fields;

    public static Schema of(com.google.cloud.bigquery.Schema schema) {
        return Schema.builder()
            .fields(
                schema.getFields()
                    .stream()
                    .map(Field::of)
                    .collect(Collectors.toList())
            )
            .build();
    }

    public com.google.cloud.bigquery.Schema to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.bigquery.Schema.of(
            this.getFields()
                .stream()
                .map(throwFunction(field -> field.to(runContext)))
                .collect(Collectors.toList())
        );
    }
}
