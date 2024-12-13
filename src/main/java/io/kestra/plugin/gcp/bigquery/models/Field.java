package io.kestra.plugin.gcp.bigquery.models;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Getter
@Builder
@Jacksonized
public class Field {
    @Schema(
        title = "The field name."
    )
    private final Property<String> name;

    @Schema(
        title = "The field type."
    )
    private final Property<StandardSQLTypeName> type;

    @Schema(
        title = "The list of sub-fields if `type` is a `LegacySQLType.RECORD`. Returns null otherwise."
    )
    private final List<Field> subFields;

    @Schema(
        title = "The field mode.",
        description = "By default, `Field.Mode.NULLABLE` is used."
    )
    private final Property<com.google.cloud.bigquery.Field.Mode> mode;

    @Schema(
        title = "The field description."
    )
    private final Property<String> description;

    @Schema(
        title = "The policy tags for the field."
    )
    private final PolicyTags policyTags;

    public static Field.Output of(com.google.cloud.bigquery.Field field) {
        return Field.Output.builder()
            .name(field.getName())
            .type(field.getType().getStandardType())
            .subFields(field.getSubFields() == null ? null : field.getSubFields()
                .stream()
                .map(Field::of)
                .collect(Collectors.toList())
            )
            .mode(field.getMode())
            .description(field.getDescription())
            .policyTags(field.getPolicyTags()  == null ? null : PolicyTags.Output.builder()
                .names(field.getPolicyTags().getNames())
                .build()
            )
            .build();
    }

    public com.google.cloud.bigquery.Field to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.Field.Builder builder = com.google.cloud.bigquery.Field.newBuilder(
            runContext.render(this.getName()).as(String.class).orElse(null),
            runContext.render(this.getType()).as(StandardSQLTypeName.class).orElse(null),
            this.getSubFields() == null ? null : FieldList.of(
                this.getSubFields()
                    .stream()
                    .map(throwFunction(field -> field.to(runContext)))
                    .collect(Collectors.toList())
            )
        );

        if (this.mode != null) {
            builder.setMode(runContext.render(this.mode).as(com.google.cloud.bigquery.Field.Mode.class).orElseThrow());
        }

        if (this.description != null) {
            builder.setDescription(runContext.render(this.description).as(String.class).orElseThrow());
        }

        if (this.policyTags != null) {
            builder.setPolicyTags(com.google.cloud.bigquery.PolicyTags.newBuilder()
                .setNames(runContext.render(this.policyTags.getNames()).asList(String.class))
                .build()
            );
        }

        return builder.build();
    }

    @Builder
    @Getter
    public static class Output {
        private final String name;
        private final StandardSQLTypeName type;
        private final List<Field.Output> subFields;
        private final com.google.cloud.bigquery.Field.Mode mode;
        private final String description;
        private final PolicyTags.Output policyTags;
    }
}
