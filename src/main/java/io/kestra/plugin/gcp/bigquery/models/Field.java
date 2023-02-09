package io.kestra.plugin.gcp.bigquery.models;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
        title = "the field name."
    )
    @PluginProperty(dynamic = true)
    private final String name;

    @Schema(
        title = "the field name."
    )
    @PluginProperty(dynamic = false)
    private final StandardSQLTypeName type;

    @Schema(
        title = "the list of sub-fields if `type` is a `LegacySQLType.RECORD`. Returns null otherwise."
    )
    @PluginProperty(dynamic = true)
    private final List<Field> subFields;

    @Schema(
        title = "the field mode.",
        description = "By default `Field.Mode.NULLABLE` is used."
    )
    @PluginProperty(dynamic = true)
    private final com.google.cloud.bigquery.Field.Mode mode;

    @Schema(
        title = "the field description."
    )
    @PluginProperty(dynamic = true)
    private final String description;

    @Schema(
        title = "the policy tags for the field."
    )
    @PluginProperty(dynamic = true)
    private final PolicyTags policyTags;

    public static Field of(com.google.cloud.bigquery.Field field) {
        return Field.builder()
            .name(field.getName())
            .type(field.getType().getStandardType())
            .subFields(
                field.getSubFields() == null ? null
                    : field.getSubFields()
                        .stream()
                        .map(Field::of)
                        .collect(Collectors.toList())
            )
            .mode(field.getMode())
            .description(field.getDescription())
            .policyTags(
                field.getPolicyTags() == null ? null
                    : PolicyTags.builder()
                        .names(field.getPolicyTags().getNames())
                        .build()
            )
            .build();
    }

    public com.google.cloud.bigquery.Field to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.Field.Builder builder = com.google.cloud.bigquery.Field.newBuilder(
            this.getName(),
            this.getType(),
            this.getSubFields() == null ? null
                : FieldList.of(
                    this.getSubFields()
                        .stream()
                        .map(throwFunction(field -> field.to(runContext)))
                        .collect(Collectors.toList())
                )
        );

        if (this.mode != null) {
            builder.setMode(this.mode);
        }

        if (this.description != null) {
            builder.setDescription(runContext.render(this.description));
        }

        if (this.policyTags != null) {
            builder.setPolicyTags(
                com.google.cloud.bigquery.PolicyTags.newBuilder()
                    .setNames(this.policyTags.getNames())
                    .build()
            );
        }

        return builder.build();
    }
}
