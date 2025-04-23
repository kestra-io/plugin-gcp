package io.kestra.plugin.gcp.firestore;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Set a document in a Google Cloud Firestore collection."
)
@Plugin(
    examples = {
        @Example(
            title = "Set a document from a map.",
            full = true,
            code = """
                id: gcp_firestore_set
                namespace: company.team

                tasks:
                  - id: set
                    type: io.kestra.plugin.gcp.firestore.Set
                    collection: "persons"
                    document:
                      firstname: "John"
                      lastname: "Doe"
                """
        ),
        @Example(
            title = "Set a document from a JSON string.",
            full = true,
            code = """
                id: gcp_firestore_set
                namespace: company.team

                inputs:
                  - id: json_string
                    type: STRING
                    default: "{\"firstname\": \"John\", \"lastname\": \"Doe\"}"

                tasks:
                  - id: set
                    type: io.kestra.plugin.gcp.firestore.Set
                    collection: "persons"
                    document: "{{ inputs.json_string }}"
                """
        )
    }
)
public class Set extends AbstractFirestore implements RunnableTask<Set.Output> {
    @Schema(
        title = "The Firestore document.",
        description = "Can be a JSON string, or a map.",
        anyOf = {String.class, Map.class}
    )
    @PluginProperty(dynamic = true)
    private Object document;

    @Schema(
        title = "The Firestore document child path."
    )
    private Property<String> childPath;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);
            var documentReference = this.childPath == null ?
                collectionRef.document() :
                collectionRef.document(runContext.render(this.childPath).as(String.class).orElseThrow());
            var future = documentReference.set(fields(runContext, this.document));

            // wait for the write to happen
            var writeResult = future.get();

            return Output.builder().updatedTime(writeResult.getUpdateTime().toDate().toInstant()).build();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fields(RunContext runContext, Object value) throws IllegalVariableEvaluationException, JsonProcessingException {
        if (value instanceof String) {
            return JacksonMapper.toMap(runContext.render((String) value));
        } else if (value instanceof Map) {
            return runContext.render((Map<String, Object>) value);
        } else if (value == null) {
            return Collections.emptyMap();
        }

        throw new IllegalVariableEvaluationException("Invalid value type '" + value.getClass() + "'");
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The document updated time"
        )
        private Instant updatedTime;
    }
}
