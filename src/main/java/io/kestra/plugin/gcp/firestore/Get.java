package io.kestra.plugin.gcp.firestore;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get a document from a Google Cloud Firestore collection."
)
@Plugin(
    examples = {
        @Example(
            title = "Get a document from its path.",
            full = true,
            code = """
                id: gcp_firestore_get
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.gcp.firestore.Get
                    collection: "persons"
                    childPath: "1"
                """
        )
    }
)
public class Get extends AbstractFirestore implements RunnableTask<Get.Output> {
    @Schema(
        title = "The Firestore document child path."
    )
    @NotNull
    private Property<String> childPath;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        try (var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);
            var data = collectionRef.document(runContext.render(this.childPath).as(String.class).orElseThrow()).get().get().getData();
            return Get.Output.builder()
                .row(data)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Map containing the fetched document."
        )
        private Map<String, Object> row;
    }
}
