package io.kestra.plugin.gcp.firestore;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Get a document from a collection."
)
@Plugin(
    examples = {
        @Example(
            title = "Get a document from its path.",
            code = {
                "collection: \"persons\"",
                "childPath: \"1\""
            }
        )
    }
)
public class Get extends AbstractFirestore implements RunnableTask<Get.Output> {
    @Schema(
        title = "The Firestore document child path."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String childPath;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        try (var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);
            var data = collectionRef.document(this.childPath).get().get().getData();
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
