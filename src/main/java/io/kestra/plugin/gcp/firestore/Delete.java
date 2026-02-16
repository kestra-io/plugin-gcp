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

import java.time.Instant;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a Firestore document",
    description = "Deletes a document by child path in the target collection; returns the update timestamp."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_firestore_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.gcp.firestore.Delete
                    collection: "persons"
                    childPath: "1"
                """
        )
    }
)
public class Delete extends AbstractFirestore implements RunnableTask<Delete.Output> {
    @Schema(
        title = "Document path",
        description = "Child path (relative to collection) of the document to delete"
    )
    @NotNull
    private Property<String> childPath;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);
            var future = collectionRef.document(runContext.render(this.childPath).as(String.class).orElseThrow()).delete();

            // wait for the write to happen
            var writeResult = future.get();

            return Delete.Output.builder().updatedTime(writeResult.getUpdateTime().toDate().toInstant()).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Updated time"
        )
        private Instant updatedTime;
    }
}
