package io.kestra.plugin.gcp.firestore;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.time.Instant;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Delete a document from a collection.",
        description = "Delete a document from a collection."
)
@Plugin(
        examples = {
                @Example(
                        title = "Delete a document.",
                        code = {
                                "collection: \"persons\"",
                                "childPath: \"1\""
                        }
                )
        }
)
public class Delete extends AbstractFirestore implements RunnableTask<Delete.Output> {
    @Schema(
            title = "The Firestore document child path.",
            description = "The Firestore document child path."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String childPath;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try(var firestore = this.connection(runContext)) {
            var collectionRef = this.collection(runContext, firestore);
            var future = collectionRef.document(runContext.render(this.childPath)).delete();

            // wait for the write to happen
            var writeResult = future.get();

            return Delete.Output.builder().updateTime(writeResult.getUpdateTime().toDate().toInstant()).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "The document update time"
        )
        private Instant updateTime;
    }
}
