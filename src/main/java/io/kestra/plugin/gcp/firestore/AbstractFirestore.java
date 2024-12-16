package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractFirestore extends AbstractTask {
    @Schema(
        title = "The Firestore collection"
    )
    protected Property<String> collection;

    Firestore connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FirestoreOptions.newBuilder()
            .setCredentials(this.credentials(runContext))
            .setProjectId(runContext.render(projectId).as(String.class).orElse(null))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }

    CollectionReference collection(RunContext runContext, Firestore firestore) throws IllegalVariableEvaluationException {
        return firestore.collection(runContext.render(collection).as(String.class).orElse(null));
    }
}
