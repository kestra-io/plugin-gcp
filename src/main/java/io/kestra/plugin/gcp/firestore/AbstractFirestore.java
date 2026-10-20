package io.kestra.plugin.gcp.firestore;

import java.io.IOException;
import java.util.Map;

import com.google.cloud.NoCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.shared.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractFirestore extends AbstractTask {
    private static final ThreadLocal<String> EMULATOR_ENDPOINT = new ThreadLocal<>();

    @Schema(
        title = "Collection path",
        description = "Target collection name or path"
    )
    @PluginProperty(group = "advanced")
    protected Property<String> collection;

    @Schema(
        title = "Database ID",
        description = "Defaults to `(default)` if not provided"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<String> databaseId = Property.ofValue("(default)");

    Firestore connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        String emulatorEndpoint = EMULATOR_ENDPOINT.get();
        var builder = FirestoreOptions.newBuilder();
        if (emulatorEndpoint == null) {
            // Resolve credentials before projectId because credentials may infer it from the service account.
            builder.setCredentials(this.credentials(runContext));
        } else {
            builder
                .setHost(emulatorEndpoint)
                .setCredentials(NoCredentials.getInstance());
        }

        return builder
            .setProjectId(runContext.render(projectId).as(String.class).orElse(null))
            .setDatabaseId(runContext.render(databaseId).as(String.class).orElse("(default)"))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }

    static AutoCloseable useEmulatorEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("Firestore emulator endpoint must not be blank");
        }

        String previousEndpoint = EMULATOR_ENDPOINT.get();
        EMULATOR_ENDPOINT.set(endpoint);

        return () ->
        {
            if (previousEndpoint == null) {
                EMULATOR_ENDPOINT.remove();
            } else {
                EMULATOR_ENDPOINT.set(previousEndpoint);
            }
        };
    }

    CollectionReference collection(RunContext runContext, Firestore firestore) throws IllegalVariableEvaluationException {
        return firestore.collection(runContext.render(collection).as(String.class).orElse(null));
    }
}
