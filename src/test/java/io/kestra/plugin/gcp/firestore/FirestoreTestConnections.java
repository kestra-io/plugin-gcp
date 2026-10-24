package io.kestra.plugin.gcp.firestore;

import java.io.IOException;
import java.util.Map;

import org.mockito.Mockito;

import com.google.cloud.NoCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

/**
 * Supplies real Floci-backed Firestore clients to spies of real task instances.
 */
final class FirestoreTestConnections {
    private FirestoreTestConnections() {
    }

    static <T extends AbstractFirestore> T spy(T task, String endpoint)
        throws IllegalVariableEvaluationException, IOException {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("Floci endpoint must not be blank");
        }

        T taskSpy = Mockito.spy(task);
        Mockito.doAnswer(invocation -> connection(taskSpy, endpoint, invocation.getArgument(0)))
            .when(taskSpy)
            .connection(Mockito.any(RunContext.class));

        return taskSpy;
    }

    private static Firestore connection(AbstractFirestore task, String endpoint, RunContext runContext)
        throws IllegalVariableEvaluationException {
        String projectId = runContext.render(task.getProjectId()).as(String.class).orElse(null);
        String databaseId = runContext.render(task.getDatabaseId()).as(String.class).orElse("(default)");

        return FirestoreOptions.newBuilder()
            .setHost(endpoint)
            .setCredentials(NoCredentials.getInstance())
            .setProjectId(projectId)
            .setDatabaseId(databaseId)
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }
}
