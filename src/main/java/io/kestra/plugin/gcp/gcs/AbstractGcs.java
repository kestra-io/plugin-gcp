package io.kestra.plugin.gcp.gcs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.shared.AbstractTask;

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
public abstract class AbstractGcs extends AbstractTask {
    private static final ThreadLocal<String> EMULATOR_ENDPOINT = new ThreadLocal<>();

    Storage connection(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        String emulatorEndpoint = EMULATOR_ENDPOINT.get();
        StorageOptions.Builder builder = StorageOptions.newBuilder();
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
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }

    static AutoCloseable useEmulatorEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("GCS emulator endpoint must not be blank");
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

    static URI encode(RunContext runContext, String blob) throws IllegalVariableEvaluationException, URISyntaxException {
        return new URI(encode(runContext.render(blob)));
    }

    static String encode(String blob) {
        return blob.replace(" ", "%20");
    }

    static String blobPath(String path) {
        return path.replace("+", " ");
    }
}
