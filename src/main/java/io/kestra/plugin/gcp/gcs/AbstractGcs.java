package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractGcs extends AbstractTask {
    Storage connection(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        return StorageOptions
            .newBuilder()
            .setCredentials(this.credentials(runContext))
            .setProjectId(runContext.render(projectId))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
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
