package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import java.io.IOException;

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
            .build()
            .getService();
    }
}
