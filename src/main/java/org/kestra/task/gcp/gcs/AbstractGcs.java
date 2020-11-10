package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.runners.RunContext;
import org.kestra.task.gcp.AbstractTask;

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
