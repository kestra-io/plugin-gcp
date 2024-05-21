package io.kestra.plugin.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task implements GcpInterface {
    protected String projectId;

    protected String serviceAccount;

    @Builder.Default
    protected List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    public GoogleCredentials credentials(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return CredentialService.credentials(runContext, this);
    }
}
