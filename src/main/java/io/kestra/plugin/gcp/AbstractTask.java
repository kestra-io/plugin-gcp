package io.kestra.plugin.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.models.property.Property;
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
    protected Property<String> projectId;

    protected Property<String> serviceAccount;

    protected Property<String> impersonatedServiceAccount;

    @Builder.Default
    protected Property<List<String>> scopes = Property.of(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

    public GoogleCredentials credentials(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials = CredentialService.credentials(runContext, this);

        //Infer projectID from credentials if projectId is null
        if (credentials instanceof ServiceAccountCredentials serviceAccountCredentials) {
            String projectIdFromServiceAccount = serviceAccountCredentials.getProjectId();
            projectId = projectId != null ? projectId : new Property<>(projectIdFromServiceAccount);
        }
        return credentials;
    }
}