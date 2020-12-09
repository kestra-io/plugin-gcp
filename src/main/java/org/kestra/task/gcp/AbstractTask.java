package org.kestra.task.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    protected GoogleCredentials credentials(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials;
        
        if (serviceAccount != null) {
            String serviceAccount = runContext.render(this.serviceAccount);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serviceAccount.getBytes());
            credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        if (this.scopes != null) {
            credentials = credentials.createScoped(runContext.render(this.scopes));
        }

        return credentials;
    }
}
