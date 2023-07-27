package io.kestra.plugin.gcp.secrets;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.api.gax.core.FixedCredentialsProvider;


import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.VersionProvider;
import io.kestra.plugin.gcp.AbstractTask;
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
abstract class AbstractSecretClient extends AbstractTask {
    SecretManagerServiceClient connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        VersionProvider versionProvider = runContext.getApplicationContext().getBean(VersionProvider.class);

        SecretManagerServiceSettings secretManagerServiceSettings = SecretManagerServiceSettings
                .newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
                .setQuotaProjectId(this.projectId)
                .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + versionProvider.getVersion()))
                .build();
        return SecretManagerServiceClient.create(secretManagerServiceSettings);
    }
}