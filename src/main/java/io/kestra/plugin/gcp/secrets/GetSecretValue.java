package io.kestra.plugin.gcp.secrets;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretVersionName;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.StreamCorruptedException;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Retrieve secret value from Google Secret Manager."
)
@Plugin(
        examples = {
                @Example(
                        title = "Get secret value",
                        code = {
                                "name: \"GITHUB_TOKEN\"",
                                "version: \"latest\""
                        }
                )
        }
)
public class GetSecretValue extends AbstractSecretClient implements RunnableTask<GetSecretValue.Output> {
    @Schema(
            title = "The Secret name"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String name;

    @Schema(
            title = "The Secret version to read"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String version;

    @Override
    public GetSecretValue.Output run(RunContext runContext) throws Exception {
        try (var secretClient = this.connection(runContext)) {
            SecretVersionName secretVersionName = SecretVersionName.of(this.projectId, this.name, this.version);
            AccessSecretVersionResponse response = secretClient.accessSecretVersion(secretVersionName);
            String secret = response.getPayload().getData().toStringUtf8();

            // Checking data is correct
            byte[] data = response.getPayload().getData().toByteArray();
            Checksum checksum = new CRC32C();
            checksum.update(data, 0, data.length);
            if (response.getPayload().getDataCrc32C() != checksum.getValue()) {
                throw new StreamCorruptedException("Data corruption detected when reading secret : " + this.name);
            }
            return GetSecretValue.Output.builder()
                    .secretValue(secret)
                    .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "String containing the fetched secret value."
        )
        private String secretValue;
    }
}
