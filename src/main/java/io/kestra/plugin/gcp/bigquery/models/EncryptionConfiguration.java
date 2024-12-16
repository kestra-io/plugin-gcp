package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class EncryptionConfiguration {
    @Schema(
        name = "The KMS key name."
    )
    private final Property<String> kmsKeyName;

    public static EncryptionConfiguration.Output of(com.google.cloud.bigquery.EncryptionConfiguration encryptionConfiguration) {
        if (encryptionConfiguration == null) {
            return null;
        }

        return EncryptionConfiguration.Output.builder()
            .kmsKeyName(encryptionConfiguration.getKmsKeyName())
            .build();
    }

    public com.google.cloud.bigquery.EncryptionConfiguration to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.bigquery.EncryptionConfiguration.newBuilder()
            .setKmsKeyName(runContext.render(this.kmsKeyName).as(String.class).orElse(null))
            .build();
    }

    @Getter
    @Builder
    public static class Output {
        private String kmsKeyName;
    }
}
