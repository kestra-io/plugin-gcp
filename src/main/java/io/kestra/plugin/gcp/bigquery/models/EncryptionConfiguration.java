package io.kestra.plugin.gcp.bigquery.models;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class EncryptionConfiguration {
    private final String kmsKeyName;

    public static EncryptionConfiguration of(com.google.cloud.bigquery.EncryptionConfiguration encryptionConfiguration) {
        if (encryptionConfiguration == null) {
            return null;
        }

        return EncryptionConfiguration.builder()
            .kmsKeyName(encryptionConfiguration.getKmsKeyName())
            .build();
    }
}
