package io.kestra.plugin.gcp.bigquery;

import java.io.IOException;
import java.util.Map;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceSettings;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
public abstract class AbstractDataTransfer extends AbstractTask {

    protected DataTransferServiceClient connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials = this.credentials(runContext);

        DataTransferServiceSettings settings = DataTransferServiceSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .setHeaderProvider(FixedHeaderProvider.create(Map.of("user-agent", "Kestra/" + runContext.version())))
            .build();

        return DataTransferServiceClient.create(settings);
    }
}
