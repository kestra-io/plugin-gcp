package io.kestra.plugin.gcp.monitoring;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractMonitoringTask extends AbstractTask {
    protected MetricServiceClient connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials = this.credentials(runContext);

        MetricServiceSettings settings = MetricServiceSettings.newBuilder()
            .setCredentialsProvider(() -> credentials)
            .build();

        return MetricServiceClient.create(settings);
    }
}
