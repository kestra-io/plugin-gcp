package io.kestra.plugin.gcp.dataflow;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.auth.http.HttpCredentialsAdapter;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.kestra.plugin.gcp.CredentialService;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
public abstract class AbstractDataflow extends AbstractTask implements DataflowConnectionInterface {

    @NotNull
    @Schema(title = "The regional endpoint (e.g. us-central1)")
    @PluginProperty(group = "connection")
    protected Property<String> location;

    protected Dataflow dataflowClient(RunContext runContext) throws Exception {
        return dataflowClient(runContext, this);
    }

    public static Dataflow dataflowClient(RunContext runContext, DataflowConnectionInterface connection) throws Exception {
        var credentials = CredentialService.credentials(runContext, connection);
        var credentialsAdapter = new HttpCredentialsAdapter(credentials);
        var requestInitializer = new com.google.api.client.http.HttpRequestInitializer() {
            @Override
            public void initialize(com.google.api.client.http.HttpRequest request) throws java.io.IOException {
                credentialsAdapter.initialize(request);
                request.setConnectTimeout(30000);
                request.setReadTimeout(60000);
            }
        };

        return new Dataflow.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            requestInitializer
        )
            .setApplicationName("Kestra")
            .build();
    }
}
