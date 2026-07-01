package io.kestra.plugin.gcp.dataflow;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.auth.http.HttpCredentialsAdapter;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.CredentialService;

public class DataflowService {
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
