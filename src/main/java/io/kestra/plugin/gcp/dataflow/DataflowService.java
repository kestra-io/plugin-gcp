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
        var requestInitializer = new HttpCredentialsAdapter(credentials);

        return new Dataflow.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            requestInitializer
        )
            .setApplicationName("Kestra")
            .build();
    }
}
