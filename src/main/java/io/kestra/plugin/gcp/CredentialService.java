package io.kestra.plugin.gcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public final class CredentialService {
    private CredentialService() {
    }

    public static GoogleCredentials credentials(RunContext runContext, GcpInterface gcpInterface) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials;


        if (gcpInterface.getServiceAccount() != null) {
            String serviceAccount = runContext.render(gcpInterface.getServiceAccount());
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serviceAccount.getBytes());
            credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);
            Logger logger = runContext.logger();

            if (logger.isTraceEnabled()) {
                byteArrayInputStream.reset();
                Map<String, String> jsonKey = JacksonMapper.ofJson().readValue(byteArrayInputStream, new TypeReference<>() {});
                if (jsonKey.containsKey("client_email")) {
                    logger.trace(" • Using service account: {}", jsonKey.get("client_email") );
                }
            }
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        if (gcpInterface.getScopes() != null) {
            credentials = credentials.createScoped(runContext.render(gcpInterface.getScopes()));
        }

        return credentials;
    }
}
