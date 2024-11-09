package io.kestra.plugin.gcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public final class CredentialService {
  private CredentialService() {
  }

  public static GoogleCredentials credentials(RunContext runContext, GcpInterface gcpInterface)
      throws IllegalVariableEvaluationException, IOException {
    GoogleCredentials credentials;

    HashMap<String, String> serviceAccount = gcpInterface.getServiceAccount();

    if (serviceAccount.get("key") != null) {
      String serviceAccountJson = runContext.render(serviceAccount.get("key"));
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serviceAccountJson.getBytes());
      credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);
      Logger logger = runContext.logger();

      if (logger.isTraceEnabled()) {
        byteArrayInputStream.reset();
        Map<String, String> jsonKey = JacksonMapper.ofJson().readValue(byteArrayInputStream, new TypeReference<>() {
        });
        if (jsonKey.containsKey("client_email")) {
          logger.trace(" • Using service account: {}", jsonKey.get("client_email"));
        }
      }
    } else {
      credentials = GoogleCredentials.getApplicationDefault();
    }

    if (gcpInterface.getScopes() != null) {
      credentials = credentials.createScoped(runContext.render(gcpInterface.getScopes()));
    }

    if (serviceAccount.get("impersonate") != null) {
      List<String> targetScopes = runContext.render(gcpInterface.getScopes());
      credentials = ImpersonatedCredentials.create(credentials, serviceAccount.get("impersonate"), null, targetScopes,
          3600);
    }

    return credentials;
  }
}
