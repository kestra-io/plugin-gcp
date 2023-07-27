import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.secretmanager.v1.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.VersionProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

public class Test {
    // Get an existing secret.
    // Get an existing secret version.
    public static void getSecretVersion(String projectId, String secretId, String versionId)
            throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests. After completing all of your requests, call
        // the "close" method on the client to safely clean up any remaining background resources.
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            // Build the name from the version.
            SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, versionId);

            // Create the secret.
            SecretVersion version = client.getSecretVersion(secretVersionName);
            System.out.printf("Secret version %s, state %s\n", version.getName(), version.getState());
            System.out.println(version.getStateValue());
        }
    }

    public static void main(String[] args){
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "kestra-392920";
        String secretId = "TEST_SECRET";
        String versionId = "latest";

        try {
            getSecretVersion(projectId, secretId, versionId);
        }catch(Exception e) {
            System.out.println(e);
            }
    }
}