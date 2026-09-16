package io.kestra.plugin.gcp;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.shared.AbstractTask;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Guards the project-id inference contract of the shared kernel from inside plugin-gcp's own build.
 * The kernel's tests live in plugin-gcp-lib and this repo's CI never runs them, so this asserts
 * that a concrete {@link AbstractTask} still infers projectId from a service-account key here.
 */
@KestraTest
class AbstractTaskInferenceTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void infersProjectIdFromServiceAccountKey() throws Exception {
        RunContext runContext = runContextFactory.of();
        Property<String> serviceAccountKey = Property.ofValue(serviceAccountJson());
        AbstractTask task = new AbstractTask() {
            {
                this.serviceAccount = serviceAccountKey;
                this.scopes = Property.ofValue(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }
        };

        GoogleCredentials credentials = task.credentials(runContext);

        assertThat(credentials, instanceOf(ServiceAccountCredentials.class));
        assertThat(task.getProjectId(), is(Property.ofValue("my-project")));
    }

    @Test
    void keepsExplicitProjectId() throws Exception {
        RunContext runContext = runContextFactory.of();
        Property<String> serviceAccountKey = Property.ofValue(serviceAccountJson());
        AbstractTask task = new AbstractTask() {
            {
                this.projectId = Property.ofValue("explicit-project");
                this.serviceAccount = serviceAccountKey;
            }
        };

        task.credentials(runContext);

        assertThat(task.getProjectId(), is(Property.ofValue("explicit-project")));
    }

    private static String serviceAccountJson() throws Exception {
        String encodedKey = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
            .encodeToString(generateRsaKeyPair().getPrivate().getEncoded());
        String pem = "-----BEGIN PRIVATE KEY-----\n" + encodedKey + "\n-----END PRIVATE KEY-----\n";

        Map<String, Object> key = new LinkedHashMap<>();
        key.put("type", "service_account");
        key.put("project_id", "my-project");
        key.put("private_key_id", "private-key-id");
        key.put("private_key", pem);
        key.put("client_email", "test@my-project.iam.gserviceaccount.com");
        key.put("client_id", "client-id");
        key.put("token_uri", "https://oauth2.googleapis.com/token");

        return JacksonMapper.ofJson().writeValueAsString(key);
    }

    private static KeyPair generateRsaKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        return keyPairGenerator.generateKeyPair();
    }
}
