package io.kestra.plugin.gcp;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageOptions;
import io.grpc.ManagedChannelBuilder;
import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Base class for GCS, Pub/Sub, and Firestore tests against the floci-gcp local emulator.
 *
 * Two fixed ports are required:
 *   - 4588: floci-gcp emulator (GCS, Pub/Sub, Firestore) — started via docker run if not already up
 *   - 4589: WireMock OAuth2 stub — intercepts token_uri calls from ServiceAccountCredentials
 *           so no real Google token exchange ever happens
 *
 * The floci-gcp container is started via direct docker CLI instead of TestContainers because
 * the TestContainers docker-java client uses API version 1.32, which is below the minimum (1.40)
 * accepted by the Docker Engine on this machine.
 *
 * Concrete test classes must carry @KestraTest; this class only manages infrastructure lifecycle.
 */
public abstract class FlociGcpTest {

    public static final String PROJECT_ID = "floci-local";
    public static final String GCS_BUCKET = "kestra-unit-test";

    private static final int EMULATOR_PORT = 4588;
    private static final int OAUTH2_STUB_PORT = 4589;
    private static final String EMULATOR_IMAGE = "floci/floci-gcp:latest";

    /**
     * Fake RSA service-account JSON.
     * token_uri points to the local WireMock stub so ServiceAccountCredentials never calls Google.
     * The private key is a real RSA-2048 PKCS8 key required for JWT signing (local operation).
     * The emulator ignores all auth headers, so the fake token is never validated.
     */
    public static final Property<String> SERVICE_ACCOUNT = Property.ofValue("""
        {
          "type": "service_account",
          "project_id": "floci-local",
          "private_key_id": "fake-key-id",
          "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDC4ncgPNoq8ftH\\n/OQFNqAP9rXDbKe8t2ZrreiB/pzpsCHitg/cZ1tUsWWX0Lc2a9R/436uT4sp4Lmh\\n0/boTsuHBQ6qUiZ1rYeMBaCJbQbMvMzzSRMFJ4DV/Ea93ZldNkqLpbY/TgZWatBw\\ncQsPI8Z7M3iMpuzxjjBqd0jeNjwnHldyF6xQbKSmiL6rFsCZ5k2UbnSvD1AbaZxj\\n2tTJ24HOUMujKj6bImA2i91IK5mrMA0NHCoS4U/A4QdG9KWmBFsNX5W/lbfjGmA9\\n2NvwAP6191pHsGcmhsa+V99X/y5mHj/bJkliFOxTnA5q0u62PYU2x6pVY1aTWD2V\\n6A4B5q0FAgMBAAECggEACvAhMoO0oc6e3vpgWLPC33jmPvEuwVNKO6BXplAowdIo\\nAx/BaFch7T3fQo2TphZlDcZ/DWaBtfIa4rlqwfAf0TsYVYV/cU3+urFBwn5svS0z\\n+mWidfQZJMMTiPcfWZF0aD3lRDLrY4p8ND6YzQE7hx4RvCDN6xYJcDhNgTDqRsgY\\nBq/s1B5F84jTja0bvzDq4IhXD3jamiHmQQ0w6yEreAo5NLR24L8I1N7AaiU5SfpJ\\n2xNFCBVSrmWi4CN/aTSKQ+3NLGTlpLrdwlz4G55nqDBEMMkR7bpoLP6Gx0X5Aao6\\n1+UGXABiXZ/vJ3y9dj765guBjRjAdKuLlDfDHtOt4wKBgQD7na6i8gJv+Knt8U+/\\ni7OqrFBhIZZQCwtfSVlzqJzBU74BJyde+66maMXrXVSn6ipi3zIYftFmDkKz7YFT\\nkjr4WlbAHqnhzEEk5FbE6+M3hUdo/D8l5Nh3k/WsiFeICntZRNpPQxxDVCRfHlUV\\n42lxADlRawjb0sWqcDDNMCSGEwKBgQDGR7yNZiIR+jaZbi7wcU6TMCnaTQ1lxkSH\\nxWCxDdXR7Rp1+/matrl8GvxX5gjkWXSytSIfNXRkoEyZa/qpxQ/AIs9o11zDKfIF\\n7n5HKhSu8yslMJn36HRgSVOQZhQ+d6PIttX0/y0cauXkxqKnUeV5pkfGnyAcizD5\\nmOPAR99DhwKBgQCp8U5Kb/qFdgYP17RtQwYOeGOxtuW3Gj6MFRZ9r5xwVwc18CP/\\nWy4S5yEGXvsWjmoibW2AbecwbuFOdVOsBlAd/aYqDIvhHfvB1xdj2Y6VqUcZ+YUN\\nKwupeB2uckfscmftWzu33TPxpZsLQ4lkRzyoPeZ4vzo0fp9TBoNvktyYUQKBgHur\\nSqM2zJFB6sQPwR8ezM9o/vG1lWGhJCU6qnBEHNTuec6U9r3UsiQCANoiE/G5Cdxc\\ntYeZo5sPkDcw7grtakGAdLUDfkwL4XRpqEFisbvc11A+3AmP5uYXVhN+V6oOnQ0X\\nXKOOdOiAlBr4+YI6xlH1sFbl8PVcq5NCFOtc6JgJAoGASz7EjZHJ3sZX/VcjevhC\\nZJ39nU6DDnaUzj7/VBmAXa+zDbGZSb5b70nErb2kP6wkAvVyid57rEVa1o5Z6EDz\\nG8lmRl8BplxkIfwaeypA/FtecseEgQ2J660uqkgMcvCqcRIhvK2Yjjg4O9ZWkXcz\\nejwBLfBOgtxdVxxewqCcq9I=\\n-----END PRIVATE KEY-----\\n",
          "client_email": "test@floci-local.iam.gserviceaccount.com",
          "client_id": "123456789",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "http://localhost:4589/token"
        }
        """);

    private static final AtomicBoolean started = new AtomicBoolean(false);

    // Shared gRPC channel for the emulator admin clients — created once per JVM, never closed.
    // Closing it while any admin client is still open would break subsequent test classes.
    private static volatile io.grpc.ManagedChannel emulatorAdminChannel;

    @BeforeAll
    static synchronized void startEmulator() {
        if (started.compareAndSet(false, true)) {
            try {
                startOAuth2Stub();
                startFlociContainer();
                ensureGcsBucket();
            } catch (Exception e) {
                started.set(false);
                throw new RuntimeException("Failed to start floci-gcp emulator infrastructure", e);
            }
        }
    }

    private static void startOAuth2Stub() {
        // Guard against re-binding when the JVM re-uses the same process across test class runs.
        try {
            var conn = (HttpURLConnection) new URL("http://localhost:" + OAUTH2_STUB_PORT + "/token").openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(500);
            conn.connect();
            // Port is already responding — WireMock is already running; skip re-start.
            conn.disconnect();
            return;
        } catch (IOException ignored) {
            // Port not yet listening — proceed with startup.
        }

        var stub = new WireMockServer(WireMockConfiguration.options().port(OAUTH2_STUB_PORT));
        stub.start();
        stub.stubFor(post(urlPathEqualTo("/token"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                    {
                      "access_token": "fake-emulator-token",
                      "token_type": "Bearer",
                      "expires_in": 3600
                    }
                    """)));
    }

    /**
     * Starts the floci-gcp emulator via `docker run` if it is not already listening on port 4588.
     * We bypass TestContainers here because its bundled docker-java sends Docker API version 1.32,
     * which is rejected by the Docker Engine on this machine (minimum supported: 1.40).
     */
    private static void startFlociContainer() throws IOException, InterruptedException {
        if (isEmulatorHealthy()) {
            return;
        }

        var process = new ProcessBuilder(
            "docker", "run", "-d", "--rm",
            "-p", EMULATOR_PORT + ":" + EMULATOR_PORT,
            EMULATOR_IMAGE
        ).redirectErrorStream(true).start();

        boolean exited = process.waitFor(30, TimeUnit.SECONDS);
        if (!exited || process.exitValue() != 0) {
            var output = new String(process.getInputStream().readAllBytes());
            throw new IllegalStateException(
                "docker run failed (exited=%s, code=%s): %s".formatted(
                    exited, exited ? process.exitValue() : "timeout", output
                )
            );
        }

        // Wait up to 60 seconds for the /health endpoint to respond.
        var deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (isEmulatorHealthy()) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("floci-gcp emulator did not become healthy within 60 seconds");
    }

    private static boolean isEmulatorHealthy() {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL("http://localhost:" + EMULATOR_PORT + "/health").openConnection();
            conn.setConnectTimeout(1_000);
            conn.setReadTimeout(1_000);
            conn.connect();
            return conn.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Creates the shared GCS test bucket against the emulator.
     * Skipped when STORAGE_EMULATOR_HOST is not set — the bucket is only needed by emulator-backed tests.
     */
    private static void ensureGcsBucket() {
        var emulatorHost = System.getenv("STORAGE_EMULATOR_HOST");
        if (emulatorHost == null || emulatorHost.isBlank()) {
            return;
        }
        var storage = StorageOptions.newBuilder()
            .setCredentials(com.google.cloud.NoCredentials.getInstance())
            .setProjectId(PROJECT_ID)
            .setHost(emulatorHost)
            .build()
            .getService();
        if (storage.get(GCS_BUCKET) == null) {
            storage.create(BucketInfo.of(GCS_BUCKET));
        }
    }

    /**
     * Returns a lazily-initialized shared gRPC channel targeting the Pub/Sub emulator.
     * Shared across all admin client calls to avoid per-call channel leaks.
     */
    private static synchronized FixedTransportChannelProvider emulatorChannelProvider() {
        if (emulatorAdminChannel == null) {
            var host = System.getenv("PUBSUB_EMULATOR_HOST");
            emulatorAdminChannel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
        }
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(emulatorAdminChannel));
    }

    /**
     * Creates a TopicAdminClient configured for the local Pub/Sub emulator.
     * The Java Pub/Sub SDK does not read PUBSUB_EMULATOR_HOST automatically.
     */
    public static TopicAdminClient topicAdminClient() throws IOException {
        var emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
        if (emulatorHost != null && !emulatorHost.isBlank()) {
            return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                    .setTransportChannelProvider(emulatorChannelProvider())
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build()
            );
        }
        return TopicAdminClient.create();
    }

    /**
     * Creates a SubscriptionAdminClient configured for the local Pub/Sub emulator.
     * The Java Pub/Sub SDK does not read PUBSUB_EMULATOR_HOST automatically.
     */
    public static SubscriptionAdminClient subscriptionAdminClient() throws IOException {
        var emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
        if (emulatorHost != null && !emulatorHost.isBlank()) {
            return SubscriptionAdminClient.create(
                SubscriptionAdminSettings.newBuilder()
                    .setTransportChannelProvider(emulatorChannelProvider())
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build()
            );
        }
        return SubscriptionAdminClient.create();
    }
}
