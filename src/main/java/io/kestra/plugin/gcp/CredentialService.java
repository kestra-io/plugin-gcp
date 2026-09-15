package io.kestra.plugin.gcp;

import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

/**
 * @deprecated moved to {@link io.kestra.plugin.gcp.shared.CredentialService}; kept for backward compatibility, will be removed in the next major.
 */
@Deprecated(since = "2.12.0", forRemoval = true)
@SuppressWarnings("removal")
public final class CredentialService {
    private CredentialService() {
    }

    public static GoogleCredentials credentials(RunContext runContext, GcpInterface gcpInterface)
        throws IllegalVariableEvaluationException, IOException {
        return io.kestra.plugin.gcp.shared.CredentialService.connection(runContext, gcpInterface).credentials();
    }
}
