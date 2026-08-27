package io.kestra.plugin.gcp.auth;

import java.util.Date;
import java.util.List;

import com.google.auth.oauth2.AccessToken;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.EncryptedString;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Fetch a GCP OAuth access token",
    description = "Refreshes and returns an OAuth access token for the configured service account and scopes, for use by downstream tasks or external systems."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch a GCP OAuth access token",
            full = true,
            code = """
                id: gcp_oauth_access_token
                namespace: company.team

                tasks:
                  - id: token
                    type: io.kestra.plugin.gcp.auth.OauthAccessToken
                    projectId: my-gcp-project
                """
        )
    }
)
public class OauthAccessToken extends AbstractTask implements RunnableTask<OauthAccessToken.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        AccessToken accessToken = this.credentials(runContext)
            .createScoped(runContext.render(this.scopes).asList(String.class))
            .refreshAccessToken();

        var output = AccessTokenOutput.builder()
            .expirationTime(accessToken.getExpirationTime())
            .scopes(accessToken.getScopes())
            .tokenValue(EncryptedString.from(accessToken.getTokenValue(), runContext));

        return Output
            .builder()
            .accessToken(output.build())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @NotNull
        @Schema(title = "An OAuth access token for the configured service account")
        private final AccessTokenOutput accessToken;
    }

    @Builder
    @Getter
    public static class AccessTokenOutput {
        List<String> scopes;

        @Schema(
            title = "OAuth access token value",
            description = "Will be automatically encrypted and decrypted in the outputs if encryption is configured"
        )
        EncryptedString tokenValue;

        Date expirationTime;
    }
}
