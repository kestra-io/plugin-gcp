package io.kestra.plugin.gcp.auth;

import com.google.auth.oauth2.AccessToken;
import io.kestra.core.models.tasks.common.EncryptedString;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import jakarta.validation.constraints.NotNull;

import java.util.Date;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Fetch a GCP OAuth access token."
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
        @Schema(title = "An OAuth access token for the current user.")
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
