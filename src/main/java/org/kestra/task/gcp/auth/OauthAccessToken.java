package org.kestra.task.gcp.auth;

import com.google.auth.oauth2.AccessToken;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.task.gcp.AbstractTask;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Fetch an OAuth access token."
)
public class OauthAccessToken extends AbstractTask implements RunnableTask<OauthAccessToken.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        AccessToken accessToken = this.credentials(runContext)
            .createScoped(runContext.render(this.scopes))
            .refreshAccessToken();

        return Output
            .builder()
            .accessToken(accessToken)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @NotNull
        @Schema(
            title = "A oauth access token for the current user"
        )
        private final AccessToken accessToken;
    }
}
