package org.kestra.task.gcp.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Fetch an OAuth access token.",
    body = {

    }
)
public class OauthAccessToken extends Task implements RunnableTask<OauthAccessToken.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        AccessToken accessToken = GoogleCredentials.getApplicationDefault().refreshAccessToken();

        return Output
            .builder()
            .accessToken(accessToken)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @NotNull
        @OutputProperty(
            description = "A oauth access token for the current user"
        )
        private final AccessToken accessToken;
    }
}
