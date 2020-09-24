package org.kestra.task.gcp.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Fetch an OAuth access token."
)
public class OauthAccessToken extends Task implements RunnableTask<OauthAccessToken.Output> {
    @InputProperty(
        description = "The scopes requested for the access token.",
        body = {
            "Full list can be found [here](https://developers.google.com/identity/protocols/oauth2/scopes)",
            "",
            "default is `[\"https://www.googleapis.com/auth/cloud-platform\"]"
        },
        dynamic = true
    )
    @NotEmpty
    private final List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");


    @Override
    public Output run(RunContext runContext) throws Exception {
        AccessToken accessToken = GoogleCredentials
            .getApplicationDefault()
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
        @OutputProperty(
            description = "A oauth access token for the current user"
        )
        private final AccessToken accessToken;
    }
}
