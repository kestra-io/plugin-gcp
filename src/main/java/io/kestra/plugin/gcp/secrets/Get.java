package io.kestra.plugin.gcp.secrets;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Get a document from a collection."
)
@Plugin(
        examples = {
                @Example(
                        title = "Get secret value",
                        code = {
                                "name: \"GITHUB_TOKEN\"",
                                "version: \"latest\""
                        }
                )
        }
)
public class Get extends AbstractSecretClient implements RunnableTask<Get.Output> {
    @Schema(
            title = "The secret name"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String name;

    @Schema(
            title = "The Secret version to read"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String version;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        try (var secretClient = this.connection(runContext)) {
            var data = secretClient.getSecret(this.name).toString();
            return io.kestra.plugin.gcp.secrets.Get.Output.builder()
                    .secretValue(data)
                    .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "String containing the fetched secret value."
        )
        private String secretValue;
    }
}
