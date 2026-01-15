package io.kestra.plugin.gcp.dataform;

import com.google.cloud.dataform.v1.DataformClient;
import com.google.cloud.dataform.v1.DataformSettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.api.gax.core.FixedCredentialsProvider;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import jakarta.validation.constraints.NotNull;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDataForm extends AbstractTask {
    @Schema(
        title = "The GCP location where your Dataform repository is hosted",
        example = "us-central1"
    )
    @NotNull
    @PluginProperty
    protected Property<String> location;

    @Schema(
        title = "The Dataform repository ID (not the full path)",
        description = "Used to construct `projects/{projectId}/locations/{location}/repositories/{repositoryId}`"
    )
    @NotNull
    @PluginProperty
    protected Property<String> repositoryId;

    protected DataformClient dataformClient(RunContext runContext) throws Exception {
        return createClient(runContext);
    }

    /**
     * Creates a DataformClient with the current run context credentials.
     */
    protected DataformClient createClient(RunContext runContext)
        throws IOException, IllegalVariableEvaluationException {

        GoogleCredentials credentials = this.credentials(runContext);
        DataformSettings settings = DataformSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build();

        return DataformClient.create(settings);
    }

    protected String buildRepositoryPath(RunContext runContext) throws IllegalVariableEvaluationException {
        return String.format(
            "projects/%s/locations/%s/repositories/%s",
            runContext.render(this.projectId).as(String.class).orElse(null),
            runContext.render(this.location).as(String.class).orElseThrow(),
            runContext.render(this.repositoryId).as(String.class).orElseThrow()
        );
    }
}