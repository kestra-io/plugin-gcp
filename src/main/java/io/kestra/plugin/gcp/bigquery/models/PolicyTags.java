package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Getter
@Builder
@Jacksonized
public class PolicyTags {
    @Schema(
        name = "The policy tags' names."
    )
    private final Property<List<String>> names;

    public static PolicyTags.Output of(com.google.cloud.bigquery.PolicyTags policyTags) {
        return PolicyTags.Output.builder()
            .names(policyTags.getNames())
            .build();
    }

    public com.google.cloud.bigquery.PolicyTags to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.bigquery.PolicyTags.newBuilder()
            .setNames(runContext.render(this.names).asList(String.class))
            .build();
    }

    @Getter
    @Builder
    public static class Output {
        private final List<String> names;
    }
}
