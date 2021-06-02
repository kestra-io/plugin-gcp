package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
        name = "the policy tags names."
    )
    @PluginProperty(dynamic = true)
    private final List<String> names;

    public static PolicyTags of(com.google.cloud.bigquery.PolicyTags policyTags) {
        return PolicyTags.builder()
            .names(policyTags.getNames())
            .build();
    }

    public com.google.cloud.bigquery.PolicyTags to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.bigquery.PolicyTags.newBuilder()
            .setNames(runContext.render(this.names))
            .build();
    }
}
