package io.kestra.plugin.gcp.spanner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.cloud.spanner.*;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.GcpInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSpanner extends Task implements GcpInterface, SpannerConnectionInterface {

    @NotNull
    @Schema(title = "The GCP project ID")
    @PluginProperty(group = "connection")
    protected Property<String> projectId;

    @Schema(title = "The GCP service account")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> serviceAccount;

    @Schema(title = "The GCP service account to impersonate")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> impersonatedServiceAccount;

    @Schema(title = "The GCP scopes to be used")
    @PluginProperty(group = "advanced")
    protected Property<List<String>> scopes;

    @NotNull
    @Schema(title = "The Spanner instance ID")
    @PluginProperty(group = "connection")
    protected Property<String> instanceId;

    @NotNull
    @Schema(title = "The Spanner database ID")
    @PluginProperty(group = "connection")
    protected Property<String> databaseId;

    @Schema(
        title = "Spanner emulator host (`host:port`), for local testing only",
        description = "When set, the task connects to a local Spanner emulator instead of the real Spanner service."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> emulatorHost;

    protected Spanner spannerClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SpannerService.spannerClient(runContext, this);
    }

    protected DatabaseId databaseId(RunContext runContext) throws IllegalVariableEvaluationException {
        return SpannerService.databaseId(runContext, this);
    }

    protected void bindParameter(Statement.Builder builder, String name, Object value) {
        SpannerService.bindParameter(builder, name, value);
    }

    protected Map<String, Object> rowToMap(Struct row) {
        return SpannerService.rowToMap(row);
    }
}
