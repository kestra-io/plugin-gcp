package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.util.Map;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;

public interface JobInterface {
    @Schema(
        title = "Job timeout",
        description = """
            Optional max duration; BigQuery may terminate the job when exceeded
            """
    )
    @PluginProperty(group = "execution")
    Property<Duration> getJobTimeout();

    @Schema(
        title = "Job labels",
        description = """
            Labels to apply to the BigQuery job
            """
    )
    @PluginProperty(group = "advanced")
    Property<Map<String, String>> getLabels();

    @Schema(
        title = "Dry run",
        description = """
            If true, validates the job without creating or updating tables
            """
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getDryRun();
}
