package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.util.Map;

import com.google.cloud.bigquery.JobInfo;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface AbstractJobInterface {
    @Schema(
        title = "Destination table",
        description = "Table to receive job output; creation depends on createDisposition"
    )
    @PluginProperty(group = "advanced")
    Property<String> getDestinationTable();

    @Schema(
        title = "Write disposition",
        description = "Action when destination exists"
    )
    Property<JobInfo.WriteDisposition> getWriteDisposition();

    @Schema(
        title = "Create disposition",
        description = "Whether the job may create the destination table"
    )
    Property<JobInfo.CreateDisposition> getCreateDisposition();

    @Schema(
        title = "Job timeout",
        description = "Optional max duration; BigQuery may terminate the job when exceeded"
    )
    @PluginProperty(group = "execution")
    Property<Duration> getJobTimeout();

    @Schema(
        title = "Job labels"
    )
    @PluginProperty(group = "advanced")
    Property<Map<String, String>> getLabels();

    @Schema(
        title = "Dry run",
        description = "If true, validates the job and returns statistics without running it"
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getDryRun();
}
