package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.JobInfo;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;

public interface AbstractJobInterface extends JobInterface {
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
}
