package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.JobInfo;
import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import java.util.Map;

public interface AbstractJobInterface {
    @Schema(
        title = "The table where to put query results.",
        description = "If not provided, a new table is created."
    )
    @PluginProperty(dynamic = true)
    String getDestinationTable();

    @Schema(
        title = "The action that should occur if the destination table already exists."
    )
    @PluginProperty
    JobInfo.WriteDisposition getWriteDisposition();

    @Schema(
        title = "Whether the job is allowed to create tables."
    )
    @PluginProperty
    JobInfo.CreateDisposition getCreateDisposition();

    @Schema(
        title = "Job timeout.",
        description = "If this time limit is exceeded, BigQuery may attempt to terminate the job."
    )
    @PluginProperty
    Duration getJobTimeout();

    @Schema(
        title = "The labels associated with this job.",
        description = "You can use these to organize and group your jobs. Label " +
            "keys and values can be no longer than 63 characters, can only contain lowercase letters, " +
            "numeric characters, underscores and dashes. International characters are allowed. Label " +
            "values are optional. Label keys must start with a letter and each label in the list must have " +
            "a different key."
    )
    @PluginProperty(dynamic = true)
    Map<String, String> getLabels();

    @Schema(
        title = "Whether the job has to be dry run or not.",
        description = " A valid query will mostly return an empty response with some processing statistics, " +
            "while an invalid query will return the same error as it would if it were an actual run."
    )
    @PluginProperty
    Boolean getDryRun();
}
