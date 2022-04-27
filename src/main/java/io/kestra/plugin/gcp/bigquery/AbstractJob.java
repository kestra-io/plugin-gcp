package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.JobInfo;
import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJob extends AbstractBigquery  {
    @Schema(
        title = "The table where to put query results",
        description = "If not provided a new table is created."
    )
    @PluginProperty(dynamic = true)
    protected String destinationTable;

    @Schema(
        title = "The action that should occur if the destination table already exists"
    )
    @PluginProperty(dynamic = false)
    protected JobInfo.WriteDisposition writeDisposition;

    @Schema(
        title = "Whether the job is allowed to create tables"
    )
    @PluginProperty(dynamic = false)
    protected JobInfo.CreateDisposition createDisposition;

    @Schema(
        title = "Job timeout.",
        description = "If this time limit is exceeded, BigQuery may attempt to terminate the job."
    )
    @PluginProperty(dynamic = false)
    protected Duration jobTimeout;

    @Schema(
        title = "The labels associated with this job.",
        description = "You can use these to organize and group your jobs. Label " +
            "keys and values can be no longer than 63 characters, can only contain lowercase letters, " +
            "numeric characters, underscores and dashes. International characters are allowed. Label " +
            "values are optional. Label keys must start with a letter and each label in the list must have " +
            "a different key."
    )
    @PluginProperty(dynamic = true)
    protected Map<String, String> labels;

    @Schema(
        title = "Sets whether the job has to be dry run or no.",
        description = " A valid query will return a mostly empty response with some processing statistics, " +
            "while an invalid query will return the same error it would if it wasn't a dry run."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    protected Boolean dryRun = false;
}
