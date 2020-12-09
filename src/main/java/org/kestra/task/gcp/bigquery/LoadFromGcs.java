package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Load an avro file from a gcs bucket",
            code = {
                "from:",
                "  - \"{{ outputs.avro-to-gcs }}\"",
                "destinationTable: \"my_project.my_dataset.my_table\"",
                "format: AVRO",
                "avroOptions:",
                "  useAvroLogicalTypes: true"
            }
        )
    }
)
@Schema(
    title = "Load data from GCS (Google Cloud Storage) to BigQuery"
)
public class LoadFromGcs extends AbstractLoad implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "Google Cloud Storage source data",
        description = "The fully-qualified URIs that point to source data in Google Cloud Storage (e.g." +
            " gs://bucket/path). Each URI can contain one '*' wildcard character and it must come after the" +
            " 'bucket' name."
    )
    @PluginProperty(dynamic = true)
    private List<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        List<String> from = runContext.render(this.from);

        LoadJobConfiguration.Builder builder = LoadJobConfiguration
            .newBuilder(BigQueryService.tableId(runContext.render(this.destinationTable)), from);

        this.setOptions(builder, runContext);

        LoadJobConfiguration configuration = builder.build();
        logger.debug("Starting query\n{}", JacksonMapper.log(configuration));

        Job loadJob = this.waitForJob(logger, () -> connection.create(JobInfo.of(configuration)));

        return this.outputs(runContext, configuration, loadJob);
    }
}
