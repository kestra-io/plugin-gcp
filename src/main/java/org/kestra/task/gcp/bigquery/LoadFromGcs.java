package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
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
@Documentation(
    description = "Load data from GCS (Google Cloud Storage) to BigQuery"
)
public class LoadFromGcs extends AbstractLoad implements RunnableTask<AbstractLoad.Output> {
    /**
     * Sets the fully-qualified URIs that point to source data in Google Cloud Storage (e.g.
     * gs://bucket/path). Each URI can contain one '*' wildcard character and it must come after the
     * 'bucket' name.
     */
    @InputProperty(
        description = "Google Cloud Storage source data",
        body = "The fully-qualified URIs that point to source data in Google Cloud Storage (e.g.\n" +
            " gs://bucket/path). Each URI can contain one '*' wildcard character and it must come after the\n" +
            " 'bucket' name.",
        dynamic = true
    )
    private List<String> from;

    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    private String projectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger(this.getClass());

        List<String> from = runContext.render(this.from);

        LoadJobConfiguration.Builder builder = LoadJobConfiguration
            .newBuilder(Connection.tableId(runContext.render(this.destinationTable)), from);

        this.setOptions(builder);

        LoadJobConfiguration configuration = builder.build();
        Job loadJob = connection.create(JobInfo.of(configuration));

        logger.debug("Starting query\n{}", JacksonMapper.log(configuration));

        return this.execute(runContext, logger, configuration, loadJob);
    }
}
