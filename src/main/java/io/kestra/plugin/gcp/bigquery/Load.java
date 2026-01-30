package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Load an csv file from an input file",
            full = true,
            code = """
                id: gcp_bq_load
                namespace: company.team

                tasks:
                  - id: load
                    type: io.kestra.plugin.gcp.bigquery.Load
                    from: "{{ inputs.file }}"
                    destinationTable: "my_project.my_dataset.my_table"
                    format: CSV
                    csvOptions:
                      fieldDelimiter: ";"
                """
        )
    },
    metrics = {
        @Metric(name = "bad.records", type = Counter.TYPE, unit = "records", description= "the number of bad records reported in a job."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The time it took for the task to run."),
        @Metric(name = "input.bytes", type = Counter.TYPE, unit = "bytes", description = "The number of bytes of source data in a load job."),
        @Metric(name = "input.files", type = Counter.TYPE, unit = "files", description = "The number of source files in a load job."),
        @Metric(name = "output.bytes", type = Counter.TYPE, unit = "bytes", description = "The size of the data loaded by a load job so far, in bytes."),
        @Metric(name = "output.rows", type = Counter.TYPE, unit = "records", description = "The number of rows loaded by a load job so far.")
    }
)
@Schema(
    title = "Load an internal file into BigQuery",
    description = "Streams a file from Kestra internal storage to a BigQuery load job. Fails on empty files by default (`failedOnEmpty` true)."
)
public class Load extends AbstractLoad implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "Source file URI",
        description = "Kestra internal storage URI for the file to load"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Fail on empty file",
        description = "If true (default), raises an error when the input file has zero bytes"
    )
    @Builder.Default
    private Property<Boolean> failedOnEmpty = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        WriteChannelConfiguration.Builder builder = WriteChannelConfiguration
            .newBuilder(BigQueryService.tableId(runContext.render(this.destinationTable).as(String.class).orElse(null)));

        this.setOptions(builder, runContext);

        WriteChannelConfiguration configuration = builder.build();
        logger.debug("Starting load\n{}", JacksonMapper.log(configuration));

        URI from = new URI(runContext.render(this.from).as(String.class).orElse(null));
        try (InputStream data = runContext.storage().getFile(from)) {
            long byteWritten = 0L;

            TableDataWriteChannel writer = connection.writer(configuration);
            try (OutputStream stream = Channels.newOutputStream(writer)) {
                byte[] buffer = new byte[10_240];

                int limit;
                while ((limit = data.read(buffer)) >= 0) {
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                    byteWritten += limit;
                }
            }

            if (byteWritten == 0) {
                if (runContext.render(failedOnEmpty).as(Boolean.class).orElseThrow()) {
                    throw new Exception("Can't load an empty file and this one don't contain any data");
                }

                return Output.builder()
                    .rows(0L)
                    .build();
            }

            Job job = this.waitForJob(logger, writer::getJob, runContext);

            return this.outputs(runContext, configuration, job);
        }
    }
}
