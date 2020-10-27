package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.WriteChannelConfiguration;
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
            code = {
                "from: \"{{ inputs.file }}\"",
                "destinationTable: \"my_project.my_dataset.my_table\"",
                "format: CSV",
                "csvOptions:",
                "  fieldDelimiter: \";\""
            }
        )
    }
)
@Schema(
    title = "Load data from local file to BigQuery"
)
public class Load extends AbstractLoad implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "The fully-qualified URIs that point to source data"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        WriteChannelConfiguration.Builder builder = WriteChannelConfiguration
            .newBuilder(BigQueryService.tableId(runContext.render(this.destinationTable)));

        this.setOptions(builder, runContext);

        WriteChannelConfiguration configuration = builder.build();
        logger.debug("Starting load\n{}", JacksonMapper.log(configuration));

        URI from = new URI(runContext.render(this.from));
        InputStream data = runContext.uriToInputStream(from);

        TableDataWriteChannel writer = connection.writer(configuration);
        try (OutputStream stream = Channels.newOutputStream(writer)) {
            byte[] buffer = new byte[10_240];

            int limit;
            while ((limit = data.read(buffer)) >= 0) {
                writer.write(ByteBuffer.wrap(buffer, 0, limit));
            }
        }

        Job job = this.waitForJob(logger, writer::getJob);

        return this.outputs(runContext, configuration, job);
    }
}
