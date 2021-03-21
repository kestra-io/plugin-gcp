package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a dataset or update if it already exists."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a dataset if not exits",
            code = {
                "name: \"my_dataset\"",
                "location: \"EU\"",
                "ifExists: \"SKIP\""
            }
        )
    }
)
public class CreateDataset extends AbstractDataset implements RunnableTask<AbstractDataset.Output> {
    @Builder.Default
    @Schema(
        title = "Policy to apply if a dataset already exists."
    )
    private final IfExists ifExists = IfExists.ERROR;

    @Override
    public AbstractDataset.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();
        DatasetInfo datasetInfo = this.datasetInfo(runContext);

        logger.debug("Creating dataset '{}'", datasetInfo);

        Dataset dataset = this.create(connection, runContext, datasetInfo);

        return AbstractDataset.Output.of(dataset);
    }

    private Dataset create(BigQuery connection, RunContext runContext, DatasetInfo datasetInfo) throws IllegalVariableEvaluationException {
        Dataset dataset;
        try {
            dataset = connection.create(datasetInfo);
        } catch (BigQueryException exception) {
            boolean exists = exception.getCode() == 409;
            if (!exists) {
                throw exception;
            }

            if (this.ifExists == IfExists.UPDATE) {
                dataset = connection.update(datasetInfo);
            } else if (this.ifExists == IfExists.SKIP) {
                dataset = connection.getDataset(runContext.render(this.name));
            } else {
                throw exception;
            }
        }

        return dataset;
    }

    public enum IfExists {
        ERROR,
        UPDATE,
        SKIP
    }
}
