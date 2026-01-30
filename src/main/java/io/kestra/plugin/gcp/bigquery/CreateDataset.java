package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import io.kestra.core.models.property.Property;
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
    title = "Create or update a BigQuery dataset",
    description = "Creates a dataset with optional location, labels, ACLs, and defaults. If it exists, behavior follows `ifExists` (default ERROR; UPDATE replaces metadata, SKIP returns existing)."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a dataset if not exits",
            full = true,
            code = """
                id: gcp_bq_create_dataset
                namespace: company.team

                tasks:
                  - id: create_dataset
                    type: io.kestra.plugin.gcp.bigquery.CreateDataset
                    name: "my_dataset"
                    location: "EU"
                    ifExists: "SKIP"
                """
        )
    }
)
public class CreateDataset extends AbstractDataset implements RunnableTask<AbstractDataset.Output> {
    @Builder.Default
    @Schema(
        title = "Existing dataset policy",
        description = "`ERROR` by default. Use `UPDATE` to apply new metadata or `SKIP` to leave the dataset unchanged."
    )
    private final Property<IfExists> ifExists = Property.ofValue(IfExists.ERROR);

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

            if (runContext.render(this.ifExists).as(IfExists.class).orElseThrow() == IfExists.UPDATE) {
                dataset = connection.update(datasetInfo);
            } else if (runContext.render(this.ifExists).as(IfExists.class).orElseThrow() == IfExists.SKIP) {
                dataset = connection.getDataset(runContext.render(this.name).as(String.class).orElseThrow());
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
