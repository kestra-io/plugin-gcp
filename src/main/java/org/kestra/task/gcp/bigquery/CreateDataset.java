package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
import org.slf4j.Logger;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class CreateDataset extends AbstractDataset implements RunnableTask {
    @Builder.Default
    private IfExists ifExists = IfExists.ERROR;

    @Override
    public RunOutput run(RunContext runContext) throws Exception {
        BigQuery connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
        DatasetInfo datasetInfo = this.datasetInfo(runContext);

        logger.debug("Creating dataset '{}'", datasetInfo);

        Dataset dataset = this.create(connection, runContext, datasetInfo);

        return RunOutput
            .builder()
            .outputs(this.outputs(dataset))
            .build();
    }

    private Dataset create(BigQuery connection, RunContext runContext, DatasetInfo datasetInfo) throws IOException {
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
