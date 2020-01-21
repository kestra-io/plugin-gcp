package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class UpdateDataset extends AbstractDataset implements RunnableTask {
    @Override
    public RunOutput run(RunContext runContext) throws Exception {
        BigQuery connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
        DatasetInfo datasetInfo = this.datasetInfo(runContext);

        logger.debug("Updating dataset '{}'", datasetInfo);

        Dataset dataset = connection.update(datasetInfo);

        return RunOutput
            .builder()
            .outputs(this.outputs(dataset))
            .build();
    }
}
