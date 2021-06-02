package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Update a dataset."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "name: \"my_dataset\"",
                "location: \"EU\"",
                "friendlyName: \"new Friendly Name\""
            }
        )
    }
)
public class UpdateDataset extends AbstractDataset implements RunnableTask<AbstractDataset.Output> {
    @Override
    public AbstractDataset.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();
        DatasetInfo datasetInfo = this.datasetInfo(runContext);

        logger.debug("Updating dataset '{}'", datasetInfo);

        Dataset dataset = connection.update(datasetInfo);

        return AbstractDataset.Output.of(dataset);
    }
}
