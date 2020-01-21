package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class DeleteDataset extends Task implements RunnableTask {
    @NotNull
    private String name;
    private String projectId;
    private Boolean deleteContents;


    @Override
    public RunOutput run(RunContext runContext) throws Exception {
        BigQuery connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
        String name = runContext.render(this.name);

        logger.debug("Deleting dataset '{}'", name);

        boolean delete;

        if (deleteContents) {
            delete = connection.delete(name, BigQuery.DatasetDeleteOption.deleteContents());
        } else {
            delete = connection.delete(name);
        }

        if (!delete) {
            throw new BigQueryException(404, "Couldn't find dataset '" + name + "'");
        }

        return RunOutput
            .builder()
            .outputs(ImmutableMap.of("dataset", name))
            .build();
    }
}
