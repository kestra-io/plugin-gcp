package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class DeleteDataset extends Task implements RunnableTask<DeleteDataset.Output> {
    @NotNull
    private String name;
    private String projectId;
    private Boolean deleteContents;


    @Override
    public Output run(RunContext runContext) throws Exception {
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

        return Output
            .builder()
            .dataset(name)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        private String dataset;
    }
}
