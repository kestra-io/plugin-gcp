package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
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
@Example(
    title = "Delete a dataset",
    code = {
        "name: \"my-bucket\"",
        "deleteContents: true"
    }
)
@Documentation(
    description = "Delete a dataset."
)
public class DeleteDataset extends Task implements RunnableTask<DeleteDataset.Output> {
    @NotNull
    @InputProperty(
        description = "The dataset's user-defined id",
        dynamic = true
    )
    private String name;

    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    private String projectId;


    @InputProperty(
        description = "Whether to delete a dataset even if non-empty",
        body = "If not provided, attempting to\n" +
            " delete a non-empty dataset will result in a exception being thrown."
    )
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
        @NotNull
        @OutputProperty(
            description = "The dataset's user-defined id"
        )
        private String dataset;
    }
}
