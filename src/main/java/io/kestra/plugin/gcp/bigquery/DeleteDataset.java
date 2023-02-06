package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Delete a dataset",
            code = {
                "name: \"my-bucket\"",
                "deleteContents: true"
            }
        )
    }
)
@Schema(
    title = "Delete a dataset."
)
public class DeleteDataset extends AbstractBigquery implements RunnableTask<DeleteDataset.Output> {
    @NotNull
    @Schema(
        title = "The dataset's user-defined id"
    )
    @PluginProperty(dynamic = true)
    private String name;

    @Schema(
        title = "Whether to delete a dataset even if non-empty",
        description = "If not provided, attempting to" +
            " delete a non-empty dataset will result in a exception being thrown."
    )
    @PluginProperty
    private Boolean deleteContents;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @NotNull
        @Schema(
            title = "The dataset's user-defined id"
        )
        private String dataset;
    }
}
