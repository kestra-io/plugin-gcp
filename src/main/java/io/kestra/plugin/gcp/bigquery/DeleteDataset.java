package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Delete a dataset.",
            full = true,
            code = """
                id: gcp_bq_delete_dataset
                namespace: company.team

                tasks:
                  - id: delete_dataset
                    type: io.kestra.plugin.gcp.bigquery.DeleteDataset
                    name: "my-dataset"
                    deleteContents: true
                """
        )
    }
)
@Schema(
    title = "Delete a dataset."
)
public class DeleteDataset extends AbstractBigquery implements RunnableTask<DeleteDataset.Output> {
    @NotNull
    @Schema(
        title = "The dataset's user-defined id."
    )
    private Property<String> name;

    @Schema(
        title = "Whether to delete a dataset even if non-empty.",
        description = "If not provided, attempting to" +
            " delete a non-empty dataset will result in a exception being thrown."
    )
    private Property<Boolean> deleteContents;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();
        String name = runContext.render(this.name).as(String.class).orElseThrow();

        logger.debug("Deleting dataset '{}'", name);

        boolean delete;

        if (runContext.render(deleteContents).as(Boolean.class).orElse(false)) {
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
