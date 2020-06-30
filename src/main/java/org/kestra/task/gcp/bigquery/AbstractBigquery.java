package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBigquery extends Task {
    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    protected String projectId;

    @InputProperty(
        description = "The geographic location where the dataset should reside",
        body = "This property is experimental\n" +
            " and might be subject to change or removed.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset Location</a>",
        dynamic = true
    )
    protected String location;

    protected BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException {
        return new Connection().of(
            runContext.render(this.projectId),
            runContext.render(this.location)
        );
    }
}
