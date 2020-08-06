package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import lombok.Getter;

import java.util.List;

@Getter
public class BigQueryException extends Exception {
    private BigQueryError error;
    private List<BigQueryError> executionsErrors;

    BigQueryException(BigQueryError error, List<BigQueryError> executionsErrors) {
        super(error.toString());

        this.error = error;
        this.executionsErrors = executionsErrors;
    }

    BigQueryException(BigQueryError error) {
        super(error.toString());

        this.error = error;
    }
}
