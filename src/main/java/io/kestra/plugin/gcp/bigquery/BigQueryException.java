package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import lombok.Getter;

import java.util.List;

@Getter
public class BigQueryException extends Exception {
    private final List<BigQueryError> errors;

    BigQueryException(List<BigQueryError> errors) {
        super("Bigquery Errors\n[ - " +
            String.join("\n - ", errors.stream().map(BigQueryError::toString).toArray(String[]::new)) +
            "\n]"
        );

        this.errors = errors;
    }
}
