package io.kestra.plugin.gcp.bigquery;

import java.util.List;

import com.google.cloud.bigquery.BigQueryError;

import lombok.Getter;

@Getter
public class BigQueryException extends Exception {
    private final List<BigQueryError> errors;

    BigQueryException(List<BigQueryError> errors) {
        super(
            "Bigquery Errors\n[ - " +
                String.join("\n - ", errors.stream().map(BigQueryError::toString).toArray(String[]::new)) +
                "\n]"
        );

        this.errors = errors;
    }
}
