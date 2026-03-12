package io.kestra.plugin.gcp.bigquery;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.BigQueryError;

import lombok.Getter;

@Getter
public class BigQueryException extends Exception {
    private final List<BigQueryError> errors;

    BigQueryException(List<BigQueryError> errors) {
        super(formatErrors(Objects.requireNonNullElse(errors, List.of())));
        this.errors = Objects.requireNonNullElse(errors, List.of());
    }

    private static String formatErrors(List<BigQueryError> errors) {
        return "Bigquery Errors\n[ - " +
            errors.stream()
                .map(BigQueryError::toString)
                .collect(Collectors.joining("\n - ")) +
            "\n]";
    }
}
