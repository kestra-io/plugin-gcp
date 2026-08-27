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
        this(errors, null);
    }

    /** Keeps the originating exception as the cause: a transport failure has nothing in its error list. */
    BigQueryException(List<BigQueryError> errors, Throwable cause) {
        super(formatErrors(Objects.requireNonNullElse(errors, List.of())), cause);
        this.errors = Objects.requireNonNullElse(errors, List.of());
    }

    private static String formatErrors(List<BigQueryError> errors) {
        return "Bigquery Errors\n[ - " +
            errors.stream()
                .map(BigQueryError::toString)
                .collect(Collectors.joining("\n - "))
            +
            "\n]";
    }
}
