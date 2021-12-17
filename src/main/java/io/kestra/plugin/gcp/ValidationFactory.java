package io.kestra.plugin.gcp;

import io.kestra.plugin.gcp.bigquery.*;
import io.micronaut.context.annotation.Factory;
import io.micronaut.validation.validator.constraints.ConstraintValidator;

import jakarta.inject.Singleton;

@Factory
public class ValidationFactory {
    @Singleton
    ConstraintValidator<StoreFetchValidation, QueryInterface> storeFetchValidationValidator() {
        return (value, annotationMetadata, context) -> {
            if (value == null) {
                return true; // nulls are allowed according to spec
            }

            if ((value.isFetch() || value.isFetchOne()) && value.isStore()) {
                return false;
            }

            return true;
        };
    }

    @Singleton
    ConstraintValidator<StoreFetchDestinationValidation, Query> storeFetchDestinationValidationValidator() {
        return (value, annotationMetadata, context) -> {
            if (value == null) {
                return true; // nulls are allowed according to spec
            }

            if ((value.isFetch() || value.isFetchOne() || value.isStore()) && value.getDestinationTable() != null) {
                return false;
            }

            return true;
        };
    }

    @Singleton
    ConstraintValidator<LoadCsvValidation, AbstractLoad> loadCsvValidationValidator() {
        return (value, annotationMetadata, context) -> {
            if (value == null) {
                return true; // nulls are allowed according to spec
            }

            if (value.getFormat() == AbstractLoad.Format.CSV && value.getCsvOptions() == null) {
                return false;
            }

            return true;
        };
    }
}
