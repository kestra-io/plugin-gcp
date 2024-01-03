package io.kestra.plugin.gcp;

import io.kestra.plugin.gcp.bigquery.Query;
import io.kestra.plugin.gcp.bigquery.StoreFetchDestinationValidation;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.validation.validator.constraints.ConstraintValidator;
import io.micronaut.validation.validator.constraints.ConstraintValidatorContext;
import jakarta.inject.Singleton;

@Singleton
@Introspected
public class StoreFetchDestinationValidator implements ConstraintValidator<StoreFetchDestinationValidation, Query> {
    @Override
    public boolean isValid(
        @Nullable Query value,
        @NonNull AnnotationValue<StoreFetchDestinationValidation> annotationMetadata,
        @NonNull ConstraintValidatorContext context) {
        if (value == null) {
            return true; // nulls are allowed according to spec
        }

        if ((value.isFetch() || value.isFetchOne() || value.isStore()) && value.getDestinationTable() != null) {
            return false;
        }

        return true;
    }
}
