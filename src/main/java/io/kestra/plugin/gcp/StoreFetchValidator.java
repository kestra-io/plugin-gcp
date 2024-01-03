package io.kestra.plugin.gcp;

import io.kestra.plugin.gcp.bigquery.QueryInterface;
import io.kestra.plugin.gcp.bigquery.StoreFetchValidation;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.validation.validator.constraints.ConstraintValidator;
import io.micronaut.validation.validator.constraints.ConstraintValidatorContext;
import jakarta.inject.Singleton;

@Singleton
@Introspected
public class StoreFetchValidator implements ConstraintValidator<StoreFetchValidation, QueryInterface> {
    @Override
    public boolean isValid(
        @Nullable QueryInterface value,
        @NonNull AnnotationValue<StoreFetchValidation> annotationMetadata,
        @NonNull ConstraintValidatorContext context) {
        if (value == null) {
            return true; // nulls are allowed according to spec
        }

        if ((value.isFetch() || value.isFetchOne()) && value.isStore()) {
            return false;
        }

        return true;
    }
}