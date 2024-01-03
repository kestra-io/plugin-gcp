package io.kestra.plugin.gcp;

import io.kestra.plugin.gcp.bigquery.AbstractLoad;
import io.kestra.plugin.gcp.bigquery.LoadCsvValidation;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.validation.validator.constraints.ConstraintValidator;
import io.micronaut.validation.validator.constraints.ConstraintValidatorContext;
import jakarta.inject.Singleton;

@Singleton
@Introspected
public class LoadCsvValidator implements ConstraintValidator<LoadCsvValidation, AbstractLoad> {
    @Override
    public boolean isValid(
        @Nullable AbstractLoad value,
        @NonNull AnnotationValue<LoadCsvValidation> annotationMetadata,
        @NonNull ConstraintValidatorContext context) {
        if (value == null) {
            return true; // nulls are allowed according to spec
        }

        if (value.getFormat() == AbstractLoad.Format.CSV && value.getCsvOptions() == null) {
            return false;
        }

        return true;
    }
}
