package io.kestra.plugin.gcp.bigquery;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import io.kestra.plugin.gcp.LoadCsvValidator;
import jakarta.validation.Constraint;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = LoadCsvValidator.class)
@Inherited
public @interface LoadCsvValidation {
    String message() default "missing csv option with CSV fileType";
}
