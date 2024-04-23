package io.kestra.plugin.gcp.bigquery;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import io.kestra.plugin.gcp.StoreFetchValidator;
import jakarta.validation.Constraint;
import jakarta.validation.Payload;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = StoreFetchValidator.class)
@Inherited
public @interface StoreFetchValidation {
    String message() default "Invalid store with fetch or fetchOne properties, you can't have both defined.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
