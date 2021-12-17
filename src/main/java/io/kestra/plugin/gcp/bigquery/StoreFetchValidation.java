package io.kestra.plugin.gcp.bigquery;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import javax.validation.Constraint;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = { })
@Inherited
public @interface StoreFetchValidation {
    String message() default "Invalid store with fetch or fetchOne properties, you can't have both defined.";
}
