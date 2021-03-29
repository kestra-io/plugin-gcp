package io.kestra.plugin.gcp.bigquery;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import javax.validation.Constraint;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = { })
public @interface LoadCsvValidation {
    String message() default "missing csv option with CSV fileType";
}
