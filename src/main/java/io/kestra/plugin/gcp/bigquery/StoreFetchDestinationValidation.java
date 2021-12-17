package io.kestra.plugin.gcp.bigquery;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import javax.validation.Constraint;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = { })
@Inherited
public @interface StoreFetchDestinationValidation {
    String message() default "Invalid store & fetch or fetchOne properties with destinationTable, you can't fetch data when destinationTable is defined";
}
