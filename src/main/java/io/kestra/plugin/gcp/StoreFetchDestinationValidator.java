package io.kestra.plugin.gcp;

import io.kestra.plugin.gcp.bigquery.Query;
import io.kestra.plugin.gcp.bigquery.StoreFetchDestinationValidation;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class StoreFetchDestinationValidator implements ConstraintValidator<StoreFetchDestinationValidation, Query> {

    @Override
    public boolean isValid(Query value, ConstraintValidatorContext context) {
        if (value == null) {
            return true; // nulls are allowed according to spec
        }

        if ((value.isFetch() || value.isFetchOne() || value.isStore()) && value.getDestinationTable() != null) {
            return false;
        }

        return true;
    }
}
