package io.kestra.plugin.gcp;

import io.kestra.plugin.gcp.bigquery.QueryInterface;
import io.kestra.plugin.gcp.bigquery.StoreFetchValidation;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class StoreFetchValidator implements ConstraintValidator<StoreFetchValidation, QueryInterface> {

    @Override
    public boolean isValid(QueryInterface value, ConstraintValidatorContext context) {
        if (value == null) {
            return true; // nulls are allowed according to spec
        }

        if ((value.isFetch() || value.isFetchOne()) && value.isStore()) {
            return false;
        }

        return true;
    }
}