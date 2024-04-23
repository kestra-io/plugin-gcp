package io.kestra.plugin.gcp;

import io.kestra.plugin.gcp.bigquery.AbstractLoad;
import io.kestra.plugin.gcp.bigquery.LoadCsvValidation;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class LoadCsvValidator implements ConstraintValidator<LoadCsvValidation, AbstractLoad> {

    @Override
    public boolean isValid(AbstractLoad value, ConstraintValidatorContext context) {
        if (value == null) {
            return true; // nulls are allowed according to spec
        }

        if (value.getFormat() == AbstractLoad.Format.CSV && value.getCsvOptions() == null) {
            return false;
        }

        return true;
    }
}
