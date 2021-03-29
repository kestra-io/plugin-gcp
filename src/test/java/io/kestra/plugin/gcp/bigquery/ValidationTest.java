package io.kestra.plugin.gcp.bigquery;

import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;
import javax.validation.ConstraintViolationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ValidationTest {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void storeFetchValidation() {
        Query.QueryBuilder<?, ?> builder = Query.builder()
            .id("test")
            .type(Query.class.getName())
            .sql("SELECT 1");

        Query task = builder
            .store(false)
            .fetch(true)
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(task);
        assertThat(valid.isPresent(), is(false));

        task = builder
            .fetch(true)
            .store(true)
            .build();

        valid = modelValidator.isValid(task);
        assertThat(valid.isPresent(), is(true));

        task = builder
            .fetchOne(true)
            .store(true)
            .build();

        valid = modelValidator.isValid(task);
        assertThat(valid.isPresent(), is(true));
    }


    @Test
    void storeFetchDestinationValidation() {
        Query.QueryBuilder<?, ?> builder = Query.builder()
            .id("test")
            .type(Query.class.getName())
            .sql("SELECT 1");

        Query task = builder
            .store(false)
            .destinationTable("project.dataset.table")
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(task);
        assertThat(valid.isPresent(), is(false));

        task = builder
            .store(true)
            .destinationTable("project.dataset.table")
            .build();

        valid = modelValidator.isValid(task);
        assertThat(valid.isPresent(), is(true));
    }
}
