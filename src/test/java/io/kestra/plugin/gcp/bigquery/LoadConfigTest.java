package io.kestra.plugin.gcp.bigquery;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.WriteChannelConfiguration;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class LoadConfigTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRenderTimePartitioningFieldWithoutDoubleEvaluation() throws Exception {
        var task = Load.builder()
            .id("load")
            .type(Load.class.getName())
            .format(AbstractLoad.Format.JSON)
            .destinationTable(Property.ofValue("my-project.my_dataset.my_table"))
            .timePartitioningType(Property.ofValue(TimePartitioning.Type.DAY))
            .timePartitioningField(Property.ofExpression("{{ field }}"))
            .build();

        var runContext = runContextFactory.of(Map.of("field", "{{ 'created_at' }}"));

        var builder = WriteChannelConfiguration
            .newBuilder(BigQueryService.tableId("my-project.my_dataset.my_table"));
        task.setOptions(builder, runContext);

        var configuration = builder.build();
        assertThat(configuration.getTimePartitioning(), notNullValue());
        assertThat(configuration.getTimePartitioning().getField(), is("{{ 'created_at' }}"));
    }

    @Test
    void shouldRenderStandardTimePartitioningFieldExpression() throws Exception {
        var task = Load.builder()
            .id("load")
            .type(Load.class.getName())
            .format(AbstractLoad.Format.JSON)
            .destinationTable(Property.ofValue("my-project.my_dataset.my_table"))
            .timePartitioningType(Property.ofValue(TimePartitioning.Type.DAY))
            .timePartitioningField(Property.ofExpression("{{ 'created_at' }}"))
            .build();

        var runContext = runContextFactory.of(Map.of());

        var builder = WriteChannelConfiguration
            .newBuilder(BigQueryService.tableId("my-project.my_dataset.my_table"));
        task.setOptions(builder, runContext);

        var configuration = builder.build();
        assertThat(configuration.getTimePartitioning(), notNullValue());
        assertThat(configuration.getTimePartitioning().getField(), is("created_at"));
    }
}
