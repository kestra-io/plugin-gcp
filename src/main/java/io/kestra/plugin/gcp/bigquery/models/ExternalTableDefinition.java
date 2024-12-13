package io.kestra.plugin.gcp.bigquery.models;

import com.google.cloud.bigquery.FormatOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Getter
@Builder
@Jacksonized
public class ExternalTableDefinition {
    @Schema(
        title = "The fully-qualified URIs that point to your data in Google Cloud Storage.",
        description = "Each URI can\n" +
            "* contain one '*' wildcard character that must come after the bucket's name. Size limits related\n" +
            "* to load jobs apply to external data sources, plus an additional limit of 10 GB maximum size\n" +
            "* across all URIs."
    )
    private final Property<List<String>> sourceUris;

    @Schema(
        title = "The source format, and possibly some parsing options, of the external data."
    )
    private final Property<FormatType> formatType;

    @Schema(title = "Whether automatic detection of schema and format options should be performed.")
    private final Property<Boolean> autodetect;

    @Schema(title = "The compression type of the data source.")
    private final Property<String> compression;

    @Schema(
        title = "The maximum number of bad records that BigQuery can ignore when reading data.",
        description = "If the number of bad records exceeds this value, an invalid error is returned in the job result."
    )
    private final Property<Integer> maxBadRecords;

    @Schema(
        title = "Whether BigQuery should allow extra values that are not represented in the table schema.",
        description = "If true, the extra values are ignored. If false, records with extra columns are treated " +
            "as bad records, and if there are too many bad records, an invalid error is returned in the job " +
            "result."
    )
    private final Property<Boolean> ignoreUnknownValues;

    public static ExternalTableDefinition.Output of(com.google.cloud.bigquery.ExternalTableDefinition externalTableDefinition) {
        ExternalTableDefinition.Output.OutputBuilder builder = ExternalTableDefinition.Output.builder()
            .sourceUris(externalTableDefinition.getSourceUris())
            .autodetect(externalTableDefinition.getAutodetect())
            .compression(externalTableDefinition.getCompression())
            .maxBadRecords(externalTableDefinition.getMaxBadRecords())
            .ignoreUnknownValues(externalTableDefinition.getIgnoreUnknownValues());

        if (externalTableDefinition.getFormatOptions() != null) {
            builder.formatType(FormatType.valueOf(externalTableDefinition.getFormatOptions().getType()));
        }
            return builder.build();
    }

    public com.google.cloud.bigquery.ExternalTableDefinition to(RunContext runContext, io.kestra.plugin.gcp.bigquery.models.Schema schema) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.ExternalTableDefinition.Builder builder = com.google.cloud.bigquery.ExternalTableDefinition.newBuilder(
            runContext.render(this.sourceUris).asList(String.class),
            schema.to(runContext),
            FormatOptions.of(runContext.render(this.formatType).as(FormatType.class).orElseThrow().name())
        );

        if (this.compression != null) {
            builder.setCompression(runContext.render(this.compression).as(String.class).orElseThrow());
        }

        if (this.autodetect != null) {
            builder.setAutodetect(runContext.render(this.autodetect).as(Boolean.class).orElseThrow());
        }

        if (this.maxBadRecords != null) {
            builder.setMaxBadRecords(runContext.render(this.maxBadRecords).as(Integer.class).orElseThrow());
        }

        if (this.ignoreUnknownValues != null) {
            builder.setIgnoreUnknownValues(runContext.render(this.ignoreUnknownValues).as(Boolean.class).orElseThrow());
        }

        return builder.build();
    }

    public enum FormatType {
        CSV,
        JSON,
        BIGTABLE,
        DATASTORE_BACKUP,
        AVRO,
        GOOGLE_SHEETS,
        PARQUET,
        ORC,
    }

    @Getter
    @Builder
    public static class Output {
        private final List<String> sourceUris;
        private final FormatType formatType;
        private final Boolean autodetect;
        private final String compression;
        private final Integer maxBadRecords;
        private final Boolean ignoreUnknownValues;
    }
}
