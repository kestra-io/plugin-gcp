package org.kestra.task.gcp.bigquery.models;

import lombok.Builder;
import lombok.Getter;
import org.kestra.core.models.annotations.OutputProperty;

import java.util.List;

@Getter
@Builder
public class ExternalTableDefinition {
    @OutputProperty(
        description = "Returns the fully-qualified URIs that point to your data in Google Cloud Storage.",
        body = {"Each URI can\n" +
            "   * contain one '*' wildcard character that must come after the bucket's name. Size limits related\n" +
            "   * to load jobs apply to external data sources, plus an additional limit of 10 GB maximum size\n" +
            "   * across all URIs."
        }
    )
    private final List<String> sourceUris;

    @OutputProperty(description = "Returns whether automatic detection of schema and format options should be performed.")
    private final Boolean autodetect;

    @OutputProperty(description = "Returns the compression type of the data source.")
    private final String compression;

    @OutputProperty(
        description = "Returns the maximum number of bad records that BigQuery can ignore when reading data.",
        body = "If the number of bad records exceeds this value, an invalid error is returned in the job result."
    )
    private final Integer maxBadRecords;

    @OutputProperty(
        description = "Returns whether BigQuery should allow extra values that are not represented in the table schema.",
        body = "If true, the extra values are ignored. If false, records with extra columns are treated " +
            "as bad records, and if there are too many bad records, an invalid error is returned in the job " +
            "result."
    )
    private final Boolean ignoreUnknownValues;

    public static ExternalTableDefinition of(com.google.cloud.bigquery.ExternalTableDefinition externalTableDefinition) {
        return ExternalTableDefinition.builder()
            .sourceUris(externalTableDefinition.getSourceUris())
            .autodetect(externalTableDefinition.getAutodetect())
            .compression(externalTableDefinition.getCompression())
            .maxBadRecords(externalTableDefinition.getMaxBadRecords())
            .ignoreUnknownValues(externalTableDefinition.getIgnoreUnknownValues())
            .build();
    }
}
