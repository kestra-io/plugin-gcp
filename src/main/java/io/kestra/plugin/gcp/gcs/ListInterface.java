package io.kestra.plugin.gcp.gcs;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface ListInterface {
    @Schema(
        title = "Source prefix",
        description = "gs:// path to list"
    )
    @NotNull
    Property<String> getFrom();

    @Schema(
        title = "Listing type",
        description = "DIRECTORY lists only immediate children; RECURSIVE traverses all descendants. Default DIRECTORY."
    )
    Property<List.ListingType> getListingType();

    @Schema(
        title = "Regex filter",
        description = "Optional regex applied to full object path (e.g., `.*2020-01-0.\\.csv`)"
    )
    Property<String> getRegExp();

    enum Filter {
        FILES,
        DIRECTORY,
        BOTH
    }

    enum ListingType {
        RECURSIVE,
        DIRECTORY,
    }
}
