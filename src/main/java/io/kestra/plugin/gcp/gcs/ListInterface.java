package io.kestra.plugin.gcp.gcs;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface ListInterface {
    @Schema(
        title = "The directory to list"
    )
    @NotNull
    Property<String> getFrom();

    @Schema(
        title = "The listing type you want (like directory or recursive)",
        description = "if DIRECTORY, will only list objects in the specified directory\n" +
            "if RECURSIVE, will list objects in the specified directory recursively\n" +
            "Default value is DIRECTORY\n" +
            "When using RECURSIVE value, be careful to move your files to a location not in the `from` scope"
    )
    Property<List.ListingType> getListingType();

    @Schema(
        title = "A regexp to filter on full path",
        description = "ex:\n"+
            "`regExp: .*` to match all files\n"+
            "`regExp: .*2020-01-0.\\\\.csv` to match files between 01 and 09 of january ending with `.csv`"
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
