package io.kestra.plugin.gcp.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.gcs.models.Blob;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.ArrayList;
import java.util.Spliterator;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractList extends AbstractGcs implements ListInterface {
    @NotNull
    protected Property<String> from;

    @Schema(
        title = "Include all versions",
        description = "If true, includes object versions; default false"
    )
    private Property<Boolean> allVersions;

    @Builder.Default
    private final Property<ListingType> listingType = Property.ofValue(ListingType.DIRECTORY);

    protected Property<String> regExp;

    public Spliterator<com.google.cloud.storage.Blob> iterator(Storage connection, URI from, RunContext runContext) throws IllegalVariableEvaluationException {
        Page<com.google.cloud.storage.Blob> list = connection.list(from.getAuthority(), options(from, runContext));

        return list.iterateAll().spliterator();
    }

    protected boolean filter(com.google.cloud.storage.Blob blob, String regExp) {
        return regExp == null || Blob.uri(blob).toString().matches(regExp);
    }

    private Storage.BlobListOption[] options(URI from, RunContext runContext) throws IllegalVariableEvaluationException {
        java.util.List<Storage.BlobListOption> options = new ArrayList<>();

        if (!from.getPath().equals("")) {
            options.add(Storage.BlobListOption.prefix(from.getPath().substring(1)));
        }

        if (this.allVersions != null) {
            options.add(Storage.BlobListOption.versions(runContext.render(this.allVersions).as(Boolean.class).orElseThrow()));
        }

        if (runContext.render(this.listingType).as(ListingType.class).orElseThrow() == ListingType.DIRECTORY) {
            options.add(Storage.BlobListOption.currentDirectory());
        }

        return Iterables.toArray(options, Storage.BlobListOption.class);
    }
}
