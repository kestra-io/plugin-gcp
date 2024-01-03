package io.kestra.plugin.gcp.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import io.kestra.core.models.annotations.PluginProperty;
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
    protected String from;

    @Schema(
        title = "If set to `true`, lists all versions of a blob. The default is `false`."
    )
    @PluginProperty(dynamic = true)
    private Boolean allVersions;

    @Builder.Default
    private final ListingType listingType = ListingType.DIRECTORY;

    protected String regExp;

    public Spliterator<com.google.cloud.storage.Blob> iterator(Storage connection, URI from) {
        Page<com.google.cloud.storage.Blob> list = connection.list(from.getAuthority(), options(from));

        return list.iterateAll().spliterator();
    }

    protected boolean filter(com.google.cloud.storage.Blob blob, String regExp) {
        return regExp == null || Blob.uri(blob).toString().matches(regExp);
    }

    private Storage.BlobListOption[] options(URI from) {
        java.util.List<Storage.BlobListOption> options = new ArrayList<>();

        if (!from.getPath().equals("")) {
            options.add(Storage.BlobListOption.prefix(from.getPath().substring(1)));
        }

        if (this.allVersions != null) {
            options.add(Storage.BlobListOption.versions(this.allVersions));
        }

        if (this.listingType == ListingType.DIRECTORY) {
            options.add(Storage.BlobListOption.currentDirectory());
        }

        return Iterables.toArray(options, Storage.BlobListOption.class);
    }
}
