package io.kestra.plugin.gcp.gcs.models;

import java.net.URI;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import io.kestra.core.models.annotations.PluginProperty;

@Data
@Builder
public class Bucket {
    @Schema(
        title = "The bucket's unique name"
    )
    @PluginProperty(group = "advanced")
    private String name;

    @Schema(
        title = "The bucket's URI."
    )
    @PluginProperty(group = "advanced")
    private URI uri;

    @Schema(
        title = "The bucket's location"
    )
    @PluginProperty(group = "advanced")
    private String location;

    @Schema(
        title = "The bucket's website index page."
    )
    @PluginProperty(group = "advanced")
    private String indexPage;

    @Schema(
        title = "The custom object to return when a requested resource is not found."
    )
    @PluginProperty(group = "advanced")
    private String notFoundPage;

    public static Bucket of(com.google.cloud.storage.Bucket bucket) {
        return Bucket.builder()
            .name(bucket.getName())
            .uri(URI.create("gs://" + bucket.getName()))
            .location(bucket.getLocation())
            .indexPage(bucket.getIndexPage())
            .notFoundPage(bucket.getNotFoundPage())
            .build();
    }
}
