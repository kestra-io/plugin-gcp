package io.kestra.plugin.gcp.gcs.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.net.URI;

@Data
@Builder
public class Bucket {
    @Schema(
        title = "The bucket's unique name"
    )
    private String name;

    @Schema(
        title = "The bucket's URI."
    )
    private URI uri;

    @Schema(
        title = "The bucket's location"
    )
    private String location;

    @Schema(
        title = "The bucket's website index page."
    )
    private String indexPage;

    @Schema(
        title = "The custom object to return when a requested resource is not found."
    )
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
