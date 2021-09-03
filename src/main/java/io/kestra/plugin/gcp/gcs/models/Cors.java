package io.kestra.plugin.gcp.gcs.models;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Data
@Builder
public class Cors {
    private final Integer maxAgeSeconds;
    private final List<HttpMethod> methods;
    private final List<com.google.cloud.storage.Cors.Origin> origins;
    private final List<String> responseHeaders;

    public static List<com.google.cloud.storage.Cors> convert(List<Cors> cors) {
        return cors
            .stream()
            .map(c -> c.convert())
            .collect(Collectors.toList());
    }

    public com.google.cloud.storage.Cors convert() {
        return com.google.cloud.storage.Cors.newBuilder()
            .setMaxAgeSeconds(this.maxAgeSeconds)
            .setMethods(this.methods == null ? null : this.methods
                .stream()
                .map(i -> com.google.cloud.storage.HttpMethod.valueOf(i.name()))
                .collect(Collectors.toList())
            )
            .setOrigins(this.origins == null ? null : this.origins
                .stream()
                .map(i -> com.google.cloud.storage.Cors.Origin.of(i.getValue()))
                .collect(Collectors.toList())
            )
            .setResponseHeaders(this.responseHeaders)
            .build();
    }

    public enum HttpMethod {
        GET,
        HEAD,
        PUT,
        POST,
        DELETE,
        OPTIONS
    }

    @Data
    @Builder
    public static class Origin {
        private final String value;
    }
}
