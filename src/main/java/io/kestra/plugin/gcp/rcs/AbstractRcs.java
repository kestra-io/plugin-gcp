package io.kestra.plugin.gcp.rcs;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
public abstract class AbstractRcs extends AbstractTask {

    @Schema(title = "The unique agent ID of the Brand Agent configured on the RBM platform")
    @NotNull
    @PluginProperty
    protected Property<String> agentId;

    @Schema(title = "The recipient's phone number (MSISDN) in E.164 format (e.g. +33612345678)")
    @NotNull
    @PluginProperty
    protected Property<String> msisdn;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected String baseUrl = "https://rcsbusinessmessaging.googleapis.com";

    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(Collections.singletonList("https://www.googleapis.com/auth/rcsbusinessmessaging"));

    protected String getAccessToken(RunContext runContext) throws Exception {
        return this.credentials(runContext)
            .createScoped(runContext.render(this.scopes).asList(String.class))
            .refreshAccessToken()
            .getTokenValue();
    }

    protected HttpResponse<String> executeRequest(RunContext runContext, String endpointPath, String httpMethod, HttpRequest.RequestBody requestBody) throws Exception {
        var token = getAccessToken(runContext);
        var rAgentId = runContext.render(this.agentId).as(String.class).orElseThrow();

        var renderedPath = runContext.render(endpointPath);
        var separator = renderedPath.contains("?") ? "&" : "?";
        var targetUrl = this.baseUrl + renderedPath + separator + "agentId=" + java.net.URLEncoder.encode(rAgentId, StandardCharsets.UTF_8);

        try (
            var client = new HttpClient(
                runContext,
                HttpConfiguration.builder()
                    .timeout(
                        TimeoutConfiguration.builder()
                            .readIdleTimeout(Property.ofValue(java.time.Duration.ofSeconds(60)))
                            .build()
                    )
                    .build()
            )
        ) {
            var requestBuilder = HttpRequest.builder()
                .uri(new URI(targetUrl))
                .method(httpMethod)
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json");

            if (requestBody != null) {
                requestBuilder.body(requestBody);
            }

            return client.request(requestBuilder.build(), String.class);
        } catch (HttpClientResponseException e) {
            var resp = e.getResponse();
            var code = resp != null && resp.getStatus() != null ? resp.getStatus().getCode() : -1;
            var body = resp != null && resp.getBody() != null ? String.valueOf(resp.getBody()) : "<empty>";
            throw new HttpClientResponseException(
                "RBM API request failed with status code " + code + ". Response body: " + body,
                resp,
                e
            );
        }
    }
}
