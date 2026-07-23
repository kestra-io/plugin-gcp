package io.kestra.plugin.gcp.rcs;

import java.net.URI;
import java.net.URLConnection;
import java.time.Instant;
import java.util.*;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a file attachment to a recipient's phone number",
    description = "Uploads and sends a media attachment (image, video, audio, or PDF) to a user via RBM."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a file attachment from a public URL",
            full = true,
            code = """
                id: rcs_send_file
                namespace: company.team

                tasks:
                  - id: send_file
                    type: io.kestra.plugin.gcp.rcs.SendFile
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    agentId: "{{ secret('RBM_AGENT_ID') }}"
                    msisdn: "+33612345678"
                    file: "https://example.com/delivery-receipt.pdf"
                """
        )
    }
)
public class SendFile extends AbstractRcs implements RunnableTask<SendFile.Output> {

    @Schema(title = "The file attachment URI (can be a public HTTPS URL or a Kestra internal storage URI)")
    @NotNull
    @PluginProperty(group = "main", internalStorageURI = true)
    private Property<String> file;

    @Schema(title = "Optional thumbnail image URI. Only applied when `file` is a public HTTPS URL; ignored when `file` is a Kestra internal storage URI")
    @PluginProperty(group = "main", internalStorageURI = true)
    private Property<String> thumbnail;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rMsisdn = runContext.render(this.msisdn).as(String.class).orElseThrow();
        var rFile = runContext.render(this.file).as(String.class).orElseThrow();
        var rThumbnail = this.thumbnail != null ? runContext.render(this.thumbnail).as(String.class).orElse(null) : null;

        String fileId;

        if (rFile.startsWith("http://") || rFile.startsWith("https://")) {
            var fileMetadata = new HashMap<String, Object>();
            fileMetadata.put("fileUrl", rFile);
            fileMetadata.put("agentId", runContext.render(this.agentId).as(String.class).orElseThrow());
            if (rThumbnail != null) {
                fileMetadata.put("thumbnailUrl", rThumbnail);
            }

            var response = this.executeRequest(
                runContext,
                "/v1/files",
                "POST",
                HttpRequest.JsonRequestBody.builder().content(fileMetadata).build()
            );
            var responseBody = JacksonMapper.toMap(response.getBody());
            fileId = (String) responseBody.get("name");
        } else {
            var fileUri = new URI(rFile);

            var filename = fileUri.getPath();
            String mimeType = null;
            if (filename != null) {
                mimeType = URLConnection.guessContentTypeFromName(filename);
            }
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            HttpResponse<String> uploadResponse;
            try (var is = runContext.storage().getFile(fileUri)) {
                var token = getAccessToken(runContext);
                var rAgentId = runContext.render(this.agentId).as(String.class).orElseThrow();
                var targetUrl = this.baseUrl + "/upload/v1/files?agentId=" + java.net.URLEncoder.encode(rAgentId, java.nio.charset.StandardCharsets.UTF_8);

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
                        .method("POST")
                        .addHeader("Authorization", "Bearer " + token)
                        .addHeader("Content-Type", mimeType)
                        .body(HttpRequest.InputStreamRequestBody.of(is));

                    uploadResponse = client.request(requestBuilder.build(), String.class);
                }
            }

            var uploadBody = JacksonMapper.toMap(uploadResponse.getBody());
            fileId = (String) uploadBody.get("name");
        }

        var payload = Map.of(
            "messageId", UUID.randomUUID().toString(),
            "contentMessage", Map.of(
                "contentInfo", Map.of(
                    "fileUrl", fileId
                )
            )
        );

        var endpointPath = "/v1/phones/" + java.net.URLEncoder.encode(rMsisdn, java.nio.charset.StandardCharsets.UTF_8) + "/agentMessages";
        var response = this.executeRequest(
            runContext,
            endpointPath,
            "POST",
            HttpRequest.JsonRequestBody.builder().content(payload).build()
        );

        var responseBody = JacksonMapper.toMap(response.getBody());
        var messageId = (String) responseBody.get("messageId");
        var sendTimeStr = (String) responseBody.get("sendTime");
        var sendTime = sendTimeStr != null ? Instant.parse(sendTimeStr) : Instant.now();

        return Output.builder()
            .messageId(messageId)
            .sendTime(sendTime)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The unique message ID assigned by the RBM platform")
        private final String messageId;

        @Schema(title = "The timestamp when the message was sent")
        private final Instant sendTime;
    }
}
