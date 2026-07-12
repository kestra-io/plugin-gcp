package io.kestra.plugin.gcp.rcs;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import io.kestra.core.http.HttpRequest;
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
    title = "Send a plain text RCS message to a recipient's phone number",
    description = "Sends a standard text message using Google RCS Business Messaging (RBM)."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a plain text RCS message",
            full = true,
            code = """
                id: rcs_send_message
                namespace: company.team

                tasks:
                  - id: send
                    type: io.kestra.plugin.gcp.rcs.SendMessage
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    agentId: "{{ secret('RBM_AGENT_ID') }}"
                    msisdn: "+33612345678"
                    text: "Your order has been shipped and will arrive within 2 business days."
                """
        )
    }
)
public class SendMessage extends AbstractRcs implements RunnableTask<SendMessage.Output> {

    @Schema(title = "The plain text content of the message")
    @NotNull
    @PluginProperty
    private Property<String> text;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rMsisdn = runContext.render(this.msisdn).as(String.class).orElseThrow();
        var rText = runContext.render(this.text).as(String.class).orElseThrow();

        var payload = Map.of(
            "messageId", UUID.randomUUID().toString(),
            "contentMessage", Map.of(
                "text", rText
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
