package io.kestra.plugin.gcp.rcs;

import java.time.Instant;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a standalone Rich Card to a recipient's phone number",
    description = "Sends a rich media card with title, description, image, and action/reply buttons."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a Rich Card with action buttons",
            full = true,
            code = """
                id: rcs_send_rich_card
                namespace: company.team

                tasks:
                  - id: send_card
                    type: io.kestra.plugin.gcp.rcs.SendRichCard
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    agentId: "{{ secret('RBM_AGENT_ID') }}"
                    msisdn: "+33612345678"
                    title: "Your delivery is ready"
                    description: "Tap below to track your package or contact support."
                    imageUrl: "https://example.com/delivery-banner.jpg"
                    suggestions:
                      - type: URL
                        text: "Track package"
                        url: "https://tracking.example.com/order/12345"
                      - type: REPLY
                        text: "Contact support"
                        postbackData: "contact_support"
                """
        )
    }
)
public class SendRichCard extends AbstractRcs implements RunnableTask<SendRichCard.Output> {

    @Schema(title = "The card title")
    @PluginProperty(group = "main")
    private Property<String> title;

    @JsonProperty("description")
    @Schema(name = "description", title = "The card description")
    @PluginProperty(group = "main")
    private Property<String> contentDescription;

    @Schema(title = "The card image URL")
    @PluginProperty(group = "main")
    private Property<String> imageUrl;

    @Schema(title = "The list of suggested action/reply buttons (max 4)")
    @PluginProperty(group = "main")
    private List<Suggestion> suggestions;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rMsisdn = runContext.render(this.msisdn).as(String.class).orElseThrow();

        var cardContent = new HashMap<String, Object>();
        if (this.title != null) {
            runContext.render(this.title).as(String.class).ifPresent(t -> cardContent.put("title", t));
        }
        if (this.contentDescription != null) {
            runContext.render(this.contentDescription).as(String.class).ifPresent(d -> cardContent.put("description", d));
        }
        if (this.imageUrl != null) {
            var rImageUrl = runContext.render(this.imageUrl).as(String.class).orElse(null);
            if (rImageUrl != null) {
                cardContent.put(
                    "media", Map.of(
                        "height", "MEDIUM",
                        "contentInfo", Map.of("fileUrl", rImageUrl)
                    )
                );
            }
        }
        if (this.suggestions != null && !this.suggestions.isEmpty()) {
            if (this.suggestions.size() > 4) {
                throw new IllegalArgumentException("RCS Standalone Card suggestions cannot exceed 4 items");
            }
            var rbmSuggestions = new ArrayList<Map<String, Object>>();
            for (var s : this.suggestions) {
                rbmSuggestions.add(s.toRbmSuggestion(runContext));
            }
            cardContent.put("suggestions", rbmSuggestions);
        }

        var payload = Map.of(
            "messageId", UUID.randomUUID().toString(),
            "contentMessage", Map.of(
                "richCard", Map.of(
                    "standaloneCard", Map.of(
                        "cardOrientation", "VERTICAL",
                        "cardContent", cardContent
                    )
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
