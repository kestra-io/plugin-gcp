package io.kestra.plugin.gcp.rcs;

import java.time.Instant;
import java.util.*;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Send a carousel of Rich Cards to a recipient's phone number",
    description = "Sends a horizontal scrolling list of 2 to 10 rich media cards."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a carousel of rich cards",
            full = true,
            code = """
                id: rcs_send_carousel
                namespace: company.team

                tasks:
                  - id: send_carousel
                    type: io.kestra.plugin.gcp.rcs.SendCarousel
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    agentId: "{{ secret('RBM_AGENT_ID') }}"
                    msisdn: "+33612345678"
                    cards:
                      - title: "Item 1"
                        description: "First option description"
                        imageUrl: "https://example.com/item1.jpg"
                        suggestions:
                          - type: REPLY
                            text: "Select Item 1"
                            postbackData: "select_1"
                      - title: "Item 2"
                        description: "Second option description"
                        imageUrl: "https://example.com/item2.jpg"
                        suggestions:
                          - type: REPLY
                            text: "Select Item 2"
                            postbackData: "select_2"
                """
        )
    }
)
public class SendCarousel extends AbstractRcs implements RunnableTask<SendCarousel.Output> {

    @Schema(title = "The list of rich cards in the carousel (between 2 and 10 cards)")
    @NotNull
    @PluginProperty
    private List<Card> cards;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rMsisdn = runContext.render(this.msisdn).as(String.class).orElseThrow();

        if (this.cards == null || this.cards.size() < 2 || this.cards.size() > 10) {
            throw new IllegalArgumentException("RCS Carousel must contain between 2 and 10 cards");
        }

        var cardContents = new ArrayList<Map<String, Object>>();
        for (var card : this.cards) {
            cardContents.add(card.toRbmCardContent(runContext));
        }

        var payload = Map.of(
            "messageId", UUID.randomUUID().toString(),
            "contentMessage", Map.of(
                "richCard", Map.of(
                    "carouselCard", Map.of(
                        "cardWidth", "MEDIUM",
                        "cardContents", cardContents
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
