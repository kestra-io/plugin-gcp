package io.kestra.plugin.gcp.rcs;

import java.util.HashMap;
import java.util.Map;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Jacksonized
public class Suggestion {

    @Schema(title = "The type of suggestion button (URL, DIAL, REPLY, MAP)")
    @NotNull
    @PluginProperty
    private Property<SuggestionType> type;

    @Schema(title = "The label text displayed on the suggestion button")
    @NotNull
    @PluginProperty
    private Property<String> text;

    @Schema(title = "The postback data payload returned to the agent when clicked")
    @PluginProperty
    private Property<String> postbackData;

    @Schema(title = "The destination URL (required if type is URL)")
    @PluginProperty
    private Property<String> url;

    @Schema(title = "The phone number to dial in E.164 format (required if type is DIAL)")
    @PluginProperty
    private Property<String> phoneNumber;

    @Schema(title = "The latitude for the location coordinates (required if type is MAP)")
    @PluginProperty
    private Property<Double> latitude;

    @Schema(title = "The longitude for the location coordinates (required if type is MAP)")
    @PluginProperty
    private Property<Double> longitude;

    @Schema(title = "The label description for the location (optional if type is MAP)")
    @PluginProperty
    private Property<String> locationLabel;

    public Map<String, Object> toRbmSuggestion(RunContext runContext) throws Exception {
        var rType = runContext.render(this.type).as(SuggestionType.class).orElseThrow();
        var rText = runContext.render(this.text).as(String.class).orElseThrow();
        var rPostback = runContext.render(this.postbackData).as(String.class).orElse(rText);

        if (rType == SuggestionType.REPLY) {
            return Map.of(
                "reply", Map.of(
                    "text", rText,
                    "postbackData", rPostback
                )
            );
        }

        var actionContent = new HashMap<String, Object>();
        actionContent.put("text", rText);
        actionContent.put("postbackData", rPostback);

        if (rType == SuggestionType.URL) {
            var rUrl = runContext.render(this.url).as(String.class).orElseThrow();
            actionContent.put("openUrlAction", Map.of("url", rUrl));
        } else if (rType == SuggestionType.DIAL) {
            var rPhone = runContext.render(this.phoneNumber).as(String.class).orElseThrow();
            actionContent.put("dialAction", Map.of("phoneNumber", rPhone));
        } else if (rType == SuggestionType.MAP) {
            var rLat = runContext.render(this.latitude).as(Double.class).orElseThrow();
            var rLng = runContext.render(this.longitude).as(Double.class).orElseThrow();
            var location = new HashMap<String, Object>();
            location.put("latitude", rLat);
            location.put("longitude", rLng);
            runContext.render(this.locationLabel).as(String.class).ifPresent(l -> location.put("label", l));
            actionContent.put("viewLocationAction", Map.of("location", location));
        }

        return Map.of("action", actionContent);
    }
}
