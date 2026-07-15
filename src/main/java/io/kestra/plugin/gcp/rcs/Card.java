package io.kestra.plugin.gcp.rcs;

import java.util.*;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Jacksonized
public class Card {
    @Schema(title = "The card title")
    @PluginProperty
    private Property<String> title;

    @Schema(title = "The card description")
    @PluginProperty
    private Property<String> description;

    @Schema(title = "The card media/image URL")
    @PluginProperty
    private Property<String> imageUrl;

    @Schema(title = "The list of suggested actions/replies (max 4 per card)")
    @PluginProperty
    private List<Suggestion> suggestions;

    public Map<String, Object> toRbmCardContent(RunContext runContext) throws Exception {
        var cardContent = new HashMap<String, Object>();
        if (this.title != null) {
            runContext.render(this.title).as(String.class).ifPresent(t -> cardContent.put("title", t));
        }
        if (this.description != null) {
            runContext.render(this.description).as(String.class).ifPresent(d -> cardContent.put("description", d));
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
                throw new IllegalArgumentException("RCS Card suggestions cannot exceed 4 items");
            }
            var rbmSuggestions = new ArrayList<Map<String, Object>>();
            for (var s : this.suggestions) {
                rbmSuggestions.add(s.toRbmSuggestion(runContext));
            }
            cardContent.put("suggestions", rbmSuggestions);
        }
        return cardContent;
    }
}
