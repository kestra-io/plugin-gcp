package io.kestra.plugin.gcp.rcs;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(title = "The action type for the suggestion button")
public enum SuggestionType {
    URL,
    DIAL,
    REPLY,
    MAP
}
