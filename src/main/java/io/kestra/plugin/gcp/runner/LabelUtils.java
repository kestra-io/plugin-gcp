package io.kestra.plugin.gcp.runner;

import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

final class LabelUtils {
    private LabelUtils() {
    }

    static Map<String, String> labels(RunContext runContext) {
        // GCP didn't support '.' in label values, so we replace them by '-'
        return ScriptService.labels(runContext, "kestra-", true, true)
            .entrySet()
            .stream()
            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().replaceAll("\\.", "-")))
            .collect(Collectors.toMap(
                entry -> entry.getKey(),
                entry -> entry.getValue()
            ));
    }
}
